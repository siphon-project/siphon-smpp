# Building an SMSC gateway

This is the headline recipe: a **commodity store-and-forward SMS gateway**.
ESMEs bind to you, you relay their mobile-originated (MO) traffic out over
configured upstream binds, and you route delivery receipts (and MO replies) back
to the right ESME — all in ~200 lines of pure-SMPP Python.

The complete script is
[`examples/gateway.py`](https://github.com/siphon-project/siphon-smpp/blob/main/examples/gateway.py);
this page walks it section by section. Pair it with the config in
[`deploy/smpp.example.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/smpp.example.yaml).

```
    ┌────────┐   submit_sm    ┌─────────────────┐   submit_sm   ┌──────────┐
    │  ESME  │ ─────────────▶ │  this gateway   │ ────────────▶ │ upstream │
    │ (apps) │ ◀───────────── │  (siphon-smpp)  │ ◀──────────── │   SMSC   │
    └────────┘   deliver_sm    └─────────────────┘  deliver_sm   └──────────┘
                (DLR / MO)                          (DLR / MO)
```

What the **crate** owns (you don't write it): TCP framing + the SMPP codec, the
bind/enquire_link/inactivity timers and sequence windowing, outbound bind
supervision with reconnect-backoff, per-bind throttling, and dispatch. What
**your script** owns: who may bind, routing, DLR correlation, and any queueing.
See [Concepts](../concepts.md#the-boundary-rust-owns-the-wire-python-owns-policy).

## State: use the shared store, not a dict

```python
import json, uuid
from siphon import smpp, log, cache
```

Correlation and the ESME session map live in **`siphon.cache`**, a shared store,
for two reasons every real deployment hits:

- It survives **script hot-reload** — edit the file and in-flight DLR
  correlations aren't lost.
- It's shared across **workers and replicas** — a delivery receipt can come back
  on any worker (or, in Kubernetes, any [replica's outbound
  bind](../kubernetes.md#outbound-you-upstream)). A module-level dict loses
  both.

## Authorising binds

Closed by default: anything not explicitly allowed is refused, and every reject
carries a *reason* (logged). In production, resolve credentials against your own
store rather than a literal dict.

```python
_CREDENTIALS = {"acme": "s3cr3t", "globex": "hunter2"}   # demo only

@smpp.on_bind
async def authorise(bind):
    expected = _CREDENTIALS.get(bind.system_id)
    if expected is None:
        return bind.reject("ESME_RINVSYSID", f"unknown system_id {bind.system_id!r}")
    if bind.password != expected:
        return bind.reject("ESME_RINVPASWD", f"bad password for {bind.system_id!r}")
    log.info(f"bind authorised: {bind.system_id} @ {bind.client_addr}")
    return bind.accept()
```

## Session tracking

Maintain a `system_id ↔ session_id` map so you can MT-deliver to a bound ESME
later, and clean it up on unbind. The handler fires for **both** inbound ESMEs
(`kind == "esme"`) and your outbound binds (`kind == "bind"`).

```python
@smpp.on_session("bound")
async def on_bound(session):
    if session.kind == "esme":
        await cache.set(f"esme_session:{session.system_id}", session.session_id)
        await cache.set(f"esme_system:{session.session_id}", session.system_id)
    else:  # one of our outbound binds came up
        log.info(f"outbound bind up: {session.system_id}")

@smpp.on_session("unbound")
async def on_unbound(session):
    if session.kind == "esme":
        await cache.delete(f"esme_session:{session.system_id}")
        await cache.delete(f"esme_system:{session.session_id}")
    else:
        log.warning(f"outbound bind down: {session.system_id}")
```

## Routing

siphon-smpp doesn't route for you — it hands you the declared
[`routing` rules](../configuration.md#routing-declarative-routing-rules) via
`smpp.routing_rules()` and you decide. Here: **longest-matching prefix wins**,
else the default chain, else the first configured bind. A chain step
`bind:<name>` names an outbound bind.

```python
def _bind_from_chain(chain):
    for step in chain:
        if step.startswith("bind:"):
            return step[len("bind:"):]
    return None

def _pick_bind(destination_addr):
    default_chain, rules = smpp.routing_rules()
    best = None  # (prefix_len, bind_name)
    for rule in rules:
        prefix = rule.get("prefix", "")
        if destination_addr.startswith(prefix):
            name = _bind_from_chain(rule.get("chain", []))
            if name and (best is None or len(prefix) > best[0]):
                best = (len(prefix), name)
    if best:
        return best[1]
    name = _bind_from_chain(default_chain)
    if name:
        return name
    configured = smpp.binds()
    return configured[0]["name"] if configured else None
```

Because routing is just data + your code, you can swap this for a database
lookup, an LCR table, or anything else without touching the runtime.

## MO: `submit_sm` from an ESME

Pick a bind, relay the message upstream, and — if the ESME asked for a receipt —
remember how to route the DLR back. The key trick: **hand the ESME our own id
space** (`our_id`), and key correlation by the **upstream** id (what the DLR will
carry).

```python
@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    bind = _pick_bind(pdu.destination_addr)
    if bind is None:
        return pdu.reply(command_status="ESME_RINVDSTADR")

    our_id = uuid.uuid4().hex[:12]         # id we return to the ESME
    try:
        resp = await smpp.submit_via(
            bind=bind,
            source_addr=pdu.source_addr, source_addr_ton=pdu.source_addr_ton,
            source_addr_npi=pdu.source_addr_npi,
            destination_addr=pdu.destination_addr, dest_addr_ton=pdu.dest_addr_ton,
            dest_addr_npi=pdu.dest_addr_npi,
            short_message=pdu.short_message, esm_class=pdu.esm_class,
            data_coding=pdu.data_coding,
            registered_delivery=pdu.registered_delivery,
        )
    except Exception as e:                  # bind down, upstream nack, timeout
        log.error(f"submit via {bind} failed: {e}")
        return pdu.reply(command_status="ESME_RSUBMITFAIL")

    if pdu.registered_delivery and resp.message_id:
        await cache.set(f"dlr:{bind}:{resp.message_id}", json.dumps({
            "esme_session": session.session_id,
            "esme_system":  session.system_id,
            "our_id":       our_id,
            "source_addr":  pdu.source_addr,
            "destination_addr": pdu.destination_addr,
        }))

    return pdu.reply(message_id=our_id)
```

Note the `try/except`: outbound failures become an `ESME_RSUBMITFAIL` for the
originating ESME rather than an unhandled exception.

## `deliver_sm`: DLR or MO reply

Inbound `deliver_sm` on an outbound bind is either a **delivery receipt**
(`pdu.is_dlr`) or a **mobile-originated** message coming back (e.g. a reply to a
two-way short code). Ack the upstream regardless.

```python
@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    if pdu.is_dlr:
        await _route_dlr(pdu, session)
    else:
        await _route_mo_reply(pdu, session)
    return pdu.reply()      # ack upstream
```

### Routing the DLR back to the ESME

Look up the correlation by the upstream id, rebuild a receipt body carrying
**our** id (the one the ESME knows), and `deliver_to` the originating session
with `esm_class=0x04`.

```python
async def _route_dlr(pdu, session):
    receipt = pdu.receipt or {}
    upstream_id = receipt.get("id", "")
    raw = json.loads(await cache.get(f"dlr:{session.system_id}:{upstream_id}") or "null")
    if not raw:
        log.warning(f"DLR for unknown upstream id {upstream_id!r} on {session.system_id}")
        return

    body = (
        f"id:{raw['our_id']} sub:001 dlvrd:001 "
        f"submit date:{receipt.get('submit_date','')} "
        f"done date:{receipt.get('done_date','')} "
        f"stat:{receipt.get('stat','UNKNOWN')} err:{receipt.get('err','000')} "
        f"text:{receipt.get('text','')}"
    ).encode()

    try:
        await smpp.deliver_to(
            session_id=raw["esme_session"],
            source_addr=raw["destination_addr"],
            destination_addr=raw["source_addr"],
            short_message=body,
            esm_class=0x04,           # delivery receipt
        )
    except Exception as e:
        log.error(f"DLR delivery to {raw['esme_system']} failed: {e}")
    finally:
        await cache.delete(f"dlr:{session.system_id}:{upstream_id}")
```

!!! tip "Why the id swap matters"
    The ESME only ever saw `our_id`. Upstream assigned its own `message_id`. By
    keying correlation on the upstream id and rewriting the receipt with `our_id`,
    the DLR is routable end-to-end and the ESME sees a consistent id — even
    across a hot-reload or a different worker handling the receipt.

### Routing an MO reply

An MO message inbound from upstream goes to whichever ESME owns the destination.
The example uses a simple owner map; a real gateway consults its number /
short-code routing table.

```python
async def _route_mo_reply(pdu, session):
    owner = await cache.get(f"msisdn_owner:{pdu.destination_addr}")
    if not owner:
        log.warning(f"inbound MO for unrouted destination {pdu.destination_addr}")
        return
    esme_session = await cache.get(f"esme_session:{owner}")
    if not esme_session:
        log.warning(f"owner {owner} not bound; dropping/queueing")
        return
    await smpp.deliver_to(
        session_id=esme_session,
        source_addr=pdu.source_addr, source_addr_ton=pdu.source_addr_ton,
        source_addr_npi=pdu.source_addr_npi,
        destination_addr=pdu.destination_addr,
        short_message=pdu.short_message, esm_class=pdu.esm_class,
        data_coding=pdu.data_coding,
    )
```

## `alert_notification` and `cancel_sm`

Upstream `alert_notification` signals a previously-unavailable MS is reachable
again — the cue to flush any MT you queued for it:

```python
@smpp.on_pdu("alert_notification")
async def on_alert(alert, session):
    log.info(f"alert on {session.system_id}: {alert.source_addr} available — flush queued MT")
```

`cancel_sm` from an ESME: this example keeps no cancellable spool, so it refuses
with a clear status. A gateway with a real outbound queue would dequeue the
matching message and `await smpp.cancel_via(...)` upstream.

```python
@smpp.on_pdu("cancel_sm")
async def on_cancel(pdu, session):
    return pdu.reply(command_status="ESME_RCANCELFAIL")
```

## From here to production

This recipe is complete but deliberately minimal on persistence. To harden it:

- **Persistence** — back `siphon.cache` with a shared, durable store so
  correlations survive a restart, and make it cluster-wide before scaling out
  ([Kubernetes & scaling](../kubernetes.md#outbound-you-upstream)).
- **Store-and-forward queue** — the `chain` steps beyond `bind:` (e.g. `queue`)
  are yours to implement: spool on failure, retry with backoff, expire.
- **Throttling policy** — the runtime enforces `max_msg_per_sec` in both
  directions (per outbound bind, and per inbound ESME session via
  `server.max_msg_per_sec` / `throttle_action`). Decide the *policy* around it:
  spool vs. shed on the outbound cap, and `pace` vs. `reject` (`ESME_RTHROTTLED`)
  for inbound over-rate. See
  [Configuration → Throttling](../configuration.md#throttling).
- **Deploy it** — [Deployment](../deployment.md) for the image and config,
  [Kubernetes & scaling](../kubernetes.md) for the HA/failover model,
  [Performance](../performance.md) to load-test your handler.
