# siphon-smpp

**An SMPP 3.4 addon for [siphon](https://github.com/siphon-project/siphon).**

<p>
  <a href="https://github.com/siphon-project/siphon-smpp/actions/workflows/ci.yaml">
    <img src="https://github.com/siphon-project/siphon-smpp/actions/workflows/ci.yaml/badge.svg" alt="CI">
  </a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/Rust-1.80%2B-000000?logo=rust&logoColor=white" alt="Rust 1.80+">
  <img src="https://img.shields.io/badge/SMPP-3.4-blue" alt="SMPP 3.4">
</p>

siphon is a high-performance SIP/IMS platform whose routing logic lives in
hot-reloaded, free-threaded Python. `siphon-smpp` plugs an `smpp` namespace into
a siphon binary so the same scripts that handle SIP requests can handle **SMPP
PDUs** — `bind`, `submit_sm`, `deliver_sm` — with the same decorator style.

The boundary is the same one siphon draws everywhere: **the Rust side owns the
wire protocol** (TCP/TLS framing, the SMPP codec, sequence-number windowing,
keep-alive and response timers, reconnect-with-backoff); **Python scripts own
policy** (which binds to accept, where to route a message, what message-id and
status code to return). Scripts never touch a socket.

---

## What it is

`siphon-smpp` is a library, not a standalone server — it runs as an extension
inside a [siphon](https://github.com/siphon-project/siphon) binary. It provides
two pieces:

- the `smpp` Python module that scripts `from siphon import smpp` (decorators +
  helper pyclasses + `submit_via`); and
- a tokio-side SMPP runtime — an inbound SMPP server plus one supervised outbound
  bind per configured peer — that dispatches each PDU into the matching script
  handler, so a hot-reloaded script is picked up on the next PDU.

A siphon binary composes it in at startup; see the siphon documentation for how
extensions are wired into a binary.

It speaks **two directions**, both described in terms of *binds*:

- **Inbound binds** — external ESMEs connect to siphon-smpp's SMPP server (the
  `server.bind_address`/`server.port` listener). They `bind_transceiver`,
  authorised by `@smpp.on_bind`, then `submit_sm` to us
  (`@smpp.on_pdu("submit_sm")`); we can `deliver_sm` back to them.
  `bind_transmitter` / `bind_receiver` are rejected — only transceiver binds are
  accepted.
- **Outbound binds** — siphon-smpp binds out as an ESME to remote SMSCs or
  aggregators (the `binds:` config list). We `submit_sm` out via
  `submit_via(bind="<name>", …)`; they `deliver_sm` back to us
  (`@smpp.on_pdu("deliver_sm")`). Each outbound bind is supervised: connect,
  hold the session, and on disconnect reconnect with exponential backoff
  (capped at 60s, reset after a healthy session).

---

---

## Script API

Scripts import the `smpp` namespace and register handlers with decorators, just
like the SIP side. A realistic SMS gateway script:

```python
from siphon import smpp, cache, log

@smpp.on_bind
async def authorize(bind):
    # bind.system_id / bind.password / bind.client_addr
    secret = await cache.fetch("esme_secrets", bind.system_id)
    if secret is not None and bind.password == secret:
        log.info(f"bind accepted: {bind.system_id} from {bind.client_addr}")
        return bind.accept()
    log.warn(f"bind rejected: {bind.system_id}")
    return bind.reject()

@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    # MO from an external ESME on an inbound bind.
    log.info(f"submit_sm {pdu.source_addr} -> {pdu.destination_addr} "
             f"({pdu.sm_length} bytes)")

    if not await cache.exists(f"reg:{pdu.destination_addr}"):
        return pdu.reply(command_status="ESME_RINVDSTADR")

    # Forward out over a configured outbound bind.
    resp = await smpp.submit_via(
        bind="aggregator-eu",
        source_addr=pdu.source_addr,
        destination_addr=pdu.destination_addr,
        short_message=pdu.short_message,   # bytes
        data_coding=pdu.data_coding,
        esm_class=pdu.esm_class,
        registered_delivery=pdu.registered_delivery,
    )
    # Ack the ESME with the upstream message-id.
    return pdu.reply(message_id=resp.message_id)

@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    # MT (or a delivery receipt) arriving back on an outbound bind.
    # session.kind == "bind", session.system_id == the bind name.
    log.info(f"deliver_sm via {session.system_id}: "
             f"{pdu.source_addr} -> {pdu.destination_addr}")
    # ... deliver onward, persist, etc. ...
    return pdu.reply()   # ESME_ROK ack
```

Notes:

- `@smpp.on_bind` receives a `Bind` (`system_id`, `password`, `client_addr`).
  Return `bind.accept()` (truthy) to accept, `bind.reject()` (falsy) to reject.
  **With no `@smpp.on_bind` handler registered, binds are rejected — closed by
  default.** The script is the sole authority on credentials.
- `@smpp.on_pdu("<command>")` handlers receive `(pdu, session)`. The `Pdu`
  mirrors the SMPP 3.4 fields (`source_addr`, `destination_addr`, `esm_class`,
  `data_coding`, `short_message` as `bytes`, `is_tpdu`, …). `Session` carries
  `kind` (`"esme"` for an inbound bind, `"bind"` for an outbound one),
  `session_id`, `system_id`, and `client_addr`.
- Return `pdu.reply(message_id="…")` to accept (the submit_sm path),
  `pdu.reply(command_status="ESME_RSUBMITFAIL")` to reject, or `pdu.reply()` /
  `None` for a default `ESME_ROK` ack (the deliver_sm path). Unknown status
  strings raise immediately, so a typo surfaces instead of silently becoming
  `ESME_ROK`.
- `await smpp.submit_via(bind="<name>", source_addr=…, destination_addr=…,
  short_message=…, …)` sends a `submit_sm` over a named outbound bind and
  resolves to a `SubmitResp` (`command_status`, `message_id`). It errors if the
  named bind isn't currently bound.
- Config readouts: `smpp.config()` (the full dict), `smpp.bind_address()` (the
  listener `host:port`), `smpp.binds()` (outbound bind descriptors),
  `smpp.routing_rules()` (`(default_chain, rules)`).
- `@smpp.on_session("bound" | "unbound")` is available as a lifecycle hook.

---

## Config

`SmppConfig` is loaded from its own YAML file, kept separate from siphon's main
config so siphon needn't know the addon's schema at compile time. Reference it
from siphon's main config under the `extensions` map:

```yaml
# siphon.yaml (main config)
extensions:
  smpp: /etc/siphon/smpp.yaml
```

```yaml
# /etc/siphon/smpp.yaml

# Inbound SMPP listener (external ESMEs bind to us).
server:
  bind_address: "0.0.0.0"
  port: 2775
  session_init_timer_ms: 5000      # default
  enquire_link_timer_ms: 30000     # default
  inactivity_timer_ms: 300000      # default (5 min)
  response_timer_ms: 30000         # default
  # tls:
  #   cert_path: /etc/siphon/tls/smpp.crt
  #   key_path:  /etc/siphon/tls/smpp.key
  #   ca_path:   /etc/siphon/tls/ca.crt

# Outbound binds (we bind out to remote SMSCs / aggregators).
binds:
  - name: aggregator-eu            # referenced by submit_via(bind="aggregator-eu")
    host: smpp.example-aggregator.com
    port: 2775
    system_id: my-esme
    password: ${SMPP_AGG_PASSWORD}   # ${VAR} / ${VAR:-default} expansion supported
    system_type: ""                  # optional; many aggregators ignore it
    bind_type: transceiver           # transmitter | receiver | transceiver (default)
    max_msg_per_sec: 100             # 0 = unlimited
    enquire_link_timer_ms: 30000
    response_timer_ms: 30000
    # tls:
    #   cert_path: ...

# Optional declarative routing, read by the script via smpp.routing_rules().
routing:
  default_chain: ["bind:aggregator-eu", "queue"]
  rules:
    - prefix: "31"                   # E.164 prefix without leading '+'; longest-prefix wins
      name: nl-fixed
      chain: ["bind:aggregator-eu"]
```

Outbound binds can also be declared entirely via environment variables, useful
for secrets and per-deployment overrides. A bind named `aggregator-eu` is
discovered from the presence of its `_HOST` var:

```bash
SMPP_BIND_AGGREGATOR_EU_HOST=smpp.example-aggregator.com
SMPP_BIND_AGGREGATOR_EU_PORT=2775
SMPP_BIND_AGGREGATOR_EU_SYSTEM_ID=my-esme
SMPP_BIND_AGGREGATOR_EU_PASSWORD=s3cr3t
SMPP_BIND_AGGREGATOR_EU_BIND_TYPE=transceiver      # optional
SMPP_BIND_AGGREGATOR_EU_SYSTEM_TYPE=               # optional
SMPP_BIND_AGGREGATOR_EU_MAX_MPS=100                # optional, 0 = unlimited
SMPP_BIND_AGGREGATOR_EU_ENQUIRE_LINK_MS=30000      # optional
SMPP_BIND_AGGREGATOR_EU_RESPONSE_MS=30000          # optional
SMPP_BIND_AGGREGATOR_EU_TLS=true                   # optional
```

The `<NAME>` segment is uppercased in the env var and lowercased to form the
bind name; names must not contain underscores. `SMPP_DEFAULT_CHAIN` overrides
`routing.default_chain`. Env-var binds are merged with any declared in the file.

---

## Rust vs. Python boundary

| Concern | Owned by Rust (this crate + `smpp34`) | Owned by Python (your script) |
|---|---|---|
| TCP / TLS framing | ✅ | |
| SMPP 3.4 PDU codec | ✅ | |
| Sequence-number windowing | ✅ | |
| Bind / enquire_link / inactivity / response timers | ✅ | |
| Outbound bind reconnect + backoff | ✅ | |
| Bind authentication (accept/reject) | | ✅ `@smpp.on_bind` |
| Message routing / destination selection | | ✅ `@smpp.on_pdu` / `submit_via` |
| Message persistence / queueing | | ✅ |
| Throttling policy | | ✅ |
| Assigning the `message_id` | | ✅ |
| Choosing the `command_status` to return | | ✅ |

The rule of thumb: if it's on the wire or on a clock, it's Rust; if it's a
decision, it's Python.

---

## Dependencies

- **[siphon](https://github.com/siphon-project/siphon)** (`siphon-sip`) — the
  host platform. Pinned to a git revision for now (PyO3 0.29; the pin must track
  siphon-sip's, since both link the `python` native library and Cargo allows
  only one version of a `links` crate per graph).
- **[`smpp34`](https://crates.io/crates/smpp34)** — the pure-Rust SMPP 3.4 codec
  and async client/server.

---

## License

MIT © SIPhon Contributors. See [LICENSE](LICENSE).
