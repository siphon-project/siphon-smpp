"""
gateway.py — a commodity SMPP store-and-forward gateway on siphon-smpp.

A worked example of an SMSC-shaped service that lives entirely in the
SMPP world: ESMEs bind to us, we relay their MO traffic out over
configured outbound binds to upstream SMSCs/aggregators, and we route
the delivery receipts (and mobile-originated replies) back to the right
ESME.

    ┌────────┐   submit_sm    ┌─────────────────┐   submit_sm   ┌──────────┐
    │  ESME  │ ─────────────▶ │  this gateway   │ ────────────▶ │ upstream │
    │ (apps) │ ◀───────────── │  (siphon-smpp)  │ ◀──────────── │   SMSC   │
    └────────┘  deliver_sm     └─────────────────┘  deliver_sm   └──────────┘
                (DLR / MO)                          (DLR / MO)

What the CRATE owns (you don't write this):
  * TCP framing + the SMPP 3.4 PDU codec (via smpp34)
  * bind / enquire_link / inactivity timers, sequence windowing
  * outbound bind supervision: connect, bind, reconnect-with-backoff
  * per-bind outbound throttling (max_msg_per_sec) and per-session
    inbound throttling (server.max_msg_per_sec)
  * dispatching each PDU into the handlers below

What YOUR SCRIPT owns (this file):
  * credential policy (who may bind, and why a bind is refused)
  * routing (which outbound bind a destination takes)
  * the store-and-forward correlation needed to route DLRs back
  * retry / queueing / persistence policy beyond a single hop

State note: correlation + the ESME session map live in `siphon.cache`,
a siphon-provided shared store. That matters for two reasons every real
deployment hits: it survives **script hot-reload** (edit this file and
in-flight DLR correlations are not lost), and it's shared across
**worker processes**. A module-level dict would lose both. Swap in
whatever shared store your siphon build exposes.

All identifiers/numbers here are synthetic (RFC 5737 / 555-01xx ranges).
"""

import json
import uuid

from siphon import smpp, log, cache


# ── Credentials ─────────────────────────────────────────────────────────
# Demo credentials. In production resolve these against your own store
# (and rotate them). Closed by default: anything not listed is refused.
_CREDENTIALS = {
    "acme":   "s3cr3t",
    "globex": "hunter2",
}


@smpp.on_bind
async def authorise(bind):
    expected = _CREDENTIALS.get(bind.system_id)
    if expected is None:
        return bind.reject("ESME_RINVSYSID", f"unknown system_id {bind.system_id!r}")
    if bind.password != expected:
        return bind.reject("ESME_RINVPASWD", f"bad password for {bind.system_id!r}")
    log.info(f"bind authorised: {bind.system_id} @ {bind.client_addr}")
    return bind.accept()


# ── Session tracking ────────────────────────────────────────────────────
# Maintain a system_id <-> session_id map so we can MT-deliver to a bound
# ESME by its system_id, and clean up on unbind.

@smpp.on_session("bound")
async def on_bound(session):
    if session.kind == "esme":
        await cache.set(f"esme_session:{session.system_id}", session.session_id)
        await cache.set(f"esme_system:{session.session_id}", session.system_id)
        log.info(f"esme bound: {session.system_id} (session {session.session_id})")
    else:  # kind == "bind" — one of our outbound binds came up
        log.info(f"outbound bind up: {session.system_id}")


@smpp.on_session("unbound")
async def on_unbound(session):
    if session.kind == "esme":
        await cache.delete(f"esme_session:{session.system_id}")
        await cache.delete(f"esme_system:{session.session_id}")
        log.info(f"esme unbound: {session.system_id}")
    else:
        log.warning(f"outbound bind down: {session.system_id}")


# ── Routing ─────────────────────────────────────────────────────────────
# Pick an outbound bind for a destination using the declarative routing
# rules from smpp.yaml: longest-matching prefix wins, else the default
# chain, else the first configured bind. A chain step of the form
# "bind:<name>" names an outbound bind.

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


# ── MO: submit_sm from an inbound ESME ──────────────────────────────────

@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    bind = _pick_bind(pdu.destination_addr)
    if bind is None:
        log.error(f"no route for {pdu.destination_addr}")
        return pdu.reply(command_status="ESME_RINVDSTADR")

    # The id we hand back to the ESME. We keep our own id space so a DLR
    # can be correlated back regardless of what the upstream SMSC assigns.
    our_id = uuid.uuid4().hex[:12]

    try:
        resp = await smpp.submit_via(
            bind=bind,
            source_addr=pdu.source_addr,
            source_addr_ton=pdu.source_addr_ton,
            source_addr_npi=pdu.source_addr_npi,
            destination_addr=pdu.destination_addr,
            dest_addr_ton=pdu.dest_addr_ton,
            dest_addr_npi=pdu.dest_addr_npi,
            short_message=pdu.short_message,
            esm_class=pdu.esm_class,
            data_coding=pdu.data_coding,
            registered_delivery=pdu.registered_delivery,
        )
    except Exception as e:  # bind not up, upstream nack, timeout, …
        log.error(f"submit via {bind} failed: {e}")
        return pdu.reply(command_status="ESME_RSUBMITFAIL")

    log.info(
        f"MO {session.system_id}: {pdu.source_addr} -> {pdu.destination_addr} "
        f"via {bind} (upstream id {resp.message_id}) as {our_id}"
    )

    # If the ESME asked for a receipt, remember how to route it back.
    # Key by the UPSTREAM id, because that's what the DLR will carry.
    if pdu.registered_delivery and resp.message_id:
        correlation = {
            "esme_session": session.session_id,
            "esme_system": session.system_id,
            "our_id": our_id,
            "source_addr": pdu.source_addr,
            "destination_addr": pdu.destination_addr,
        }
        await cache.set(f"dlr:{bind}:{resp.message_id}", json.dumps(correlation))

    return pdu.reply(message_id=our_id)


# ── Inbound deliver_sm from an outbound bind (DLR or MO reply) ───────────

@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    if pdu.is_dlr:
        await _route_dlr(pdu, session)
    else:
        await _route_mo_reply(pdu, session)
    return pdu.reply()  # ack the upstream


async def _route_dlr(pdu, session):
    receipt = pdu.receipt or {}
    upstream_id = receipt.get("id", "")
    raw = json.loads(await cache.get(f"dlr:{session.system_id}:{upstream_id}") or "null")
    if not raw:
        log.warning(f"DLR for unknown upstream id {upstream_id!r} on {session.system_id}")
        return

    esme_session = raw["esme_session"]
    # Rebuild a receipt body carrying OUR id (the one the ESME knows).
    body = (
        f"id:{raw['our_id']} sub:001 dlvrd:001 "
        f"submit date:{receipt.get('submit_date', '')} "
        f"done date:{receipt.get('done_date', '')} "
        f"stat:{receipt.get('stat', 'UNKNOWN')} err:{receipt.get('err', '000')} "
        f"text:{receipt.get('text', '')}"
    ).encode()

    try:
        await smpp.deliver_to(
            session_id=esme_session,
            source_addr=raw["destination_addr"],
            destination_addr=raw["source_addr"],
            short_message=body,
            esm_class=0x04,  # delivery receipt
        )
        log.info(f"DLR routed to {raw['esme_system']}: {receipt.get('stat')}")
    except Exception as e:
        log.error(f"DLR delivery to {raw['esme_system']} failed: {e}")
    finally:
        await cache.delete(f"dlr:{session.system_id}:{upstream_id}")


async def _route_mo_reply(pdu, session):
    # A mobile-originated message inbound from upstream (e.g. a reply to
    # a two-way short code). Route it to whichever ESME owns the
    # destination. Here we use a simple owner map; a real gateway would
    # consult its number/short-code routing table.
    owner = await cache.get(f"msisdn_owner:{pdu.destination_addr}")
    if not owner:
        log.warning(f"inbound MO for unrouted destination {pdu.destination_addr}")
        return
    esme_session = await cache.get(f"esme_session:{owner}")
    if not esme_session:
        log.warning(f"owner {owner} for {pdu.destination_addr} is not bound; dropping/queueing")
        return
    try:
        await smpp.deliver_to(
            session_id=esme_session,
            source_addr=pdu.source_addr,
            source_addr_ton=pdu.source_addr_ton,
            source_addr_npi=pdu.source_addr_npi,
            destination_addr=pdu.destination_addr,
            short_message=pdu.short_message,
            esm_class=pdu.esm_class,
            data_coding=pdu.data_coding,
        )
        log.info(f"MO {pdu.source_addr} -> {pdu.destination_addr} delivered to {owner}")
    except Exception as e:
        log.error(f"MO delivery to {owner} failed: {e}")


# ── alert_notification from an outbound bind ────────────────────────────

@smpp.on_pdu("alert_notification")
async def on_alert(alert, session):
    # An upstream SMSC signalling that a previously-unavailable MS
    # (alert.source_addr) is now reachable. The cue to flush any MT you
    # queued for it.
    log.info(
        f"alert_notification on {session.system_id}: "
        f"{alert.source_addr} available — flush queued MT"
    )


# ── cancel_sm from an inbound ESME ──────────────────────────────────────

@smpp.on_pdu("cancel_sm")
async def on_cancel(pdu, session):
    # We don't keep a cancellable spool in this example, so refuse with a
    # clear status. A gateway with a real outbound queue would dequeue
    # the matching message and `await smpp.cancel_via(...)` upstream.
    log.info(f"cancel_sm from {session.system_id} (not supported by this gateway)")
    return pdu.reply(command_status="ESME_RCANCELFAIL")
