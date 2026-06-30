"""
siphon.smpp — SMPP 3.4 namespace.

Imported by user scripts as `from siphon import smpp`. Decorators register
handlers; the Rust runtime (siphon-smpp's `task()`) reads them via
`ScriptHandle::handlers_for(...)` and dispatches PDUs / lifecycle events
into them.

Handler kinds:
  * "smpp.on_bind"     — single-arg decorator (no filter); returns
                         `bind.accept()` / `bind.reject(status, reason)`
                         (or bare truthy/falsy).
  * "smpp.on_pdu"      — `command` is supplied as `options.command` so
                         the Rust dispatcher can match by command name
                         ("submit_sm", "deliver_sm", "data_sm",
                         "cancel_sm", "alert_notification").
  * "smpp.on_session"  — `event` is supplied as `options.event`
                         ("bound" / "unbound") — lifecycle hook.

Hot reload: handlers are resolved from the registry on EVERY PDU /
event, so editing your script (and letting siphon reload it) takes effect
on the next message — no restart, no rebind. Keep handlers idempotent and
free of import-time side effects.

Send helpers (all async — `await` them):
  Outbound, target a bind by name:
    * submit_via(bind=…, source_addr=…, destination_addr=…, short_message=…, **fields)
    * submit_multi_via(bind=…, source_addr=…, destinations=[…], short_message=…, **fields)
    * data_via(bind=…, source_addr=…, destination_addr=…, **fields)
    * cancel_via(bind=…, message_id=…, **fields)
    * query_via(bind=…, message_id=…, **fields) -> QueryResp
    * replace_via(bind=…, message_id=…, short_message=…, **fields)
  Inbound, target a bound ESME by session_id:
    * deliver_to(session_id=…, source_addr=…, destination_addr=…, short_message=…, **fields)
    * data_to(session_id=…, source_addr=…, destination_addr=…, **fields)
    * alert_to(session_id=…, source_addr=…, esme_addr=…, **fields)
All are attached as Rust pyfunctions at namespace-init time.

Pyclasses (`Pdu`, `PduReply`, `Session`, `Bind`, `BindResult`,
`AlertNotification`, `SmppResp`, `QueryResp`) are attached to the module
by Rust; they're listed at the bottom for IDE / docs.
"""

import asyncio


# `_siphon_registry` is created by siphon's script engine when it
# loads the user script, *after* this namespace module has been
# constructed. Importing at module-load time would fail with
# ModuleNotFoundError; the decorator functions defer the import to
# call time (when the script is being parsed and the registry is
# already in sys.modules).

def _registry():
    import _siphon_registry as _r
    return _r


# ── Decorators ──────────────────────────────────────────────────────────

def on_bind(fn):
    """Authorise an SMPP bind. Receives a `Bind` (system_id, password,
    client_addr) and returns:

        return bind.accept()                       # authorise
        return bind.reject("ESME_RINVPASWD", "bad password")   # deny + why
        return bind.reject("ESME_RINVSYSID", "unknown esme")

    A bare truthy/falsy return still works (truthy = accept). With no
    @smpp.on_bind handler at all, the default is REJECT — binds are
    closed by default, scripts must explicitly authorise them. The
    `reason` is logged on the reject for operator visibility."""
    _registry().register("smpp.on_bind", None, fn,
                         asyncio.iscoroutinefunction(fn), None)
    return fn


def on_pdu(command):
    """
    Register a handler for a specific SMPP command.

    `command` is the PDU command name as a string:
      * "submit_sm"          — MO from an inbound ESME (server side)
      * "submit_sm_multi"    — one submit to many destinations
                               (pdu.destinations is the address list)
      * "deliver_sm"         — MT / MO / **DLR** from an outbound bind
      * "data_sm"            — TLV-based message, either direction
      * "cancel_sm"          — cancel request from an inbound ESME
                               (pdu.message_id + addressing)
      * "query_sm"           — message-state query from an inbound ESME
                               (reply with pdu.reply_query(...))
      * "replace_sm"         — replace request from an inbound ESME
                               (pdu.message_id + new pdu.short_message)
      * "alert_notification" — MS-available alert from an outbound bind

    Handler signature: `(pdu, session)` (for "alert_notification" the
    first arg is an `AlertNotification`). Return either:
      * `pdu.reply(message_id="…")` — accept (submit_sm path)
      * `pdu.reply(command_status="ESME_RSUBMITFAIL")` — reject
      * `pdu.reply()` — accept with default ESME_ROK (deliver_sm path)
      * `pdu.reply_query(message_state=2, final_date="…", error_code=0)`
        — query_sm success (message_state 1=ENROUTE … 8=REJECTED)
      * `None` — same as bare `pdu.reply()`

    For "deliver_sm", check `pdu.is_dlr`; if set, `pdu.receipt` is the
    parsed delivery-receipt dict (id, stat, err, …) — route it back to
    the originating ESME with `await smpp.deliver_to(...)`.

    The Rust dispatcher matches on `options.command`.
    """
    def decorator(fn):
        _registry().register("smpp.on_pdu", None, fn,
                             asyncio.iscoroutinefunction(fn),
                             {"command": command})
        return fn
    return decorator


def on_session(event):
    """Lifecycle hook: `event` is "bound" or "unbound".

    Handler signature: `(session)`. Fires when an inbound ESME binds /
    unbinds (`session.kind == "esme"`) and when an outbound bind comes
    up / goes down (`session.kind == "bind"`). Use it to maintain a
    system_id → session_id map for MT routing, emit metrics, or flush
    queues. The return value is ignored."""
    def decorator(fn):
        _registry().register("smpp.on_session", event, fn,
                             asyncio.iscoroutinefunction(fn),
                             {"event": event})
        return fn
    return decorator


# ── Cfg readouts ────────────────────────────────────────────────────────
# `_config` is set by the Rust install closure (siphon_smpp::namespace).
# Shape: {server: {...}, binds: [{...}], routing: {default_chain, rules}}

def bind_address():
    """Listening address, e.g. "0.0.0.0:2775" — useful for /healthz."""
    s = _config["server"]  # noqa: F821
    return f"{s['bind_address']}:{s['port']}"


def config():
    """Read-only view of the addon config as a dict.

    Used by routing logic to read `routing` rules; can also walk
    `binds` for diagnostics."""
    return dict(_config)  # noqa: F821


def binds():
    """List of outbound bind descriptors. Each is a dict with at
    least `name`, `host`, `port`, `system_id`, `bind_type`."""
    return list(_config.get("binds", []))  # noqa: F821


def routing_rules():
    """Returns `(default_chain, rules)` as `(list[str], list[dict])`."""
    r = _config.get("routing", {})  # noqa: F821
    return r.get("default_chain", []), r.get("rules", [])


# ── Pyclasses (attached by the Rust install closure) ───────────────────
#
# These names are populated by siphon_smpp::namespace() before the
# script runs:
#
#   Pdu               — passed into @on_pdu handlers; fields + .message_id
#                        + .reply() / .reply_query() + .is_dlr / .receipt
#   PduReply          — what .reply() / .reply_query() return (you usually
#                        don't construct these directly)
#   Session           — passed into @on_pdu / @on_session;
#                        .kind / .session_id / .system_id / .client_addr
#   Bind              — passed into @on_bind;
#                        .system_id / .password / .client_addr
#                        + .accept() / .reject(status, reason)
#   BindResult        — what bind.accept()/reject() return
#   AlertNotification — passed into @on_pdu("alert_notification");
#                        .source_addr / .esme_addr / .ms_availability_status
#   SmppResp          — return value from most send helpers
#                        (.command_status / .message_id / .ok)
#   QueryResp         — return value from query_via
#                        (.message_state / .final_date / .error_code / .ok)
