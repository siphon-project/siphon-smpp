"""
siphon.smpp — SMPP 3.4 namespace.

Imported by user scripts as `from siphon import smpp`. Decorators write
into `_siphon_registry` (siphon ≥ a290cc4 HandlerKind::Custom); the
Rust side (siphon-smpp's `task()`) reads handlers via
`ScriptHandle::handlers_for("smpp.on_pdu")` and dispatches PDUs into
them.

Handler kinds:
  * "smpp.on_bind"     — single-arg decorator (no filter); returns
                         truthy to accept, falsy to reject.
  * "smpp.on_pdu"      — `command` is supplied as `options.command` so
                         the Rust dispatcher can match by command name
                         (`"submit_sm"`, `"deliver_sm"`, etc.).
  * "smpp.on_session"  — filter is "bound" / "unbound" — lifecycle hook.

Outbound submission goes through `await submit_via(bind=…, …)` —
exposed as a Rust pyfunction installed at namespace-init time.

Pyclasses (`Pdu`, `PduReply`, `Session`, `Bind`, `SubmitResp`) are
attached to the module by Rust; they're listed here for IDE / docs.
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
    client_addr); returns truthy to accept, falsy to reject. Default
    if no @smpp.on_bind handler is registered: REJECT (closed by
    default — scripts must explicitly authorise binds)."""
    _registry().register("smpp.on_bind", None, fn,
                         asyncio.iscoroutinefunction(fn), None)
    return fn


def on_pdu(command):
    """
    Register a handler for a specific SMPP command.

    `command` is the PDU command name as a string: "submit_sm" (server
    side, MO from an external ESME), "deliver_sm" (client/bind side,
    MT from an aggregator), "data_sm", etc.

    Handler signature: `(pdu, session)`. Return either:
      * `pdu.reply(message_id="…")` — accept (submit_sm path)
      * `pdu.reply(command_status="ESME_RSUBMITFAIL")` — reject
      * `pdu.reply()` — accept with default ESME_ROK (deliver_sm path)
      * `None` — same as bare `pdu.reply()`

    The Rust dispatcher matches on `options.command`, so the kind
    filter is None and the discriminator is in options.
    """
    def decorator(fn):
        _registry().register("smpp.on_pdu", None, fn,
                             asyncio.iscoroutinefunction(fn),
                             {"command": command})
        return fn
    return decorator


def on_session(event):
    """Lifecycle hook: `event` is "bound" or "unbound"."""
    def decorator(fn):
        _registry().register("smpp.on_session", event, fn,
                             asyncio.iscoroutinefunction(fn), None)
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

    Used by routing.py to read `routing` rules; can also walk
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
#   Pdu          — passed into @on_pdu handlers, has fields + .reply()
#   PduReply     — what .reply() returns (you usually don't construct
#                   these directly, just call pdu.reply(...))
#   Session      — passed into @on_pdu, .kind/.session_id/.system_id
#   Bind         — passed into @on_bind, .system_id/.password/.client_addr
#   SubmitResp   — return value from submit_via
#
# `submit_via` — async function, see below — also attached.
