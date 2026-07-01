# Script API

Everything your handler script can call, imported from the `smpp` namespace:

```python
from siphon import smpp, cache, log
```

There are four kinds of thing here: [decorators](#decorators) that register
handlers, the [objects](#objects) handlers receive (`Pdu`, `Bind`, `Session`,
`AlertNotification`), the [send helpers](#send-helpers) that push PDUs out, and
[config readouts](#config-readouts). Handlers are `async def` and are resolved
from the registry on every PDU, so edits [hot-reload](concepts.md#dispatch-hot-reload).

## Decorators

### `@smpp.on_bind` { #on_bind }

Authorises an inbound bind. Receives a [`Bind`](#bind); return `bind.accept()` or
`bind.reject(status, reason)`. A bare truthy/falsy return also works.

```python
@smpp.on_bind
async def authorise(bind):
    expected = await cache.get(f"esme_pw:{bind.system_id}")
    if expected is None:
        return bind.reject("ESME_RINVSYSID", f"unknown system_id {bind.system_id!r}")
    if bind.password != expected:
        return bind.reject("ESME_RINVPASWD", "bad password")
    return bind.accept()
```

!!! warning "Closed by default"
    With **no** `@smpp.on_bind` handler registered, all binds are **rejected**.
    You must opt ESMEs in.

### `@smpp.on_pdu("<command>")`

Registers a handler for an inbound PDU command. The handler receives
`(pdu, session)`. Supported commands:

`submit_sm`, `submit_sm_multi`, `deliver_sm`, `data_sm`, `cancel_sm`,
`query_sm`, `replace_sm`, `alert_notification`.

```python
@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    return pdu.reply(message_id="abc123")
```

For `alert_notification` the first argument is an
[`AlertNotification`](#alertnotification), not a `Pdu`. If no handler is
registered for a command, the [default](operations.md) applies.

### `@smpp.on_session("bound" | "unbound")`

Fires on session lifecycle transitions, for **both** inbound ESMEs and outbound
binds. Receives a [`Session`](#session). Use it to maintain your
`system_id → session_id` map so you can MT-deliver back later.

```python
@smpp.on_session("bound")
async def bound(session):
    if session.kind == "esme":
        await cache.set(f"esme_session:{session.system_id}", session.session_id)

@smpp.on_session("unbound")
async def unbound(session):
    if session.kind == "esme":
        await cache.delete(f"esme_session:{session.system_id}")
```

## Objects

### `Pdu`

The script-side view of an SMPP message, mirroring the SMPP 3.4 fields. Common
fields:

| Field | Type | Notes |
|---|---|---|
| `source_addr` / `destination_addr` | `str` | Addresses. |
| `source_addr_ton` / `source_addr_npi` | `int` | Type-of-number / numbering-plan. |
| `dest_addr_ton` / `dest_addr_npi` | `int` | Same for the destination. |
| `short_message` | `bytes` | The message payload (raw bytes, not decoded text). |
| `esm_class` | `int` | ESM class bits (`0x04` = delivery receipt, etc.). |
| `data_coding` | `int` | DCS. |
| `registered_delivery` | `int` | Whether the sender wants a receipt. |
| `message_id` | `str` | Present on responses / management PDUs. |
| `destinations` | `list` | `submit_sm_multi` only — the address list. |
| `is_dlr` | `bool` | `deliver_sm` only — true if it's a delivery receipt. |
| `receipt` | `dict` \| `None` | `deliver_sm` only — the parsed receipt (below). |
| `is_tpdu` | `bool` | Whether the payload looks like a GSM TPDU. |

**Replies** (return the result from your handler):

| Call | Effect |
|---|---|
| `pdu.reply(message_id="…")` | Accept with a message id (`ESME_ROK`). |
| `pdu.reply(command_status="ESME_RSUBMITFAIL")` | Reject with a status. |
| `pdu.reply()` or returning `None` | Default `ESME_ROK` ack. |
| `pdu.reply_query(message_state=…, final_date=…, error_code=…)` | Answer a `query_sm`. |

Unknown status strings raise immediately, so a typo fails fast rather than
silently sending the wrong status.

**The `receipt` dict** (for `deliver_sm` where `is_dlr`):

```python
{ "id": "...", "stat": "DELIVRD", "err": "000",
  "submit_date": "...", "done_date": "...", "text": "...", "raw": "..." }
```

### `Bind`

Passed to `@smpp.on_bind`.

| Field / method | Meaning |
|---|---|
| `system_id` | The ESME's claimed identity. |
| `password` | The bind password to verify. |
| `client_addr` | The peer's network address (for allow-listing / logging). |
| `bind.accept()` | Accept the bind. |
| `bind.reject(status, reason)` | Reject; `reason` is logged. |

### `Session`

Passed to session/PDU handlers.

| Field | Meaning |
|---|---|
| `kind` | `"esme"` (an inbound ESME) or `"bind"` (one of your outbound binds). |
| `session_id` | Stable id for the session; the target for `deliver_to` / `data_to` / `alert_to`. |
| `system_id` | The peer's system id. |
| `client_addr` | The peer's network address. |

### `AlertNotification`

First argument to `@smpp.on_pdu("alert_notification")`. Carries `source_addr`
(and TON/NPI) of the now-available MS, and `esme_addr`. It's a notification —
there's nothing to reply.

## Send helpers

All send helpers are `await`able. **Outbound** helpers target an outbound bind by
`bind=` name; **inbound** helpers target a bound ESME by `session_id=`. Most
return an `SmppResp`; `query_via` returns a `QueryResp`.

### Outbound — to an upstream SMSC (by bind name)

| Helper | Sends | Returns |
|---|---|---|
| `submit_via(bind, …)` | `submit_sm` | `SmppResp` |
| `submit_multi_via(bind, destinations=[…], …)` | `submit_sm_multi` | `SmppResp` |
| `data_via(bind, …)` | `data_sm` | `SmppResp` |
| `cancel_via(bind, …)` | `cancel_sm` | `SmppResp` |
| `query_via(bind, message_id=…, …)` | `query_sm` | `QueryResp` |
| `replace_via(bind, message_id=…, …)` | `replace_sm` | `SmppResp` |

```python
resp = await smpp.submit_via(
    bind="aggregator-eu",
    source_addr=pdu.source_addr,
    destination_addr=pdu.destination_addr,
    short_message=pdu.short_message,      # bytes
    data_coding=pdu.data_coding,
    registered_delivery=pdu.registered_delivery,
)
# resp.ok, resp.command_status, resp.message_id
```

### Inbound — to a bound ESME (by session_id)

| Helper | Sends |
|---|---|
| `deliver_to(session_id, …)` | `deliver_sm` (MT or a delivery receipt with `esm_class=0x04`) |
| `data_to(session_id, …)` | `data_sm` |
| `alert_to(session_id, …)` | `alert_notification` |

```python
await smpp.deliver_to(
    session_id=esme_session,
    source_addr=pdu.destination_addr,
    destination_addr=pdu.source_addr,
    short_message=receipt_body,           # bytes
    esm_class=0x04,                       # delivery receipt
)
```

### Response objects

| Type | Fields |
|---|---|
| `SmppResp` | `ok` (bool), `command_status` (str), `message_id` (str) |
| `QueryResp` | `message_state`, `final_date`, `error_code` |

Send helpers raise on hard failures (bind not up, timeout); check `resp.ok` /
`resp.command_status` for a soft nack from the peer. Wrap outbound sends in
`try/except` and translate failures into a reply status for the originating ESME
— see the [gateway example](cookbook/smsc-gateway.md#mo-submit_sm-from-an-esme).

## Config readouts

Read (never write) the loaded [configuration](configuration.md):

| Call | Returns |
|---|---|
| `smpp.config()` | The full parsed config. |
| `smpp.bind_address()` | The inbound listener address. |
| `smpp.binds()` | The list of configured outbound binds (`[{name, host, …}]`). |
| `smpp.routing_rules()` | `(default_chain, rules)` for your routing logic. |

## Testing your scripts

You can unit-test SMPP scripts without a running SMSC. The
[`siphon-sip` SDK](https://pypi.org/project/siphon-sip/) (`pip install
siphon-sip`) mocks the `smpp` namespace — the same decorators, PDU objects, and
send helpers described above — and ships an `SmppTestHarness` that dispatches
binds, PDUs, and lifecycle events into your handlers so you can assert on the
replies and on what your script sent back:

```python
from siphon_sdk.smpp_testing import SmppTestHarness

def test_gateway_authorises_and_submits():
    harness = SmppTestHarness()
    harness.load_script("examples/gateway.py")

    # bind_transceiver → @smpp.on_bind
    assert harness.bind("esme1", password="s3cret")

    # submit_sm → @smpp.on_pdu("submit_sm")
    reply = harness.submit_sm(source_addr="15550100",
                              destination_addr="15550101",
                              short_message=b"hi")
    assert reply.ok

    # a DLR delivered on an outbound bind is routed back to the ESME
    harness.deliver_sm(esm_class=0x04,
                       short_message=b"id:msg-1 stat:DELIVRD err:000")
    assert harness.sent[0][0] == "deliver_to"
```

The mock also gives IDEs and LLMs the full type hints and docstrings for the
namespace, which helps when authoring scripts. The mock tracks this crate's
runtime surface — CI (`scripts/check_sdk_parity.py`) fails if they drift.

## Hot reload, restated

Handlers are looked up per-PDU, so editing your script (and letting SIPhon reload
it) takes effect on the next message — no restart, no rebind. Keep handlers free
of import-time side effects, and keep cross-message state in
[`siphon.cache`](concepts.md#where-state-lives), not module globals, so a reload
mid-traffic is safe.
