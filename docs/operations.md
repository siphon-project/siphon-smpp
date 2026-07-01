# SMPP operation coverage

siphon-smpp has **full SMPP 3.4 operation coverage** — every meaningful PDU
dispatches to a script handler with a sensible default when you don't register
one. Built on [`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34) 1.2.

The tables below list, for each PDU, which [decorator](script-api.md#decorators)
it dispatches to and what happens with **no** handler registered.

## Inbound — an ESME binds to you (server)

| PDU | Dispatched to | Default (no handler) |
|---|---|---|
| `bind_transceiver` | `@smpp.on_bind` | **reject** — closed by default |
| `bind_transmitter` / `bind_receiver` | — | reject `ESME_RINVSYSID` (transceiver only) |
| `submit_sm` | `@smpp.on_pdu("submit_sm")` | `ESME_ROK` ack |
| `submit_sm_multi` | `@smpp.on_pdu("submit_sm_multi")` (`pdu.destinations`) | reject `ESME_RSYSERR` |
| `data_sm` | `@smpp.on_pdu("data_sm")` | reject `ESME_RSYSERR` |
| `cancel_sm` | `@smpp.on_pdu("cancel_sm")` | reject `ESME_RCANCELFAIL` |
| `query_sm` | `@smpp.on_pdu("query_sm")` → `pdu.reply_query(…)` | reject `ESME_RQUERYFAIL` |
| `replace_sm` | `@smpp.on_pdu("replace_sm")` | reject `ESME_RREPLACEFAIL` |
| `enquire_link` | runtime (keep-alive) | auto-ack |
| `unbind` | runtime + `@smpp.on_session("unbound")` | accept |

The defaults are deliberately conservative: message *management* PDUs
(`cancel`/`query`/`replace`) reject unless you implement them, because a silent
success would be a lie. `submit_sm` acks by default only so an echo works out of
the box — a real gateway always handles it.

!!! note "Throttling happens before dispatch"
    If a session exceeds `server.max_msg_per_sec` and `server.throttle_action` is
    `reject`, the runtime answers `ESME_RTHROTTLED` itself — your handler doesn't
    run for that PDU. With the default `pace` action it delays the response
    instead. See [Configuration → Throttling](configuration.md#throttling).

## Outbound — you bind to a remote SMSC (client)

These arrive on your [outbound binds](concepts.md#outbound-binds-you-connect-to-upstream-you-are-the-client).

| PDU | Dispatched to | Default (no handler) |
|---|---|---|
| `deliver_sm` (incl. **DLR**) | `@smpp.on_pdu("deliver_sm")` | `ESME_ROK` ack |
| `data_sm` | `@smpp.on_pdu("data_sm")` | reject `ESME_RSYSERR` |
| `alert_notification` | `@smpp.on_pdu("alert_notification")` | no-op |

`deliver_sm` is where **delivery receipts** come back:
[`pdu.is_dlr`](script-api.md#pdu) flags a receipt and `pdu.receipt` is the parsed
dict. Correlating that receipt back to the originating ESME is the heart of the
[gateway walkthrough](cookbook/smsc-gateway.md#deliver_sm-dlr-or-mo-reply).

## Send helpers

The other half — PDUs **you** originate. Outbound helpers target a bind by name;
inbound helpers target a bound ESME by `session_id`.

| Helper | Direction | Backed by (`smpp34`) |
|---|---|---|
| `submit_via` | → outbound bind | `SMSC::submit_sm` |
| `submit_multi_via` | → outbound bind | `SMSC::send_submit_sm_multi` |
| `data_via` | → outbound bind | `SMSC::send_data_sm` |
| `cancel_via` | → outbound bind | `SMSC::send_cancel_sm` |
| `query_via` | → outbound bind | `SMSC::send_query_sm` |
| `replace_via` | → outbound bind | `SMSC::send_replace_sm` |
| `deliver_to` | → bound ESME (`session_id`) | `ESME::send_deliver_sm` |
| `data_to` | → bound ESME | `ESME::send_data_sm` |
| `alert_to` | → bound ESME | `ESME::send_alert_notification` |

See the [Script API](script-api.md#send-helpers) for signatures and return types.

## Scope

siphon-smpp is **pure SMPP** — ESME ↔ SMSC, DLRs, message management,
store-and-forward. It is intentionally *not* an interworking gateway: it does not
translate SMPP to other protocols. That keeps the addon a clean, commodity SMPP
building block; higher-level interworking is out of scope and lives in your own
application logic above it.
