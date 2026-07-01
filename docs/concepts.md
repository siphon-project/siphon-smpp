# Concepts & architecture

siphon-smpp is a **library** that adds an `smpp` namespace and an SMPP runtime to
a [SIPhon](https://siphon-sip.org/) binary. This page explains the model you
program against: the two bind directions, what runs in Rust vs. Python, how
dispatch and hot-reload work, and where state lives.

## The boundary: Rust owns the wire, Python owns policy

Everything that is *hard to get right and never changes per deployment* is in
Rust; everything that is a *decision* is in your Python script. Scripts never
touch a socket.

```
                      ┌──────────── your SMSC (a SIPhon binary) ─────────────┐
   ESMEs   ──bind──▶  │  siphon-smpp runtime (Rust):                         │  ──bind──▶  upstream
  (apps)   ◀─deliver─ │    • inbound listener (server)                       │  ◀─deliver─   SMSCs
                      │    • outbound binds (client, reconnect + throttle)   │           (aggregators)
                      │    • SMPP codec, timers, sequence windowing          │
                      │    • dispatch ──▶ your smpp.py handlers (Python)     │
                      └──────────────────────────────────────────────────────┘
                                     your script decides:
                              auth · routing · DLR correlation · queueing
```

| The crate owns (Rust) | Your script owns (Python) |
|---|---|
| TCP/TLS framing, the SMPP 3.4 codec ([`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34)) | who may bind, and the reject reason |
| bind / enquire_link / inactivity / response timers | routing — which outbound bind a destination takes |
| sequence windowing, PDU dispatch | DLR correlation + routing receipts back to the ESME |
| outbound bind supervision + reconnect-with-backoff | the store-and-forward queue, retries, persistence |
| throttling — per-bind outbound + per-session inbound (token buckets) | throttling *policy* |

Rule of thumb: **on the wire or on a clock → Rust; a decision → Python.**

## Two directions, both "binds"

SMPP is symmetric-ish: the same PDUs flow in both directions depending on who
initiated the TCP connection. siphon-smpp models both as *binds*.

### Inbound binds — ESMEs connect to you (you are the server)

External ESMEs (applications, other gateways) open a TCP connection to your
listener (`server.bind_address` / `server.port`) and `bind_transceiver`. Your
[`@smpp.on_bind`](script-api.md#on_bind) handler authorises them. Once bound they
send you:

- `submit_sm`, `submit_sm_multi` — mobile-originated / application traffic
- `data_sm` — the alternate submit
- `cancel_sm`, `query_sm`, `replace_sm` — message management

You push messages back to a bound ESME by its `session_id` with
[`deliver_to` / `data_to` / `alert_to`](script-api.md#send-helpers).

Inbound message PDUs are rate-limited per session by an optional
`server.max_msg_per_sec` token bucket — the ingress mirror of a bind's outbound
cap. Over-rate, the runtime either paces the response or answers
`ESME_RTHROTTLED`, per `server.throttle_action`
([Configuration → Throttling](configuration.md#throttling)).

!!! note "Transceiver only"
    `bind_transmitter` and `bind_receiver` are rejected — siphon-smpp is
    transceiver-only inbound. One bidirectional session per ESME keeps the
    session model (and your routing) simple.

### Outbound binds — you connect to upstream (you are the client)

For each entry in the `binds:` config list, siphon-smpp opens its **own** TCP
connection out to a remote SMSC / aggregator and binds as an ESME. You send
traffic out over a named bind with the [`*_via`](script-api.md#send-helpers)
helpers (`submit_via(bind="aggregator-eu", …)`), and the upstream sends you back
`deliver_sm` (including delivery receipts), `data_sm`, and `alert_notification`.

Each outbound bind is **supervised**: connect, hold the session open (answering
`enquire_link`), and on disconnect reconnect with exponential backoff (capped at
60 s, reset after a healthy session). An optional per-bind `max_msg_per_sec`
token bucket paces outbound submits.

## Dispatch & hot-reload

Every inbound PDU is decoded by Rust and dispatched to the matching handler,
resolved **from the script registry on every PDU**. That's the key to hot-reload:

1. A PDU arrives, Rust decodes it into a [`Pdu`](script-api.md#pdu).
2. The runtime looks up the handler registered for that command *right now*.
3. Your handler runs on SIPhon's Python runtime and returns a reply (or `None`
   for the default ack).

Because the lookup happens per-PDU, editing your `smpp.py` and letting SIPhon
reload it takes effect on the **next message** — no restart, no rebind, no
dropped sessions. Keep handlers free of import-time side effects so a reload is
safe mid-traffic.

If no handler is registered for a command, siphon-smpp applies a **sensible
default** (e.g. `ESME_ROK` for `submit_sm`, reject for `cancel_sm`, and — for
binds — *reject, closed by default*). See the full
[operation coverage](operations.md) table.

## Where state lives

Handlers are just functions; the interesting state (DLR correlation, the
`system_id → session_id` map, your outbound queue) has to survive two things
every real deployment hits:

- **Script hot-reload** — a module-level `dict` is wiped when the script
  reloads. In-flight DLR correlations would be lost.
- **Multiple workers** — SIPhon can run the handler body across worker threads
  (and, on free-threaded CPython, across cores), and across replicas in
  production. A per-process dict isn't shared.

The examples key correlation and session maps in **`siphon.cache`**, a
SIPhon-provided shared store, for exactly this reason. In a clustered deployment
that store must be shared across replicas too, because a delivery receipt can
come back on *any* replica's outbound bind — see
[Kubernetes & scaling](kubernetes.md#outbound-you-upstream).

## The runtime, briefly

- **`SmppServerListener`** — the inbound server. Accepts connections, runs the
  bind handshake through `@smpp.on_bind`, then dispatches `submit`/`data`/
  `cancel`/`query`/`replace` to your handlers.
- **`SmppClientListener`** — one per outbound bind. Owns the reconnect-with-
  backoff supervisor and dispatches inbound `deliver`/`data`/`alert` from that
  peer.
- **`RateLimiter`** — token buckets enforcing `max_msg_per_sec`: per-bind on
  outbound submits, and per-session on inbound message PDUs (with
  `throttle_action` deciding pace vs. `ESME_RTHROTTLED`).

You don't program these directly — you configure them
([Configuration](configuration.md)) and write the handlers they call
([Script API](script-api.md)).

## Next

- [Quickstart](quickstart.md) — run an echo SMSC and drive load at it.
- [Building an SMSC gateway](cookbook/smsc-gateway.md) — the full worked example.
- [Script API](script-api.md) — the complete handler + helper reference.
