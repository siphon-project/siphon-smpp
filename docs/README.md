# siphon-smpp

**An SMPP 3.4 addon for [SIPhon](https://siphon-sip.org/) — build a full
store-and-forward SMSC in hot-reloaded Python, with every socket, timer and
codec byte in Rust.**

`siphon-smpp` plugs an `smpp` namespace into a SIPhon binary so your scripts can
speak **SMPP** — binds, `submit_sm`, `deliver_sm`, delivery receipts, `data_sm`,
`cancel_sm`, `alert_notification` — with the same decorator-style, hot-reloaded
Python SIPhon uses everywhere. It gives you enough surface to write a **full
store-and-forward SMSC / SMS gateway** on top.

```python
from siphon import smpp, cache

@smpp.on_bind
async def authorise(bind):
    if bind.password != await cache.get(f"pw:{bind.system_id}"):
        return bind.reject("ESME_RINVPASWD", "bad password")
    return bind.accept()

@smpp.on_pdu("submit_sm")
async def relay(pdu, session):
    resp = await smpp.submit_via(bind="aggregator-eu",
                                 source_addr=pdu.source_addr,
                                 destination_addr=pdu.destination_addr,
                                 short_message=pdu.short_message,
                                 registered_delivery=pdu.registered_delivery)
    return pdu.reply(message_id=resp.message_id)
```

That's a credential-checked, MO-relaying SMSC front end. The full worked gateway
— routing, DLR correlation back to the originating ESME, MO replies — is in the
[Cookbook](cookbook/smsc-gateway.md).

## The boundary

**Rust owns the wire; Python owns policy.** Scripts never touch a socket.

| The crate owns (Rust) | Your script owns (Python) |
|---|---|
| TCP/TLS framing, the SMPP codec ([`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34)) | who may bind, and the reject reason |
| bind / enquire_link / inactivity / response timers | routing (which bind a destination takes) |
| sequence windowing, PDU dispatch | DLR correlation + routing back to the ESME |
| outbound bind supervision + reconnect-with-backoff | store-and-forward queue, retries, persistence |
| throttling — per-bind outbound + per-session inbound | throttling *policy* |

Rule of thumb: **on the wire or on a clock → Rust; a decision → Python.**

## Where to start

<div class="grid cards" markdown>

- **[Concepts & architecture](concepts.md)** — the two-direction bind model and
  what runs where.
- **[Quickstart](quickstart.md)** — stand up an echo SMSC and drive load at it in
  a few minutes.
- **[Configuration](configuration.md)** — the `smpp.yaml` reference, env-var
  binds, routing rules.
- **[Cookbook: building an SMSC gateway](cookbook/smsc-gateway.md)** — the full
  worked store-and-forward gateway, line by line.
- **[Script API](script-api.md)** — decorators, PDU/session objects, and every
  send helper.
- **[Kubernetes & scaling](kubernetes.md)** — the SMPP failover model and how to
  run it HA.

</div>

## What it is (and isn't)

`siphon-smpp` is a **library**, not a standalone server. It runs as an extension
inside a [SIPhon](https://siphon-sip.org/) binary that you build and compose — see
[Using it in a SIPhon build](integration.md). There is no `siphon-smpp` daemon to
run on its own; to **deploy** an SMSC built on it, see
[Deployment](deployment.md) and [Kubernetes & scaling](kubernetes.md).

It speaks **two directions**, both described in terms of *binds*:

- **Inbound binds** — external ESMEs connect to your listener, `bind_transceiver`,
  and send you `submit_sm` / `data_sm` / `cancel_sm`; you `deliver_sm` back to
  them. (`bind_transmitter` / `bind_receiver` are rejected — transceiver only.)
- **Outbound binds** — siphon-smpp binds out as an ESME to remote SMSCs /
  aggregators. You `submit_sm` out; they send you `deliver_sm` (incl. delivery
  receipts), `data_sm`, `alert_notification`. Each outbound bind is supervised:
  connect, hold, reconnect with exponential backoff.

See [Concepts & architecture](concepts.md) for the full picture.

!!! info "Built on smpp34"
    The wire is [`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34) — the
    pure-Rust SMPP 3.4 codec and async client/server, provided by
    [Real Time Telecom B.V.](https://github.com/Real-Time-Telecom-B-V) (MIT, on
    [crates.io](https://crates.io/crates/smpp34)). siphon-smpp is a thin,
    scriptable layer over it.

## License

MIT. siphon-smpp is an addon for [SIPhon](https://siphon-sip.org/); need a hand
building on it? See [Commercial support](support.md).
