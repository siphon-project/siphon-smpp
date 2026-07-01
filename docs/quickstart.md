# Quickstart

Stand up the smallest possible SMSC — an echo that accepts any bind and acks
every `submit_sm` — then drive load at it. This is the fastest way to see the
whole path work: bind → decode → your Python handler → response.

!!! note "You bring the SIPhon binary"
    siphon-smpp is a library, not a server. It runs inside a
    [SIPhon](https://siphon-sip.org/) binary built with the `smpp` addon
    registered. See [Using it in a SIPhon build](integration.md) for how the
    addon gets composed in; this page assumes you have that binary (call it
    `siphon`). The load harness below can also self-test with **no SIPhon at
    all**.

## 1. The echo handler

This is [`examples/echo.py`](https://github.com/siphon-project/siphon-smpp/blob/main/examples/echo.py)
— the hello-world SMSC:

```python
import uuid
from siphon import smpp, log

@smpp.on_bind
async def authorise(bind):
    # Wide open — fine for a local echo, NEVER for anything real.
    log.info(f"bind from {bind.system_id} @ {bind.client_addr}")
    return bind.accept()

@smpp.on_pdu("submit_sm")
async def echo(pdu, session):
    message_id = uuid.uuid4().hex[:12]
    log.info(f"submit_sm {session.system_id}: "
             f"{pdu.source_addr} -> {pdu.destination_addr} "
             f"({len(pdu.short_message)} bytes) => {message_id}")
    return pdu.reply(message_id=message_id)
```

Two handlers: one authorises binds (here, everyone), one acks `submit_sm` with a
freshly minted `message_id`. That's a valid, if trivial, SMSC.

## 2. The addon config

siphon-smpp reads its own YAML, separate from SIPhon's main config. A minimal
inbound-only listener:

```yaml
# smpp.yaml
server:
  bind_address: "0.0.0.0"
  port: 2775
```

Reference it from your main `siphon.yaml`, and point SIPhon's script at
`echo.py`:

```yaml
# siphon.yaml
extensions:
  smpp: smpp.yaml
```

See [Configuration](configuration.md) for every knob (outbound binds, routing,
TLS, timers, env-var injection).

## 3. Run it

```bash
./siphon -c siphon.yaml
```

The listener is now up on `0.0.0.0:2775`. Bind a transceiver, `submit_sm`, and
you get an `ESME_ROK` + `message_id` back. Edit `echo.py`, save, and SIPhon
hot-reloads the handlers — the next PDU uses the new code, no restart.

## 4. Drive load at it

The repo ships a standalone load driver,
[`harness/`](https://github.com/siphon-project/siphon-smpp/blob/main/harness/README.md).
Both ends speak the same `smpp34` wire library siphon-smpp uses, so the load path
is faithful to production.

**Self-test — no SIPhon needed** (mock SMSC + driver, also the CI smoke test):

```bash
cd harness
./run.sh self-test                       # mock SMSC + 50k submit_sm, window 64
COUNT=200000 WINDOW=128 ./run.sh self-test
```

**Against your running echo SMSC:**

```bash
cd harness
./run.sh drive --host 127.0.0.1 --port 2775 --count 1000000 --window 128
```

`drive` binds one transceiver ESME, keeps `--window` submits in flight, and
reports throughput plus submit→resp latency percentiles (p50/p90/p99/p999/max).

For a repeatable sweep of the SMPP window (and a markdown table you can paste),
use `./bench.sh`; for the aggregate across many parallel binds, `./bench_multi.sh`.
See [Performance & load testing](performance.md) for what the numbers mean and
how to reproduce them.

## Next

- **Do something real** → [Building an SMSC gateway](cookbook/smsc-gateway.md):
  credential-checked binds, prefix routing, DLR correlation back to the ESME.
- **Understand the model** → [Concepts & architecture](concepts.md).
- **All the knobs** → [Configuration](configuration.md) and the
  [Script API](script-api.md).
- **Ship it** → [Deployment](deployment.md) and
  [Kubernetes & scaling](kubernetes.md).
