# siphon-smpp

**An SMPP 3.4 addon for [siphon](https://github.com/siphon-project/siphon-sip) — build a full SMSC in Python.**

<p>
  <a href="https://github.com/siphon-project/siphon-smpp/actions/workflows/ci.yaml">
    <img src="https://github.com/siphon-project/siphon-smpp/actions/workflows/ci.yaml/badge.svg" alt="CI">
  </a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/Rust-1.80%2B-000000?logo=rust&logoColor=white" alt="Rust 1.80+">
  <img src="https://img.shields.io/badge/SMPP-3.4-blue" alt="SMPP 3.4">
</p>

📖 **Documentation: [smpp.siphon-sip.org](https://smpp.siphon-sip.org)** — concepts,
quickstart, configuration, the SMSC cookbook, the script API, and Kubernetes /
scaling guides.

`siphon-smpp` plugs an `smpp` namespace into a siphon binary so your scripts can
speak **SMPP** — binds, `submit_sm`, `deliver_sm`, delivery receipts, `data_sm`,
`cancel_sm`, `alert_notification` — with the same hot-reloaded, decorator-style
Python siphon uses everywhere. It gives you enough surface to write a **full
store-and-forward SMSC / SMS gateway** on top, while keeping every socket,
timer and codec byte in Rust.

The boundary: **Rust owns the wire** (TCP/TLS framing, the SMPP codec,
sequence-number windowing, keep-alive/response timers, reconnect-with-backoff,
inbound + outbound throttling); **Python owns policy** (which binds to accept and *why*,
where to route, how to correlate a DLR, what status to return). Scripts never
touch a socket.

> **Built on [`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34) — the
> pure-Rust SMPP 3.4 codec and async client/server, provided by
> [Real Time Telecom B.V.](https://github.com/Real-Time-Telecom-B-V)**

---

## What it is

`siphon-smpp` is a **library**, not a standalone server — it runs as an
extension inside a [siphon](https://github.com/siphon-project/siphon-sip) binary
that you build. It provides:

- the `smpp` Python module your scripts `from siphon import smpp` (decorators,
  helper pyclasses, and the `submit_via` / `deliver_to` / … send helpers); and
- a tokio-side SMPP runtime — an inbound SMPP server plus one supervised
  outbound bind per configured peer — that dispatches each PDU into the matching
  script handler.

A siphon binary composes it in at startup; see the siphon documentation for how
extensions are wired into a binary. To **deploy** an SMSC built on it, see
[`deploy/`](deploy/) (Dockerfile, docker-compose, Kubernetes HA templates).

It speaks **two directions**, both described in terms of *binds*:

- **Inbound binds** — external ESMEs connect to siphon-smpp's listener
  (`server.bind_address`/`server.port`). They `bind_transceiver`, authorised by
  `@smpp.on_bind`, then send us `submit_sm` / `data_sm` / `cancel_sm`; we can
  `deliver_sm` / `data_sm` / `alert_notification` back to them by `session_id`.
  `bind_transmitter` / `bind_receiver` are rejected — transceiver only. Inbound
  message PDUs are rate-limited by an optional per-session
  `server.max_msg_per_sec` token bucket — the ingress mirror of a bind's
  outbound cap — either pacing the response or rejecting with `ESME_RTHROTTLED`
  per `server.throttle_action`.
- **Outbound binds** — siphon-smpp binds out as an ESME to remote SMSCs /
  aggregators (the `binds:` config list). We `submit_sm` / `data_sm` /
  `cancel_sm` out via `*_via(bind="<name>", …)`; they send us `deliver_sm`
  (incl. delivery receipts), `data_sm` and `alert_notification`. Each outbound
  bind is supervised: connect, hold, and on disconnect reconnect with
  exponential backoff (capped at 60s, reset after a healthy session), paced by
  an optional per-bind `max_msg_per_sec` token bucket.

Throttling is symmetric: outbound sends are paced per bind (`max_msg_per_sec`
on each `binds:` entry), inbound submits are rate-limited per ESME session
(`server.max_msg_per_sec`). Outbound is always a pure speed limit (delay, never
reject). Inbound picks its over-rate behaviour with `server.throttle_action`:
`pace` (default — delay the response, backpressuring through the ESME's window)
or `reject` (answer immediately with `ESME_RTHROTTLED`, the SMPP-native
back-off signal).

---

## Building an SMSC

The headline example, [`examples/gateway.py`](examples/gateway.py), is a worked
commodity SMS gateway: credential-checked binds, prefix routing to outbound
binds, store-and-forward **DLR correlation routed back to the originating
ESME**, MO-reply routing, and `alert_notification` handling — all in ~200 lines
of pure-SMPP Python. [`examples/echo.py`](examples/echo.py) is the hello-world
(accept any bind, echo every `submit_sm`).

What the **crate** gives you vs. what your **script** owns:

| The crate owns | Your script owns |
|---|---|
| TCP/TLS framing, SMPP codec (`smpp34`) | who may bind, and the reject reason |
| bind / enquire_link / inactivity / response timers | routing (which bind a destination takes) |
| sequence windowing, PDU dispatch | DLR correlation + routing back to the ESME |
| outbound bind supervision + reconnect | store-and-forward queue, retries |
| per-bind outbound + per-session inbound throttling | throttling *policy*, persistence |

Rule of thumb: **on the wire or on a clock → Rust; a decision → Python.**

---

## Script API

```python
from siphon import smpp, cache, log

# ── Authorise binds, with an explicit reason ────────────────────────────
@smpp.on_bind
async def authorise(bind):
    expected = await cache.get(f"esme_pw:{bind.system_id}")
    if expected is None:
        return bind.reject("ESME_RINVSYSID", f"unknown system_id {bind.system_id!r}")
    if bind.password != expected:
        return bind.reject("ESME_RINVPASWD", "bad password")
    return bind.accept()

# ── Track sessions so we can MT back to a bound ESME ────────────────────
@smpp.on_session("bound")
async def bound(session):
    if session.kind == "esme":
        await cache.set(f"esme_session:{session.system_id}", session.session_id)

# ── MO from an inbound ESME → forward over an outbound bind ─────────────
@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    resp = await smpp.submit_via(
        bind="aggregator-eu",
        source_addr=pdu.source_addr,
        destination_addr=pdu.destination_addr,
        short_message=pdu.short_message,        # bytes
        data_coding=pdu.data_coding,
        registered_delivery=pdu.registered_delivery,
    )
    return pdu.reply(message_id=resp.message_id)

# ── deliver_sm back on an outbound bind: DLR or MO ──────────────────────
@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    if pdu.is_dlr:
        r = pdu.receipt or {}                   # {id, stat, err, submit_date, …, raw}
        # …look up the originating ESME session by r["id"] and route back…
        await smpp.deliver_to(session_id=esme_session,
                              source_addr=pdu.destination_addr,
                              destination_addr=pdu.source_addr,
                              short_message=pdu.short_message,
                              esm_class=0x04)    # delivery receipt
    return pdu.reply()                          # ESME_ROK ack
```

Key points:

- **`@smpp.on_bind`** receives a `Bind` (`system_id`, `password`,
  `client_addr`). Return `bind.accept()` or `bind.reject(status, reason)` — the
  reason is logged on the reject. A bare truthy/falsy return still works. **With
  no handler, binds are rejected — closed by default.**
- **`@smpp.on_pdu("<command>")`** handlers receive `(pdu, session)` and cover
  `submit_sm`, `submit_sm_multi`, `deliver_sm`, `data_sm`, `cancel_sm`,
  `query_sm`, `replace_sm`, and `alert_notification` (first arg is an
  `AlertNotification`). The `Pdu` mirrors the SMPP 3.4 fields (`source_addr`,
  `destination_addr`, `esm_class`, `data_coding`, `short_message` as `bytes`,
  `is_tpdu`, …; `submit_sm_multi` carries the address list in `pdu.destinations`).
  For `deliver_sm`, `pdu.is_dlr` flags a delivery receipt and `pdu.receipt` is the
  parsed receipt dict (`id`, `stat`, `err`, `submit_date`, `done_date`, `text`,
  `raw`).
- **`Session`** carries `kind` (`"esme"` inbound / `"bind"` outbound),
  `session_id`, `system_id`, `client_addr`. `deliver_to` / `data_to` /
  `alert_to` target a bound ESME by `session_id`.
- **Replies**: `pdu.reply(message_id="…")` to accept, `pdu.reply(command_status=
  "ESME_RSUBMITFAIL")` to reject, `pdu.reply()` / `None` for a default
  `ESME_ROK` ack. Unknown status strings raise immediately.
- **`@smpp.on_session("bound" | "unbound")`** fires for both inbound ESME and
  outbound bind lifecycle; the handler receives a `Session`.
- **Send helpers** (all `await`): most return an `SmppResp` (`command_status`,
  `message_id`, `ok`); `query_via` returns a `QueryResp` (`message_state`,
  `final_date`, `error_code`).
  - outbound, by bind name: `submit_via`, `submit_multi_via`, `data_via`,
    `cancel_via`, `query_via`, `replace_via`;
  - inbound, by `session_id`: `deliver_to`, `data_to`, `alert_to`.
- **Config readouts**: `smpp.config()`, `smpp.bind_address()`, `smpp.binds()`,
  `smpp.routing_rules()`.
- **Hot reload**: handlers are resolved from the registry on every PDU, so
  editing your script (and letting siphon reload it) takes effect on the next
  message — no restart, no rebind.

---

## SMPP operation coverage

Full SMPP 3.4 operation coverage — every meaningful PDU dispatches to a script
handler with a sensible default. Built on `smpp34` 1.2.

### Inbound — an ESME binds to us (server)

| PDU | Dispatched to | Default (no handler) | |
|---|---|---|---|
| `bind_transceiver` | `@smpp.on_bind` | reject (closed by default) | ✅ |
| `bind_transmitter` / `bind_receiver` | — | reject `ESME_RINVSYSID` (transceiver only) | ✅ |
| `submit_sm` | `@smpp.on_pdu("submit_sm")` | `ESME_ROK` ack | ✅ |
| `submit_sm_multi` | `@smpp.on_pdu("submit_sm_multi")` (`pdu.destinations`) | reject `ESME_RSYSERR` | ✅ |
| `data_sm` | `@smpp.on_pdu("data_sm")` | reject `ESME_RSYSERR` | ✅ |
| `cancel_sm` | `@smpp.on_pdu("cancel_sm")` | reject `ESME_RCANCELFAIL` | ✅ |
| `query_sm` | `@smpp.on_pdu("query_sm")` → `pdu.reply_query(...)` | reject `ESME_RQUERYFAIL` | ✅ |
| `replace_sm` | `@smpp.on_pdu("replace_sm")` | reject `ESME_RREPLACEFAIL` | ✅ |
| `enquire_link` | runtime (keep-alive) | auto-ack | ✅ |
| `unbind` | runtime + `@smpp.on_session("unbound")` | accept | ✅ |

### Outbound — we bind to a remote SMSC (client)

| PDU | Dispatched to | Default (no handler) | |
|---|---|---|---|
| `deliver_sm` (incl. **DLR**) | `@smpp.on_pdu("deliver_sm")` | `ESME_ROK` ack | ✅ |
| `data_sm` | `@smpp.on_pdu("data_sm")` | reject `ESME_RSYSERR` | ✅ |
| `alert_notification` | `@smpp.on_pdu("alert_notification")` | no-op | ✅ |

### Send helpers

| Helper | Direction | Backed by | |
|---|---|---|---|
| `submit_via` | → outbound bind | `smpp34` `SMSC::submit_sm` | ✅ |
| `submit_multi_via` | → outbound bind | `SMSC::send_submit_sm_multi` | ✅ |
| `data_via` | → outbound bind | `SMSC::send_data_sm` | ✅ |
| `cancel_via` | → outbound bind | `SMSC::send_cancel_sm` | ✅ |
| `query_via` | → outbound bind | `SMSC::send_query_sm` | ✅ |
| `replace_via` | → outbound bind | `SMSC::send_replace_sm` | ✅ |
| `deliver_to` | → bound ESME (`session_id`) | `ESME::send_deliver_sm` | ✅ |
| `data_to` | → bound ESME | `ESME::send_data_sm` | ✅ |
| `alert_to` | → bound ESME | `ESME::send_alert_notification` | ✅ |

---

## Config

`SmppConfig` is loaded from its own YAML file, separate from siphon's main
config. Reference it from siphon's main config under `extensions`:

```yaml
# siphon.yaml (main config)
extensions:
  smpp: /etc/siphon/smpp.yaml
```

```yaml
# /etc/siphon/smpp.yaml
server:                              # inbound listener (ESMEs bind to us)
  bind_address: "0.0.0.0"
  port: 2775
  session_init_timer_ms: 5000        # default
  enquire_link_timer_ms: 30000       # default
  inactivity_timer_ms: 300000        # default (5 min)
  response_timer_ms: 30000           # default
  max_msg_per_sec: 200               # inbound throttle, per ESME session; 0 = unlimited
  throttle_action: pace              # over-rate: pace (default) | reject (ESME_RTHROTTLED)
  # tls: { cert_path: …, key_path: …, ca_path: … }

binds:                               # outbound binds (we bind to remote SMSCs)
  - name: aggregator-eu             # referenced by submit_via(bind="aggregator-eu")
    host: smpp.example-aggregator.com
    port: 2775
    system_id: my-esme
    password: ${SMPP_AGG_PASSWORD}   # ${VAR} / ${VAR:-default} expansion
    bind_type: transceiver           # transmitter | receiver | transceiver (default)
    max_msg_per_sec: 100             # outbound throttle; 0 = unlimited

routing:                            # optional; read via smpp.routing_rules()
  default_chain: ["bind:aggregator-eu", "queue"]
  rules:
    - prefix: "31"                   # E.164 prefix (no '+'); longest-prefix wins
      name: nl
      chain: ["bind:aggregator-eu"]
```

Outbound binds can also be declared entirely via environment variables (handy
for secrets). A bind named `aggregator-eu` is discovered from its `_HOST` var:

```bash
SMPP_BIND_AGGREGATOR_EU_HOST=smpp.example-aggregator.com
SMPP_BIND_AGGREGATOR_EU_PORT=2775
SMPP_BIND_AGGREGATOR_EU_SYSTEM_ID=my-esme
SMPP_BIND_AGGREGATOR_EU_PASSWORD=s3cr3t
SMPP_BIND_AGGREGATOR_EU_MAX_MPS=100        # optional, 0 = unlimited
```

The `<NAME>` segment is uppercased in the env var and lowercased to form the
bind name; names must not contain underscores. `SMPP_DEFAULT_CHAIN` overrides
`routing.default_chain`; `SMPP_SERVER_MAX_MPS` and
`SMPP_SERVER_THROTTLE_ACTION` override `server.max_msg_per_sec` and
`server.throttle_action` (the inbound throttle). Env-var binds merge with any
declared in the file. See
[`deploy/smpp.example.yaml`](deploy/smpp.example.yaml) for an annotated config.

---

## Performance

Two layers, benched separately: the per-message Rust work this crate adds
(codec, below), and the end-to-end SMSC path under load (the [`harness/`](harness/)).

### Codec — per-message Rust work (`cargo bench`)

The SMPP wire codec itself is benched in `smpp34`'s own suite; siphon-smpp's
benches ([`benches/codec.rs`](benches/codec.rs)) cover the work this crate adds on
top. Indicative single-core numbers:

| Path | Time |
|---|---|
| wire PDU → script `Pdu` (`from_deliver` / `from_submit`) | ~32 ns |
| delivery-receipt parse (`Receipt::parse`) | ~0.53 µs |
| `deliver_sm` → `Pdu` → receipt parse (full DLR path) | ~0.56 µs |
| `smpp.yaml` parse (boot / hot-reload) | ~9 µs |

A counting-allocator [leak check](examples/leak_check.rs)
(`./scripts/mem_leak_test.sh`) hammers those paths and asserts **live bytes stay
flat** (Δ 0 over 10 cycles × 200k iterations). Both run in CI.

### End-to-end — live siphon + Python dispatch (the [`harness/`](harness/))

The harness floods `submit_sm` at a real `siphon --features smpp` running
[`bench_echo.py`](harness/bench_echo.py) (accept-bind + ack-`submit_sm`, no I/O),
so it measures the whole path — bind → `smpp34` decode → Python handler dispatch →
`submit_sm_resp` — and nothing else. Numbers below are illustrative (loopback, a
24-core developer laptop, in-memory registrar, siphon-smpp v1.1.0 via siphon-bin),
not lab-grade — reproduce your own with the scripts.

**Single bind** — `./harness/bench.sh` (100k `submit_sm`, sweeping the SMPP window):

| window | submit_sm/s | p50 | p90 | p99 | p999 |
|---:|---:|---:|---:|---:|---:|
| 1   | 7,505  | 0.10 ms | 0.23 ms | 0.29 ms | 0.44 ms |
| 8   | 11,007 | 0.67 ms | 1.09 ms | 1.56 ms | 2.07 ms |
| 32  | 10,910 | 2.91 ms | 3.44 ms | 4.07 ms | 5.10 ms |
| 64  | 11,225 | 5.69 ms | 6.56 ms | 7.74 ms | 9.60 ms |
| 128 | 11,383 | 11.30 ms | 12.80 ms | 14.15 ms | 17.42 ms |
| 256 | 10,936 | 23.57 ms | 26.42 ms | 28.87 ms | 30.28 ms |
| 512 | 11,063 | 46.70 ms | 52.20 ms | 56.43 ms | 58.46 ms |

**~11k `submit_sm/s` through one bind at sub-millisecond p50 (0.10 ms at window 1).**
Throughput is flat across the window, so latency past the knee is pure queueing
(Little's law: p50 ≈ window ÷ throughput).

**Aggregate** — `./harness/bench_multi.sh` (N parallel binds, window 64):

| binds | aggregate submit_sm/s | per-bind |
|---:|---:|---:|
| 1  | 6,808  | 6,808 |
| 4  | 6,909  | 1,727 |
| 8  | 7,530  | 941 |
| 16 | 10,912 | 682 |
| 24 | 11,595 | 483 |

Aggregate barely moves as binds are added, with the host mostly idle — the ceiling
is the **per-message Python handler body**, not the SMPP path (the same harness
against a Rust-only mock SMSC does ~138k/s) and not I/O (the echo touches none).
That handler runs in CPython, and on a standard (GIL) interpreter it serializes to
roughly one core's throughput no matter how many binds or cores you have.

Two ways past it: keep per-message handler work minimal (push heavy lifting into
Rust), and — the real unlock — run siphon against **free-threaded CPython**
(3.13t / 3.14t), which this whole stack (`smpp34` included) already targets. There
the handler body runs on every core, so aggregate scales with binds instead of
flatlining: early 3.14t runs here lifted it close to an order of magnitude (to
~10⁵ `submit_sm/s`) on the same box. Point `bench_multi.sh` at a free-threaded
build to measure your own.

---

## Dependencies

- **[`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34)** — the
  pure-Rust SMPP 3.4 wire codec and async client/server. Provided by
  **[Real Time Telecom B.V.](https://github.com/Real-Time-Telecom-B-V)** (MIT,
  on [crates.io](https://crates.io/crates/smpp34)). siphon-smpp is a thin,
  scriptable layer over it.
- **[siphon](https://github.com/siphon-project/siphon-sip)** (`siphon-sip`) — the
  host platform. Pinned to a git revision for now (PyO3 0.29; the pin must track
  siphon-sip's, since both link the `python` native library and Cargo allows
  only one version of a `links` crate per graph).

---

## Development

```bash
cargo test                 # unit + integration tests
cargo clippy --all-targets --all-features -- -D warnings
cargo bench                # criterion benches
./scripts/mem_leak_test.sh # live-bytes leak check (PASS/FAIL)
cargo deny check           # advisories, licenses, bans, sources
```

---

## License

MIT © SIPhon Contributors. See [LICENSE](LICENSE).
