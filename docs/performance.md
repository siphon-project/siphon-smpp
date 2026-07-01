# Performance & load testing

Two layers, benched separately: the per-message **Rust work** this crate adds
(codec), and the **end-to-end SMSC path** under load (the load harness). This
page has both, plus how to reproduce and interpret them.

!!! note "Reproduce your own"
    The numbers below are illustrative — loopback, a developer laptop, an
    in-memory registrar. They're here to show *shape*, not to be a spec sheet.
    Every tool is in the repo; run it on your hardware with your handler.

## Codec — per-message Rust work (`cargo bench`)

The SMPP wire codec itself is benched in `smpp34`'s own suite; siphon-smpp's
[`benches/codec.rs`](https://github.com/siphon-project/siphon-smpp/blob/main/benches/codec.rs)
cover the work *this crate* adds on top. Indicative single-core numbers:

| Path | Time |
|---|---|
| wire PDU → script `Pdu` (`from_deliver` / `from_submit`) | ~32 ns |
| delivery-receipt parse (`Receipt::parse`) | ~0.53 µs |
| `deliver_sm` → `Pdu` → receipt parse (full DLR path) | ~0.56 µs |
| `smpp.yaml` parse (boot / hot-reload) | ~9 µs |

```bash
cargo bench                  # criterion benches
./scripts/mem_leak_test.sh   # live-bytes leak check (PASS/FAIL)
```

A counting-allocator [leak check](https://github.com/siphon-project/siphon-smpp/blob/main/examples/leak_check.rs)
hammers those paths and asserts **live bytes stay flat** (Δ 0 over 10 cycles ×
200k iterations). Both run in CI.

## End-to-end — live SIPhon + Python dispatch (the harness)

The [`harness/`](https://github.com/siphon-project/siphon-smpp/blob/main/harness/README.md)
floods `submit_sm` at a real `siphon --features smpp` running a minimal
echo handler (accept-bind + ack-`submit_sm`, no I/O), so it measures the **whole
path** — bind → `smpp34` decode → Python handler dispatch → `submit_sm_resp` —
and nothing else. Both ends speak the same `smpp34` wire library siphon-smpp uses.

### Single bind — window sweep

`./harness/bench.sh` (100k `submit_sm`, sweeping the SMPP window):

| window | submit_sm/s | p50 | p90 | p99 | p999 |
|---:|---:|---:|---:|---:|---:|
| 1   | 7,505  | 0.10 ms | 0.23 ms | 0.29 ms | 0.44 ms |
| 8   | 11,007 | 0.67 ms | 1.09 ms | 1.56 ms | 2.07 ms |
| 32  | 10,910 | 2.91 ms | 3.44 ms | 4.07 ms | 5.10 ms |
| 64  | 11,225 | 5.69 ms | 6.56 ms | 7.74 ms | 9.60 ms |
| 128 | 11,383 | 11.30 ms | 12.80 ms | 14.15 ms | 17.42 ms |
| 256 | 10,936 | 23.57 ms | 26.42 ms | 28.87 ms | 30.28 ms |
| 512 | 11,063 | 46.70 ms | 52.20 ms | 56.43 ms | 58.46 ms |

**~11k `submit_sm/s` through one bind at sub-millisecond p50** (0.10 ms at window
1). Throughput is flat across the window, so latency past the knee is pure
queueing (Little's law: p50 ≈ window ÷ throughput).

### Aggregate — many parallel binds

`./harness/bench_multi.sh` (N parallel binds, window 64):

| binds | aggregate submit_sm/s | per-bind |
|---:|---:|---:|
| 1  | 6,808  | 6,808 |
| 4  | 6,909  | 1,727 |
| 8  | 7,530  | 941 |
| 16 | 10,912 | 682 |
| 24 | 11,595 | 483 |

Aggregate barely moves as binds are added, with the host mostly idle. The ceiling
is the **per-message Python handler body**, not the SMPP path (the same harness
against a Rust-only mock SMSC does ~138k/s) and not I/O (the echo touches none).

## Scaling past the GIL

That flat aggregate is the GIL: on a standard CPython build, the per-message
Python handler serializes to roughly one core's throughput no matter how many
binds or cores you have. Two ways past it:

1. **Keep per-message handler work minimal** — push heavy lifting into Rust and
   `await` I/O rather than blocking.
2. **Run SIPhon against free-threaded CPython** (3.13t / 3.14t) — the real
   unlock. This whole stack (`smpp34` included) already targets it. There the
   handler body runs on every core, so aggregate scales with binds instead of
   flatlining: early 3.14t runs here lifted it close to an order of magnitude (to
   ~10⁵ `submit_sm/s`) on the same box.

```bash
# Build SIPhon against a free-threaded interpreter, then run bench_multi against it:
PYO3_PYTHON=python3.14t cargo build -p siphon-bin --release --features smpp
SIPHON_BIN=/path/to/siphon BINDLIST="1 4 8 16 24" ./harness/bench_multi.sh
```

Free-threaded CPython support is still stabilising — treat it as experimental.
For [Kubernetes](kubernetes.md#autoscaling-hpa-with-caveats) this is why you
scale replicas for **redundancy** first and throughput second: more pods don't
lift single-node throughput the way a free-threaded interpreter does.

## Using the harness

The harness is its own tiny workspace (only `smpp34` + tokio + clap), so it
builds fast without the SIPhon stack.

```bash
cd harness

# Self-test — no SIPhon (mock SMSC + driver); also the CI smoke test:
./run.sh self-test
COUNT=200000 WINDOW=128 ./run.sh self-test

# Drive load at a running SMSC:
./run.sh drive --host 127.0.0.1 --port 2775 --count 1000000 --window 128

# Repeatable window sweep (prints a markdown table):
SIPHON_BIN=/path/to/siphon ./bench.sh
COUNT=2000000 WINDOWS="32 128 512" ./bench.sh

# Aggregate across N parallel binds:
SIPHON_BIN=/path/to/siphon BINDLIST="1 4 8 16 24" ./bench_multi.sh
```

`drive` binds one transceiver ESME, keeps `--window` submits in flight, and
reports throughput + submit→resp latency percentiles. Swap the echo script for
[`gateway.py`](cookbook/smsc-gateway.md) (with a mock upstream) to load-test the
full store-and-forward path including DLR correlation. Full reference:
[`harness/README.md`](https://github.com/siphon-project/siphon-smpp/blob/main/harness/README.md).

## Interpreting your numbers

- **Latency past the knee is queueing, not work** — if throughput is flat while
  p50 climbs with the window, you're seeing Little's law, not slowdown. Lower the
  window for lower latency; it won't cost throughput.
- **If aggregate is flat and the host is idle**, you're GIL-bound in the handler
  — minimise handler work or move to free-threaded CPython.
- **If aggregate is flat and the host is busy**, profile the handler; you're
  doing real per-message work (JSON, crypto, sync I/O) that belongs in Rust or
  behind an `await`.
