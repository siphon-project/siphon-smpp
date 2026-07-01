# Load-test harness

A standalone SMPP load driver (`smpp-load`) for an SMSC built on siphon-smpp,
plus a mock SMSC so you can smoke-test the driver — and CI — without standing up
siphon. It's its own little workspace (only `smpp34` + tokio + clap), so it
builds fast and doesn't pull the siphon stack.

Both ends speak `smpp34` — the same wire library siphon-smpp uses — so the load
path is faithful to production.

## Self-test (no siphon)

```bash
./run.sh self-test                       # mock SMSC + 50k submit_sm, window 64
COUNT=200000 WINDOW=128 ./run.sh self-test
```

This is also the CI smoke test: it bind→submit→resp round-trips through real
`smpp34` framing and fails (non-zero exit) on any submit error. Sample output:

```
── results ──────────────────────────────
  submitted : 30000  ok 30000  errors 0
  elapsed   : 0.182s
  throughput: 164451 submit_sm/s
  latency   : p50 0.27ms  p90 0.42ms  p99 0.49ms  p999 0.65ms  max 41.12ms
```

(The mock is a trivial in-process ack, so these numbers measure the *driver* and
the loopback — not a real SMSC. Use the real flow below for meaningful numbers.)

## Real load test (against a siphon-bin SMSC)

This is the thing the harness exists for — load-test the actual siphon + smpp
dispatch path, and it doubles as the end-to-end SMPP smoke test for the
`siphon-bin` integration.

1. **Build the SMPP-enabled siphon binary** (from the siphon-sip repo's
   `siphon-bin` package):
   ```bash
   cargo build -p siphon-bin --release --features smpp
   ```

2. **Point siphon at the echo script + this addon config.** In your
   `siphon.yaml`, load `examples/echo.py` as the script and add:
   ```yaml
   extensions:
     smpp: harness/smpp.yaml      # the inbound listener on :2775
   ```
   `examples/echo.py` accepts any bind and acks every `submit_sm` with a
   generated message_id — the minimal path that still exercises bind auth, PDU
   decode, script dispatch, and the response.

3. **Run it**, then drive load:
   ```bash
   ./siphon -c siphon.yaml &                       # your SMPP-enabled siphon
   ./run.sh drive --host 127.0.0.1 --port 2775 \
       --count 1000000 --window 128
   ```

Swap `examples/echo.py` for `examples/gateway.py` (with outbound binds + a mock
upstream `serve`) to load-test the full store-and-forward path including DLR
correlation.

### One-shot: `bench.sh`

`bench.sh` automates the whole thing — start siphon, wait for the listener,
sweep a range of SMPP windows, and print a markdown table for the top-level
README. It uses `bench_echo.py` (a logging-free variant of `echo.py` with a
constant `message_id`) and `siphon.bench.yaml` (a minimal SMPP-only siphon
config with `log.level: warn`), so the numbers reflect the dispatch path, not
log I/O.

```bash
# Option A — you already have an SMPP-enabled siphon binary:
SIPHON_BIN=/path/to/siphon ./bench.sh

# Option B — build it from a siphon-sip checkout (needs PR #20 merged):
SIPHON_SIP_DIR=~/workspace/siphon-sip ./bench.sh

# Knobs: COUNT=2000000 WINDOWS="32 128 512" ./bench.sh
```

It measures the full path — TCP accept → `smpp34` decode → `@smpp.on_pdu`
dispatch → reply → `submit_sm_resp` — i.e. the real siphon + smpp number, not
the driver/loopback ceiling that `self-test` reports. (`siphon-bin --features
smpp` shipped in siphon-sip
[#20](https://github.com/siphon-project/siphon-sip/pull/20).)

### Aggregate across binds: `bench_multi.sh`

`bench.sh` drives one bind (the per-bind ceiling + latency curve). A real SMSC
serves many ESMEs at once — `bench_multi.sh` launches N parallel binds against
one siphon and reports the aggregate `submit_sm/s` (sum of each bind's sustained
rate), sweeping the bind count:

```bash
SIPHON_BIN=/path/to/siphon BINDLIST="1 4 8 16 24" ./bench_multi.sh
```

On a standard (GIL) CPython build, aggregate barely rises with binds: the
per-message Python handler body serializes on the GIL. Build siphon against a
**free-threaded** interpreter (`PYO3_PYTHON=python3.14t cargo build …`, run with
that `libpython3.14t` on `LD_LIBRARY_PATH`) and the same sweep scales across
cores — near an order of magnitude here (free-threaded CPython support is still
stabilising, so treat it as experimental). `bench_echo_io.py` +
`siphon.bench.io.yaml` are a diagnostic variant whose handler `await`s a
simulated I/O roundtrip, to show the ceiling is CPU-under-GIL, not event-loop
concurrency.

## `smpp-load` reference

```
smpp-load drive  --host H --port P --count N --window W
                 [--system-id ID] [--password PW]
                 [--source-addr A] [--destination-addr B] [--body-len N]
smpp-load serve  --host H --port P        # mock SMSC (accept + ack)
```

`drive` binds one transceiver ESME, keeps `--window` submits in flight, and
reports throughput + submit→resp latency percentiles (p50/p90/p99/p999/max).
All addresses default to synthetic `555-01xx` numbers.
