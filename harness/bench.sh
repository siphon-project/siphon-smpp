#!/usr/bin/env bash
# bench.sh — end-to-end siphon-smpp load test (ready for siphon-bin #20).
#
# Brings up a real `siphon-bin --features smpp` running the silent bench script,
# then sweeps `smpp-load drive` across a range of SMPP windows and prints a
# markdown table you can paste into the top-level README.
#
# Unlike `run.sh self-test` (which drives the in-process mock and only measures
# the driver + loopback), this measures the FULL path:
#   TCP accept -> smpp34 decode -> script dispatch (@smpp.on_pdu) -> reply -> resp
#
# Usage:
#   # Option A — point at a prebuilt binary:
#   SIPHON_BIN=/path/to/siphon ./bench.sh
#
#   # Option B — build it from a siphon-sip checkout (needs PR #20 merged):
#   SIPHON_SIP_DIR=~/workspace/siphon-sip ./bench.sh
#
# Knobs (env):
#   SIPHON_BIN       prebuilt siphon binary (skips the build)
#   SIPHON_SIP_DIR   siphon-sip checkout to build `siphon-bin --features smpp`
#   SIPHON_CONFIG    siphon config (default: harness/siphon.bench.yaml)
#   HOST PORT        SMPP listener to drive (default 127.0.0.1 2775)
#   COUNT            submits per window (default 1000000)
#   WINDOWS          space-separated window sweep (default "1 8 32 64 128 256")
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SIPHON_CONFIG="${SIPHON_CONFIG:-harness/siphon.bench.yaml}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-2775}"
COUNT="${COUNT:-1000000}"
WINDOWS="${WINDOWS:-1 8 32 64 128 256}"
SIPHON_LOG="${SIPHON_LOG:-${TMPDIR:-/tmp}/siphon-smpp-bench.log}"

# ── Resolve the siphon binary ────────────────────────────────────────────────
if [[ -z "${SIPHON_BIN:-}" ]]; then
  if [[ -n "${SIPHON_SIP_DIR:-}" ]]; then
    # siphon-bin is an excluded standalone workspace, so it builds from its own
    # dir (its own target/), and the [[bin]] artifact is named `siphon`.
    echo "[*] building siphon (siphon-bin --features smpp) in $SIPHON_SIP_DIR/siphon-bin …"
    ( cd "$SIPHON_SIP_DIR/siphon-bin" && cargo build --release --features smpp )
    SIPHON_BIN="$SIPHON_SIP_DIR/siphon-bin/target/release/siphon"
  else
    echo "error: set SIPHON_BIN=/path/to/siphon, or SIPHON_SIP_DIR=/path/to/siphon-sip to build it." >&2
    echo "       (siphon-bin --features smpp needs siphon-sip PR #20 merged.)" >&2
    exit 2
  fi
fi
[[ -x "$SIPHON_BIN" ]] || { echo "error: SIPHON_BIN not executable: $SIPHON_BIN" >&2; exit 2; }

# ── Build the load driver ────────────────────────────────────────────────────
echo "[*] building smpp-load (release)…"
cargo build --release --quiet --manifest-path harness/Cargo.toml
LOAD="$ROOT/harness/target/release/smpp-load"

# ── Start siphon ─────────────────────────────────────────────────────────────
# Redirect siphon's own logs to a file so they don't clutter the results table
# (also keeps the one harmless generic_nack from the TCP readiness probe below
# out of the output). Tail $SIPHON_LOG to watch it.
echo "[*] starting siphon: $SIPHON_BIN -c $SIPHON_CONFIG  (log: $SIPHON_LOG)"
"$SIPHON_BIN" -c "$SIPHON_CONFIG" > "$SIPHON_LOG" 2>&1 &
siphon_pid=$!
trap 'kill "$siphon_pid" 2>/dev/null || true' EXIT

# Wait for the SMPP listener to accept (or siphon to die trying). The bare
# connect-and-close makes smpp34 log a single ESME_RINVCMDLEN generic_nack —
# harmless, and it lands in $SIPHON_LOG, not here.
printf '[*] waiting for %s:%s ' "$HOST" "$PORT"
for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null; then
    exec 3>&- 3<&-
    echo "— up"
    break
  fi
  if ! kill -0 "$siphon_pid" 2>/dev/null; then
    echo
    echo "error: siphon exited during startup — last lines of $SIPHON_LOG:" >&2
    tail -20 "$SIPHON_LOG" >&2 || true
    exit 1
  fi
  printf '.'
  sleep 0.5
done

# ── Sweep ────────────────────────────────────────────────────────────────────
echo
echo "## siphon-smpp end-to-end (bench_echo.py, $COUNT submits/window)"
echo
echo "| window | throughput (submit_sm/s) | p50 ms | p90 ms | p99 ms | p999 ms |"
echo "|-------:|-------------------------:|-------:|-------:|-------:|--------:|"
for w in $WINDOWS; do
  out="$("$LOAD" drive --host "$HOST" --port "$PORT" --count "$COUNT" --window "$w" || true)"
  tput="$(printf '%s\n' "$out" | sed -n 's/.*throughput: *\([0-9]*\).*/\1/p')"
  lat="$(printf '%s\n' "$out" | sed -n 's/.*latency *: *//p')"
  p50="$(printf '%s\n' "$lat" | sed -n 's/.*p50 \([0-9.]*\)ms.*/\1/p')"
  p90="$(printf '%s\n' "$lat" | sed -n 's/.*p90 \([0-9.]*\)ms.*/\1/p')"
  p99="$(printf '%s\n' "$lat" | sed -n 's/.*p99 \([0-9.]*\)ms.*/\1/p')"
  p999="$(printf '%s\n' "$lat" | sed -n 's/.*p999 \([0-9.]*\)ms.*/\1/p')"
  printf '| %6s | %24s | %6s | %6s | %6s | %7s |\n' \
    "$w" "${tput:-FAIL}" "${p50:--}" "${p90:--}" "${p99:--}" "${p999:--}"
done
echo
echo "[*] done."
