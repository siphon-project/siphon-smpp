#!/usr/bin/env bash
# bench_multi.sh — aggregate throughput across N parallel SMPP binds.
#
# A single ESME bind is one TCP connection, and siphon processes that
# connection's PDUs on one reader — so bench.sh (one bind) measures the per-bind
# ceiling and the latency curve. A real SMSC serves many ESMEs at once; this
# launches N parallel binds against the same siphon and reports the aggregate
# submit_sm/s (total delivered ÷ wall-clock), the capacity number that scales
# with cores.
#
# Usage (knobs mirror bench.sh):
#   SIPHON_BIN=/path/to/siphon ./bench_multi.sh
#   SIPHON_SIP_DIR=~/workspace/siphon-sip ./bench_multi.sh
#   BINDLIST="1 4 8 16 24"  COUNT=100000  WINDOW=64  ./bench_multi.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SIPHON_CONFIG="${SIPHON_CONFIG:-harness/siphon.bench.yaml}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-2775}"
COUNT="${COUNT:-100000}"          # submit_sm PER bind
WINDOW="${WINDOW:-64}"
BINDLIST="${BINDLIST:-1 4 8 16 24}"
SIPHON_LOG="${SIPHON_LOG:-${TMPDIR:-/tmp}/siphon-smpp-bench.log}"
WORK="$(mktemp -d)"

# ── Resolve the siphon binary ────────────────────────────────────────────────
if [[ -z "${SIPHON_BIN:-}" ]]; then
  if [[ -n "${SIPHON_SIP_DIR:-}" ]]; then
    echo "[*] building siphon (siphon-bin --features smpp) in $SIPHON_SIP_DIR/siphon-bin …"
    ( cd "$SIPHON_SIP_DIR/siphon-bin" && cargo build --release --features smpp )
    SIPHON_BIN="$SIPHON_SIP_DIR/siphon-bin/target/release/siphon"
  else
    echo "error: set SIPHON_BIN=/path/to/siphon, or SIPHON_SIP_DIR=/path/to/siphon-sip to build it." >&2
    exit 2
  fi
fi
[[ -x "$SIPHON_BIN" ]] || { echo "error: SIPHON_BIN not executable: $SIPHON_BIN" >&2; exit 2; }

# ── Build the load driver ────────────────────────────────────────────────────
echo "[*] building smpp-load (release)…"
cargo build --release --quiet --manifest-path harness/Cargo.toml
LOAD="$ROOT/harness/target/release/smpp-load"

# ── Start siphon ─────────────────────────────────────────────────────────────
echo "[*] starting siphon: $SIPHON_BIN -c $SIPHON_CONFIG  (log: $SIPHON_LOG)"
"$SIPHON_BIN" -c "$SIPHON_CONFIG" > "$SIPHON_LOG" 2>&1 &
siphon_pid=$!
trap 'kill "$siphon_pid" 2>/dev/null || true; rm -rf "$WORK"' EXIT

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

# ── Sweep the number of concurrent binds ─────────────────────────────────────
echo
echo "## siphon-smpp aggregate throughput (bench_echo.py, $COUNT submits/bind, window $WINDOW)"
echo
echo "| binds | aggregate submit_sm/s | per-bind submit_sm/s | total ok |"
echo "|------:|----------------------:|---------------------:|---------:|"
for n in $BINDLIST; do
  pids=()
  for i in $(seq 1 "$n"); do
    "$LOAD" drive --host "$HOST" --port "$PORT" --count "$COUNT" --window "$WINDOW" \
      --system-id "load$i" --source-addr "1555010$i" > "$WORK/d.$n.$i" 2>&1 &
    pids+=("$!")
  done
  wait "${pids[@]}" || true

  # Aggregate = sum of each bind's own throughput (its ok ÷ its own elapsed, as
  # reported by that driver). Robust to skew across the parallel drivers — each
  # contributes the rate it actually sustained, which is the multi-ESME capacity
  # number an SMSC cares about (a single straggler can't tank the whole figure,
  # as it would with total ÷ slowest-wall-clock).
  okt=0 ; agg=0
  for i in $(seq 1 "$n"); do
    o=$(sed -n 's/.*submitted : [0-9]* *ok \([0-9]*\).*/\1/p' "$WORK/d.$n.$i"); o=${o:-0}
    t=$(sed -n 's/.*throughput: *\([0-9]*\).*/\1/p' "$WORK/d.$n.$i"); t=${t:-0}
    okt=$((okt + o))
    agg=$((agg + t))
  done
  perbind=$(awk -v a="$agg" -v n="$n" 'BEGIN{printf "%.0f", a/n}')
  printf '| %5s | %21s | %20s | %8s |\n' "$n" "$agg" "$perbind" "$okt"
done
echo
echo "[*] done."
