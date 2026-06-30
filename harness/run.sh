#!/usr/bin/env bash
# Load-test harness orchestration.
#
#   ./run.sh self-test [extra drive args…]   # build, run mock SMSC, drive it
#   ./run.sh drive --host H --port P …        # drive an already-running SMSC
#   ./run.sh serve --port P                   # just the mock SMSC
#
# self-test needs no siphon — it drives the built-in mock. For a REAL test,
# run your `siphon-bin --features smpp` (with examples/echo.py + smpp.yaml),
# then: ./run.sh drive --host <siphon-host> --port 2775 --count 1000000 --window 128
set -euo pipefail
cd "$(dirname "$0")"

cmd="${1:-self-test}"; shift || true

echo "[*] building smpp-load (release)…"
cargo build --release --quiet
BIN=./target/release/smpp-load

case "$cmd" in
  self-test)
    port=12775
    echo "[*] starting mock SMSC on :$port"
    "$BIN" serve --port "$port" &
    serve_pid=$!
    trap 'kill $serve_pid 2>/dev/null || true' EXIT
    sleep 1
    echo "[*] driving load…"
    "$BIN" drive --port "$port" --count "${COUNT:-50000}" --window "${WINDOW:-64}" "$@"
    ;;
  drive|serve)
    exec "$BIN" "$cmd" "$@"
    ;;
  *)
    echo "usage: ./run.sh [self-test|drive|serve] [args…]" >&2
    exit 2
    ;;
esac
