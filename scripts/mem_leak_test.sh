#!/usr/bin/env bash
# Build + run the live-bytes leak check (examples/leak_check.rs).
# Exits non-zero if any phase's live bytes grow past its budget.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[*] building leak_check (release)..."
cargo build --release --example leak_check --quiet

echo "[*] running..."
./target/release/examples/leak_check
