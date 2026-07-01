"""
bench_echo_io.py — submit_sm echo that simulates a downstream I/O roundtrip.

Same as bench_echo.py but the handler `await`s for BENCH_IO_MS (default 2 ms)
before replying, standing in for the DB write / upstream forward a real
store-and-forward SMSC does per message. The point is diagnostic: an `await`
yields the event loop, so if siphon's aggregate throughput is bounded by event-
loop *concurrency* it will rise here (many awaits overlap); if it's bounded by
the per-message *GIL-held Python CPU*, it stays put — because every message
still has to run its Python body under the GIL regardless of the await.

Driven by harness/bench_multi.sh with SIPHON_CONFIG=harness/siphon.bench.io.yaml.
"""

import asyncio
import os

from siphon import smpp

_DELAY = float(os.environ.get("BENCH_IO_MS", "2")) / 1000.0


@smpp.on_bind
async def authorise(bind):
    return bind.accept()


@smpp.on_pdu("submit_sm")
async def echo(pdu, session):
    if _DELAY:
        await asyncio.sleep(_DELAY)  # stand-in for a downstream I/O roundtrip
    return pdu.reply(message_id="0")
