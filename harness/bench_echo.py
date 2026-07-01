"""
bench_echo.py — zero-overhead submit_sm echo, for measurement only.

Same dispatch path as examples/echo.py, but with NO per-PDU logging and a
constant message_id: at high submit rates a log line (and a uuid) per PDU is
the bottleneck, not siphon-smpp's dispatch, so this strips both to expose the
real ceiling.

Use examples/echo.py when you want to *see* traffic; use this when you want to
*measure* it. Driven by harness/bench.sh via harness/siphon.bench.yaml.
"""

from siphon import smpp


@smpp.on_bind
async def authorise(bind):
    # Wide open — this is a localhost load rig, never anything real.
    return bind.accept()


@smpp.on_pdu("submit_sm")
async def echo(pdu, session):
    # Constant id: the load driver doesn't read it, and minting a unique one
    # per PDU would measure uuid throughput rather than the SMPP path.
    return pdu.reply(message_id="0")
