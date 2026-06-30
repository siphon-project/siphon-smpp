"""
echo.py — the smallest possible SMSC on siphon-smpp.

Accepts any bind, and "accepts" every submit_sm with a freshly minted
message_id (it doesn't actually forward anything — it's the hello-world).
Point an SMPP client at the listener (default 0.0.0.0:2775), bind as a
transceiver, and submit_sm: you get an ESME_ROK + message_id back.

Run it by pointing your siphon build's script path at this file. Edit the
file and siphon hot-reloads it — the next PDU uses the new code.
"""

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
    log.info(
        f"submit_sm {session.system_id}: "
        f"{pdu.source_addr} -> {pdu.destination_addr} "
        f"({len(pdu.short_message)} bytes) => {message_id}"
    )
    return pdu.reply(message_id=message_id)
