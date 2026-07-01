# Cookbook

Worked, copy-pasteable recipes for building real things on siphon-smpp. Each
recipe is pure SMPP (ESME ↔ SMSC) and uses only the [Script API](../script-api.md).

<div class="grid cards" markdown>

- **[Building an SMSC gateway](smsc-gateway.md)** — the headline recipe. A
  commodity store-and-forward SMS gateway: credential-checked binds, prefix
  routing to upstream binds, delivery-receipt correlation routed back to the
  originating ESME, mobile-originated replies, and `alert_notification`
  handling — all in ~200 lines of Python.

</div>

## The two starting points in the repo

| Example | What it is |
|---|---|
| [`examples/echo.py`](https://github.com/siphon-project/siphon-smpp/blob/main/examples/echo.py) | The hello-world: accept any bind, ack every `submit_sm`. Covered in the [Quickstart](../quickstart.md). |
| [`examples/gateway.py`](https://github.com/siphon-project/siphon-smpp/blob/main/examples/gateway.py) | The full store-and-forward gateway. Walked through in [Building an SMSC gateway](smsc-gateway.md). |

## Patterns you'll reuse

These show up in every non-trivial script; the gateway recipe uses all of them.

- **Closed-by-default auth** — no `@smpp.on_bind` means every bind is rejected;
  opt ESMEs in explicitly, and return a *reason* on reject so it's logged. See
  [`@smpp.on_bind`](../script-api.md#on_bind).
- **Session maps in shared state** — track `system_id → session_id` in
  [`siphon.cache`](../concepts.md#where-state-lives), not a module dict, so it
  survives hot-reload and is shared across workers/replicas.
- **Your own id space** — hand the ESME *your* `message_id`, keep a
  correlation from the upstream id to it, and rewrite receipts on the way back.
  This is what makes DLRs routable regardless of what upstream assigns.
- **Translate outbound failures into a reply status** — wrap `*_via` sends in
  `try/except` and map failures to `ESME_RSUBMITFAIL` (etc.) for the originating
  ESME, rather than letting the handler throw.

Start with [Building an SMSC gateway](smsc-gateway.md).
