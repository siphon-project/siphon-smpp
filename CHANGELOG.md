# Changelog

All notable changes to `siphon-smpp` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added — "build a full SMSC" surface

- **Bind reject reasons**: `bind.reject(status, reason)` returns a `BindResult`
  the runtime maps onto the wire `bind_*_resp` status and logs the reason;
  `bind.accept()` unchanged. Bare truthy/falsy returns still work.
- **Delivery receipts reach scripts**: `deliver_sm` is no longer swallowed when
  it's a DLR — every `deliver_sm` dispatches to `@smpp.on_pdu("deliver_sm")`.
  New `Pdu.is_dlr` and `Pdu.receipt` (best-effort parse of the receipt body to
  `{id, sub, dlvrd, submit_date, done_date, stat, err, text, raw}`).
- **SMSC→ESME sends**, targeting a bound ESME by `session_id`: `deliver_to`,
  `data_to`, `alert_to`. Lets a script MT-deliver and route DLRs back to the
  originating ESME.
- **More outbound sends**: `data_via`, `cancel_via` (+ `query_via` /
  `replace_via` forward-compat stubs that raise `NotImplementedError` until
  `smpp34` exposes the send).
- **More inbound dispatch**: `data_sm` (both directions), `cancel_sm`, and
  `alert_notification` (outbound) now dispatch to `@smpp.on_pdu(...)`.
- **Session lifecycle**: `@smpp.on_session("bound" | "unbound")` fires for both
  inbound ESME and outbound bind lifecycle. Inbound `Session.system_id` is now
  populated.
- **Outbound throttling**: per-bind `max_msg_per_sec` is enforced with a token
  bucket that paces (not rejects) `submit_via` / `data_via`.
- **Pyclasses**: `BindResult`, `AlertNotification`; `SubmitResp` → `SmppResp`
  (now the response type for all send helpers, with `.ok`).
- **Benches + leak check**: `benches/codec.rs` (criterion) over the per-PDU hot
  paths; `examples/leak_check.rs` + `scripts/mem_leak_test.sh` assert flat live
  bytes. Both gated in CI.
- **Deployment templates**: `deploy/` — Dockerfile, docker-compose, and
  Kubernetes HA/failover manifests with a documented failover model.
- **Examples**: `examples/gateway.py` (a commodity store-and-forward SMS gateway
  with DLR correlation) and `examples/echo.py`.
- **Public API**: `Pdu` and `Receipt` (+ `Pdu::from_submit/from_deliver/
  from_data`, `Receipt::parse`) re-exported for codec-adjacent reuse.

### Changed

- `smpp34` dependency floor bumped to `1.1` (alert/data/cancel send helpers).

## [0.1.0]

Initial open-source release — an SMPP 3.4 addon for the
[siphon](https://github.com/siphon-project/siphon) scripting platform.

### Added

- `namespace(cfg)` + `task(cfg)` builder hooks that plug an `smpp` Python
  namespace and a tokio SMPP runtime into a siphon binary.
- SMPP server for **inbound binds** (transceiver only; TX/RX rejected) with
  script-driven `@smpp.on_bind` authentication and `@smpp.on_pdu("submit_sm")`
  dispatch.
- **Outbound binds** to remote SMSCs/aggregators with reconnect + exponential
  backoff; `submit_via(bind="…")` from scripts; `@smpp.on_pdu("deliver_sm")`.
- YAML + `SMPP_BIND_<NAME>_*` env-var configuration ([`SmppConfig`]).
- Script-facing pyclasses: `Pdu`, `PduReply`, `Session`, `Bind`.
