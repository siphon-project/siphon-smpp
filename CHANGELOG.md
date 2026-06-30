# Changelog

All notable changes to `siphon-smpp` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] — 2026-06-30

First open-source release — an SMPP 3.4 addon for
[siphon](https://github.com/siphon-project/siphon-sip) with enough surface to build
a full store-and-forward SMSC in scripts. Built on
[`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34) 1.2.

### Composition

- `namespace(cfg)` + `task(cfg)` hooks that plug an `smpp` Python namespace and a
  tokio SMPP runtime into a composing siphon binary.
- YAML + `SMPP_BIND_<NAME>_*` env-var configuration (`SmppConfig`), with
  `${VAR}` / `${VAR:-default}` expansion and declarative routing rules.

### Binds & authentication

- SMPP **server** for inbound binds (transceiver only; TX/RX rejected), with
  script-driven `@smpp.on_bind` authorisation. `bind.reject(status, reason)`
  returns a `BindResult` mapped onto the wire status and logged; closed by
  default (no handler → reject).
- **Outbound binds** to remote SMSCs/aggregators, each supervised with
  reconnect + exponential backoff and an optional per-bind `max_msg_per_sec`
  token-bucket throttle.
- `@smpp.on_session("bound" | "unbound")` lifecycle for both inbound ESME and
  outbound bind; inbound `Session` carries `system_id`.

### Operation coverage (full unless noted)

- **Inbound dispatch** to `@smpp.on_pdu(...)`: `submit_sm`, `data_sm`,
  `cancel_sm`, `query_sm` (reply via `pdu.reply_query(...)`), `replace_sm`.
  `submit_sm_multi` is not yet exposed (stub PDU in `smpp34`).
- **Outbound dispatch**: `deliver_sm` (incl. **delivery receipts** — `Pdu.is_dlr`
  + parsed `Pdu.receipt`), `data_sm`, `alert_notification`.
- **Outbound send helpers** (target a bind): `submit_via`, `data_via`,
  `cancel_via`, `query_via` (→ `QueryResp`), `replace_via`.
- **Inbound send helpers** (target a bound ESME by `session_id`): `deliver_to`,
  `data_to`, `alert_to` — MT-deliver and route DLRs back to the originating ESME.
- Pyclasses: `Pdu`, `PduReply`, `Session`, `Bind`, `BindResult`,
  `AlertNotification`, `SmppResp`, `QueryResp`. `Pdu` + `Receipt` (and
  `Pdu::from_*` / `Receipt::parse`) are re-exported for codec-adjacent reuse.

### Quality & ops

- Criterion benches (`benches/codec.rs`) over the per-PDU hot paths; a
  counting-allocator leak check (`examples/leak_check.rs` +
  `scripts/mem_leak_test.sh`) asserting flat live bytes. Both gated in CI.
- Deployment templates (`deploy/`): Dockerfile, docker-compose, and Kubernetes
  HA/failover manifests with a documented failover model.
- Examples: `examples/gateway.py` (a commodity store-and-forward SMS gateway with
  DLR correlation) and `examples/echo.py`.
