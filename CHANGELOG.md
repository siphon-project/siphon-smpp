# Changelog

All notable changes to `siphon-smpp` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.2.1] — 2026-07-01

### Added

- **SDK testing support for SMPP scripts** — the `siphon-sip` SDK now mocks the
  `smpp` namespace, so scripts can be unit-tested with `SmppTestHarness` and
  authored with full type hints/docstrings via `pip install siphon-sip` (no
  running SMSC). Documented under **Testing your scripts** in the script API
  reference. A CI parity check (`scripts/check_sdk_parity.py`) fails the build if
  the mock drifts from the runtime `smpp` surface.

## [1.2.0] — 2026-07-01

### Added

- **Inbound throttling** — a per-ESME-session ingress rate cap, the mirror of a
  bind's outbound `max_msg_per_sec`. `server.max_msg_per_sec` (0 = unlimited)
  gives each bound ESME its own token bucket, so one busy ESME can't starve
  another; inbound `submit_sm` / `data_sm` / `submit_sm_multi` are gated before
  dispatch. `server.throttle_action` selects the over-rate behaviour: `pace`
  (default — delay the response, backpressuring through the ESME's window) or
  `reject` (answer immediately with `ESME_RTHROTTLED`). Both are overridable
  from the environment (`SMPP_SERVER_MAX_MPS`, `SMPP_SERVER_THROTTLE_ACTION`)
  and exposed to scripts via the `_config` server dict.

## [1.1.0] — 2026-06-30

### Added

- **`submit_sm_multi` support** — full operation coverage (no stubs). Inbound
  `submit_sm_multi` dispatches to `@smpp.on_pdu("submit_sm_multi")` with the
  destination list on `pdu.destinations` (SME addresses and/or distribution-list
  names). Outbound `submit_multi_via(bind=…, source_addr=…, destinations=[…],
  short_message=…)` sends one message to many destinations via
  `smpp34`'s `SMSC::send_submit_sm_multi`. `Pdu` gains a `destinations` list.

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
