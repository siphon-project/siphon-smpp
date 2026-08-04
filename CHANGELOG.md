# Changelog

All notable changes to `siphon-smpp` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.4.0] — 2026-08-04

### Added

- **Optional parameters (TLVs) can be read and written from a script.** They are
  a dict keyed by the SMPP 3.4 spec name or by a raw integer tag for
  vendor-specific ones —
  `tlvs={"MESSAGE_PAYLOAD": b"…", "SAR_MSG_REF_NUM": 42, 0x1400: b"\x01"}` — on
  `submit_via`, `submit_multi_via`, `data_via`, `deliver_to` and `data_to`.
  Values encode explicitly: `bytes` go on the wire verbatim, `str` becomes a
  NUL-terminated C-Octet-String (§3.2.1.1), and `int` is encoded at the
  parameter's *spec* width, so `MESSAGE_STATE` is one octet and
  `SAR_MSG_REF_NUM` two whatever number you pass. A tag that isn't
  integer-typed rejects an `int` rather than guessing a width and putting a
  malformed TLV on the wire.

  Inbound, `Pdu` gained `tlvs` (`{tag: bytes}`), `tlv(name_or_tag)`, and typed
  shortcuts: `message_payload`, `receipted_message_id`, `message_state`,
  `user_message_reference`, `sar_msg_ref_num`, `sar_total_segments`,
  `sar_segment_seqnum`, `more_messages_to_send`, `network_error_code`.

  This is what makes three things possible that weren't: messages past the
  254-byte `short_message` limit, concatenation via the `sar_*` parameters, and
  delivery receipts carrying `receipted_message_id` / `message_state` rather
  than only the de-facto text body.
- **`pdu.body`** — `message_payload` when the peer used it, `short_message`
  otherwise. The two are mutually exclusive (§5.3.2.32) and which one arrives is
  the sender's choice, so a handler reading `short_message` directly drops every
  long message and every `data_sm`. The wire fields are still exposed unchanged;
  nothing is synthesized into `short_message`.
- **`pdu.reply(tlvs={…})` on the `data_sm` path**, landing on `data_sm_resp` —
  the one SMPP 3.4 response PDU with optional parameters (§4.2.3:
  `delivery_failure_reason`, `network_error_code`,
  `additional_status_info_text`, `dpf_result`). So a rejection reason can travel
  with the rejection. Used elsewhere it raises instead of dropping them.
- `pdu.receipt` now falls back to `receipted_message_id` (0x001E) and
  `message_state` (0x0427), and reports the numeric code as `message_state`. A
  receipt sent only as TLVs, with no `id:`/`stat:` body at all, parses. Where
  both are present the text body wins and the TLVs fill the gaps: interop runs
  on the text form and SMSCs populate the TLVs inconsistently, so existing
  receipts keep parsing exactly as they did. Both stay readable.

### Fixed

- **Every `data_sm` was empty in both directions.** A `data_sm` has no
  `short_message` field — its message exists only as the `message_payload`
  optional parameter (§4.2.2) — and until `smpp34` 1.3.0 the PDU had no `tlvs`
  field at all. So `data_via` / `data_to` took no message argument and could not
  have carried one, and `Pdu::from_data` hardcoded an empty body. Both now take
  `short_message=`, which is folded into `message_payload` on the way out and
  read back out of it on the way in. Supplying both `short_message=` and an
  explicit `MESSAGE_PAYLOAD` raises rather than silently picking one.
- **Inbound `alert_notification` reached handlers as garbage**, via `smpp34`
  1.3.0. Its `decode` parsed from byte 0 while the read loop hands it a complete
  PDU, so every field was 16 bytes off — `source_addr_ton` came out of
  `command_length` and the addresses were shredded — and it did so without
  erroring, which is why it went unnoticed. `ms_availability_status` was also
  written as a bare octet rather than TLV 0x0422 (§4.12.1); the bare form is
  still accepted on decode for peers on smpp34 ≤ 1.2.1. Guarded here by
  hand-written wire vectors for both forms.
- **A long `short_message` from a script panicked the SMPP runtime.** smpp34's
  `submit_sm` / `deliver_sm` / `submit_sm_multi` / `replace_sm` constructors
  `assert!` on the 254-byte limit, and script input reached them unchecked, so
  a 255-byte body took down the tokio task instead of failing the call. The send
  helpers now raise a `ValueError` pointing at `MESSAGE_PAYLOAD` (and at nothing,
  for `replace_sm`, which has no optional parameters to fall back on). Same for
  `submit_sm_multi` past 254 destinations.

### Changed

- **`smpp34` to 1.3.0.**
- `examples/gateway.py` relays the body with `pdu.body`, carries the `sar_*`
  concatenation parameters across the hop (dropping them turns one message into
  fragments the far end cannot reassemble), attaches `RECEIPTED_MESSAGE_ID` +
  `MESSAGE_STATE` to the receipts it routes back — remapped to the gateway's own
  message id, not the upstream one — and gained a `data_sm` handler.

## [1.3.1] — 2026-07-27

### Fixed

- **Lost SMPP responses under pipelining, via `smpp34` 1.2.1.** Both of smpp34's
  writer tasks registered a request's pending-response entry only after the socket
  write returned, while the read loop drops any response it has no entry for. A
  response arriving in that gap was discarded and the caller blocked until its
  response timer expired (30s), so the PDU was lost rather than slow. This hit the
  SMSC to ESME direction too, which is `deliver_to`, the delivery-receipt path.
  Measured on the load harness pinned to 2 CPUs, 5000 submits per run: 10 of 30
  runs lost at least one response on 1.2.0, 0 of 52 on 1.2.1.

### Changed

- **The load harness now surfaces smpp34's diagnostics.** `smpp-load` installed no
  `log` subscriber, so every `error!` the codec emits (a dropped response, an
  undecodable PDU, a request that never got answered) went nowhere and a failed
  run reported a bare `errors 1` with nothing to explain it. It now defaults to
  `RUST_LOG=warn`, and the failure summary breaks the count down by SMPP error
  instead of only totalling it. This is diagnostics only, the pass/fail rule is
  unchanged: any error still fails the run.
- **Dependency bumps.** `siphon-sip` to 1.5.0 and `smpp34` to 1.2.1; the
  cargo-minor-patch group (async-trait, serde, thiserror, tokio); and the lockfile
  moves that cleared RUSTSEC-2026-0204 (`crossbeam-epoch` to 0.9.20, an invalid
  pointer dereference in the `fmt::Pointer` impl for `Atomic`/`Shared`) and the
  yanked `spin` 0.9.8.

## [1.3.0] — 2026-07-09

### Added

- **Prometheus metrics** — SMPP observability registered into siphon's shared
  metrics store (`custom_metrics()`), the same registry that serves `/metrics`;
  no `prometheus` dependency is added to this crate. `siphon_smpp_binds`
  (gauge, `direction`/`state`) reports bound sessions and is sampled every 10s;
  `siphon_smpp_pdus_total` (`direction`/`command`/`result`),
  `siphon_smpp_throttled_total` (`direction`),
  `siphon_smpp_bind_reconnects_total` (`bind`),
  `siphon_smpp_dispatch_errors_total` (`command`),
  `siphon_smpp_dispatch_duration_seconds` (histogram, `command`) and
  `siphon_smpp_bind_requests_total` (`result`) are recorded inline at the
  dispatch and bind sites. When the host metrics engine is not initialised
  (e.g. headless, no admin server) the series are skipped with one log line and
  every emit path is a no-op — the dispatch hot path then reads no clock and
  touches no metric, only a couple of `OnceLock` loads. Bench group `metrics`
  covers the enabled-path per-PDU cost.

### Changed

- Dependency bumps: `criterion` 0.5 → 0.8 (dev-only bench harness; switched to
  `std::hint::black_box`), the `siphon-sip` git pin to `7b0fab0`, and the
  GitHub Actions workflow dependencies.

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
