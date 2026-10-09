# Changelog

All notable changes to `siphon-smpp` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.6.0] — 2026-10-09

**Read this before upgrading.** A send that used to be reported as accepted can
now be reported as rejected, because it was rejected all along. Scripts that
check the result start seeing rejections; scripts that do not check it need to.

### Fixed

- **Every response that arrived was reported to the script as `ESME_ROK`.** The
  send helpers (`submit_via`, `submit_multi_via`, `data_via`, `cancel_via`,
  `query_via`, `replace_via`, `deliver_to`, `data_to`) built their result as a
  success for any response PDU that decoded, without looking at the
  `command_status` in its header. A peer answering `ESME_RTHROTTLED`,
  `ESME_RMSGQFUL`, `ESME_RINVDSTADR`, `ESME_RSUBMITFAIL`, or an ESME answering
  `ESME_RX_T_APPN` to a `deliver_sm`, was handed to the script as
  `command_status == "ESME_ROK"`, `ok == True`. A rejected message therefore
  looked delivered: not retried, not re-routed, not held. `resp.ok` has been
  on the result since 1.0.0, and the script API reference says to check it and
  `resp.command_status` for a peer's rejection; neither has ever been able to
  show one.

  The result now carries the status the peer sent, by name and as
  `command_status_code`. `ok` is true only for `ESME_ROK`, and the object is
  **falsy** otherwise, so `if resp:` means what it reads as. `message_id` is
  empty on a rejection (a `submit_sm_resp` with a non-zero status has no body,
  §4.4.2). A status outside SMPP 3.4 Table 5-2 — reserved, or in the vendor
  range — reads as its hex value (`"0x00000401"`) rather than borrowing a name.

  What an unmodified script sees change: `resp.ok`, `resp.command_status` and
  `bool(resp)` are no longer constant. Nothing was removed or retyped.

- **A handler's reject status could be answered `ESME_ROK`.** For an inbound
  `submit_sm` (and a `data_sm` from an ESME) the runtime chose between accept
  and reject by whether the reply had a `message_id`, not by its
  `command_status`. `pdu.reply(command_status="ESME_RTHROTTLED",
  message_id=our_id)` — natural for a handler that allocates its id first — went
  out as an acceptance carrying that id. The reply's status now decides, on
  every path.

- **The default `submit_sm` acknowledgement was a malformed PDU.** `pdu.reply()`
  with no `message_id`, a handler returning `None`, and the no-handler default
  all produced an `ESME_ROK` `submit_sm_resp` that was a bare 16-octet header.
  `message_id` is mandatory there (§4.4.2): an independent dissector flags the
  PDU as malformed, and this crate's own codec refuses to decode it
  (`ESME_RINVPARLEN`), so an ESME built on it never saw the acknowledgement. It
  now carries an empty `message_id`.

- **Seven statuses could not be named in a reply.** `pdu.reply(command_status=…)`
  and `bind.reject(…)` rejected `ESME_RINVOPTPARSTREAM`, `ESME_ROPTPARNOTALLWD`,
  `ESME_RINVPARLEN`, `ESME_RMISSINGOPTPARAM`, `ESME_RINVOPTPARAMVAL`,
  `ESME_RDELIVERYFAILURE` and `ESME_RUNKNOWNERR` as unknown. All of Table 5-2 is
  accepted now.

### Added

- **`smpp.SmppSendError`**, raised by a send helper that got no response PDU.
  It subclasses `RuntimeError`, which is what these cases raised before, so an
  existing `except RuntimeError` / `except Exception` still catches it. Its
  `reason` separates what used to be one message string:
  `"timeout"` (no response within `response_timer_ms`), `"closed"` (the session
  ended while the request was outstanding), `"nack"` (the peer answered
  `generic_nack`; `command_status` / `command_status_code` carry its status) and
  `"unanswered"` (the session reported a failure with no response while still
  up). After `"timeout"` or `"closed"` it is not known whether the peer took the
  message. A bind or session that is not bound still raises `KeyError` before
  anything is sent.
- **`resp.command_status_code`** (the status as an integer) and
  **`resp.throttled`** (true for `ESME_RTHROTTLED` and `ESME_RMSGQFUL`) on
  `SmppResp` and `QueryResp`. `QueryResp` gained the same truthiness.
- **`pdu.validity_period`, `pdu.schedule_delivery_time`,
  `pdu.replace_if_present_flag`, `pdu.sm_default_msg_id`** on the inbound `Pdu`.
  They are mandatory fields of `submit_sm`, `submit_sm_multi` and `replace_sm`
  that the codec decoded and the script could not read, so a submitted validity
  period was invisible. (`deliver_sm` has them in its layout but the
  specification requires them NULL there.)

### Changed

- **`examples/gateway.py` and the cookbook check what they send.** Both treated
  any returned response as success — the same mistake the runtime made on their
  behalf.
- The egress `max_msg_per_sec` limiter is unchanged: it paces what is sent and
  takes no account of what the peer answers. A throttled response is surfaced to
  the script, which decides whether to back off further.

### Known limits

- `"timeout"`, `"closed"` and `"unanswered"` are told apart from the elapsed
  time and the session registry, because the codec reports all three with the
  same error. A `generic_nack` carrying `ESME_RSYSERR` is reported as
  `"unanswered"`, and one carrying a status outside Table 5-2 as
  `ESME_RUNKNOWNERR`, for the same reason.
- `data_sm_resp` carries an SMSC `message_id` (§4.7.2) that the codec does not
  expose, so `data_via` still returns an empty `message_id` on success.

## [1.5.1] — 2026-08-18

### Fixed

- **A bind could be torn down the instant it succeeded, via `smpp34` 1.4.1.**
  Both sides read the bind handshake with a single `read()` and handed the whole
  buffer to `CommandHeader::decode`, which rejects it when the buffer length and
  `command_length` disagree. TCP has no message boundaries, so anything a peer
  sends immediately behind its bind PDU can coalesce into the same segment and
  take the session down before it carried a thing — every request on it then
  failing at once rather than the odd one a correlation race would lose.

  This is squarely our shape of traffic in both directions. An upstream SMSC with
  queued MT sends its first `deliver_sm` the moment it accepts our bind, so it
  catches up with its own `bind_transceiver_resp` and the outbound bind dies on
  arrival (`PDU length 451 does not match command_length 31`, then `Unable to
  decode bind response`) — the supervisor then reconnects into the same failure
  for as long as the peer has traffic waiting. Symmetrically, an inbound ESME
  that pipelines its first `submit_sm` without waiting for the bind response is
  doing something legal, and we rejected the whole read. The handshake is now
  framed like any other read, and bytes that arrived behind the bind PDU are
  replayed into the session loop instead of being discarded. The bug predates
  1.4.0 and every release of the crate before it.

### Changed

- **`smpp34` to 1.4.1.** No API change — the fix is entirely internal to the
  handshake read path, so nothing on our side moved.

## [1.5.0] — 2026-08-18

### Fixed

- **A listening socket we never got was reported as "SMPP server listening".**
  The server task logged that line *before* calling `SmppServer::start`, then
  parked on `pending::<()>()` for the process lifetime. Under `smpp34` ≤ 1.3.0 a
  failed bind panicked a tokio worker; 1.4.0 turns it into the defaulted
  `SmppServerListener::on_listen_failed`, which we did not implement, so a port
  already in use, a privileged port, or an address the host does not own would
  have left us parked forever claiming to serve while accepting nothing. We now
  implement the hook, log the cause at `error!`, and the task returns instead of
  parking. The "listening" line moved after `start()`, which as of 1.4.0 means
  the socket is actually accepting.
- **A refused outbound connect cost the full 15-second bind deadline, every
  retry, and was reported as a timeout it never was.** A connect that fails
  starts no session, so `is_alive()` could never flip and the supervisor polled
  it out to `bind_deadline` before backing off — then logged "bind did not
  complete within 15s" with no cause. `SmppClientListener::on_connection_failed`
  (new in 1.4.0) now logs the real reason (`TCP connect to … failed: Connection
  refused`), counts it, and ends the wait at once, so backoff starts on the
  failure rather than 15 seconds after it.

### Added

- `siphon_smpp_bind_connect_failures_total{bind}` — outbound bind attempts that
  never reached a session. Deliberately separate from
  `siphon_smpp_bind_reconnects_total`, which counts an *established* session
  dropping: a peer refusing connections outright and one that keeps dropping
  healthy sessions are different faults and shouldn't share a series.

### Changed

- **`smpp34` to 1.4.0** — a robustness release that takes `src/` from 50
  production `.unwrap()`/`.expect()` calls to zero, several reachable straight
  from the wire. The ones that could have reached us: `get_error()` panicked on
  any `command_status` outside the enum, though §5.1.3 leaves 0x400–0x4FF
  vendor-specific and real SMSCs use it; the response timers keyed on
  `SystemTime`, so an NTP step backwards skewed or panicked them; and writing a
  rejection to a peer that had already gone panicked the session. No signature
  or documented guarantee changed.
- Dependency bumps merged ahead of this: `siphon-sip` to 1.5.1, and the
  cargo-minor-patch group (pyo3 0.29.0 → 0.29.2, thiserror 2.0.19 → 2.0.20,
  proc-macro2 0.1.91 → 0.1.92).

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
