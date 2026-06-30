# Changelog

All notable changes to `siphon-smpp` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
