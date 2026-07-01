//! SMPP addon configuration.
//!
//! Loaded from a separate file referenced by siphon's `extensions:`
//! map (siphon ≥ a290cc4). Three top-level concerns:
//!
//!   * [`server`]  — the inbound SMPP listener (an ESME binds to *us*)
//!   * [`binds`]  — outbound SMPP binds to other ESMEs / aggregators
//!     (we bind to *them* to forward MT traffic)
//!   * [`routing`] — declarative SMS routing rules + default chain;
//!     consumed by the PoC's `routing.py` Python module
//!
//! The config dict is exposed to the script as `siphon.smpp.config()`.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct SmppConfig {
    /// Inbound SMPP listener (ESMEs binding to us).
    #[serde(default)]
    pub server: ServerConfig,

    /// Outbound SMPP binds (we bind to other ESMEs / aggregators).
    /// Order is preserved; first-bind-success ordering is up to the
    /// runtime task.
    #[serde(default)]
    pub binds: Vec<BindConfig>,

    /// Declarative SMS routing rules. Read by the script via
    /// `siphon.smpp.config()["routing"]`.
    #[serde(default)]
    pub routing: RoutingConfig,

    // ── Back-compat: the original flat-shape config still works ────
    // (kept until all existing smpp.yaml files are migrated to the
    // nested `server:` form).
    #[serde(default)]
    pub bind_address: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

impl SmppConfig {
    /// Resolved listen `host:port` — prefers nested `server:`, falls
    /// back to flat `bind_address` + `port`, then to defaults.
    pub fn listen(&self) -> (String, u16) {
        let host = self
            .server
            .bind_address
            .clone()
            .or_else(|| self.bind_address.clone())
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let port = self.server.port.or(self.port).unwrap_or(2775);
        (host, port)
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ServerConfig {
    pub bind_address: Option<String>,
    pub port: Option<u16>,

    #[serde(default = "default_session_init")]
    pub session_init_timer_ms: u64,
    #[serde(default = "default_enquire_link")]
    pub enquire_link_timer_ms: u64,
    #[serde(default = "default_inactivity")]
    pub inactivity_timer_ms: u64,
    #[serde(default = "default_response")]
    pub response_timer_ms: u64,

    /// Optional inbound throughput cap (msg/s) applied **per bound ESME
    /// session** — the ingress mirror of a bind's `max_msg_per_sec`. Each
    /// session gets its own token bucket, so one busy ESME can't starve
    /// another. 0 = unlimited. See [`throttle_action`](Self::throttle_action)
    /// for what happens when the cap is hit.
    #[serde(default)]
    pub max_msg_per_sec: u32,

    /// What to do with an inbound submit that exceeds
    /// [`max_msg_per_sec`](Self::max_msg_per_sec): `pace` (delay the
    /// `*_resp`, the default) or `reject` (answer immediately with
    /// `ESME_RTHROTTLED`). No effect when `max_msg_per_sec` is 0.
    #[serde(default)]
    pub throttle_action: ThrottleAction,

    pub tls: Option<TlsConfig>,
}

/// Behaviour when a bound ESME exceeds `server.max_msg_per_sec`.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ThrottleAction {
    /// Delay the response until a token frees, backpressuring the ESME
    /// through its outstanding-PDU window. A speed limit, not an error.
    #[default]
    Pace,
    /// Answer the over-rate submit immediately with `ESME_RTHROTTLED`
    /// (0x58); the ESME is expected to back off and retry.
    Reject,
}

impl ThrottleAction {
    /// Lowercase wire/label form (`"pace"` / `"reject"`), used for the
    /// script-visible `_config` dict and log lines.
    pub fn as_str(self) -> &'static str {
        match self {
            ThrottleAction::Pace => "pace",
            ThrottleAction::Reject => "reject",
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct BindConfig {
    /// Bind identifier. Referenced from routing chains as
    /// `bind:<name>` and from the script as `await
    /// smpp.submit_via(bind="<name>", …)`.
    pub name: String,

    pub host: String,
    pub port: u16,

    pub system_id: String,
    pub password: String,

    /// Optional SMPP system_type. Many aggregators ignore it.
    #[serde(default)]
    pub system_type: String,

    /// "transmitter" | "receiver" | "transceiver". Default: transceiver.
    #[serde(default = "default_bind_type")]
    pub bind_type: String,

    /// Optional throughput cap (msg/s). 0 = unlimited.
    #[serde(default)]
    pub max_msg_per_sec: u32,

    #[serde(default = "default_enquire_link")]
    pub enquire_link_timer_ms: u64,
    #[serde(default = "default_response")]
    pub response_timer_ms: u64,

    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RoutingConfig {
    /// Chain tried for any destination not matching a specific rule.
    /// Step values: "ims" | "ss7" | "sgd" | "queue" | "bind:<name>".
    #[serde(default)]
    pub default_chain: Vec<String>,

    /// Prefix-matched rules. Longest-prefix-wins; ties: first wins.
    #[serde(default)]
    pub rules: Vec<RoutingRule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RoutingRule {
    /// E.164 prefix without leading `+`. Empty string = catch-all.
    pub prefix: String,
    /// Same step grammar as `default_chain`.
    pub chain: Vec<String>,
    /// Optional descriptive name for logs/metrics.
    #[serde(default)]
    pub name: String,
    /// Free-form metadata the script may use (e.g. rate caps,
    /// preferred bind on retry).
    #[serde(default)]
    pub options: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_path: Option<String>,
}

fn default_session_init() -> u64 {
    5000
}
fn default_enquire_link() -> u64 {
    30000
}
// 5 min — 10× enquire_link. With the old 60 s a single missed
// keep-alive tore the session down; in live tests against didww
// that cascaded into a DynamoDB-row delete and `ESME_RX_T_APPN`
// rejection for any deliver_sm landing in the rebind window. See
// dev/siphon-smpp-spec-timers-and-late-response.md for the chain.
fn default_inactivity() -> u64 {
    300_000
}
// 30 s — matches Kannel; SMPP 5.0 §4.7 suggests 60 s. The previous
// 2 s default sat inside the typical long-tail latency of public
// SMS aggregators (didww, Twilio, …) and produced spurious
// session teardowns on healthy submissions.
fn default_response() -> u64 {
    30_000
}
fn default_bind_type() -> String {
    "transceiver".into()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Parse {
        path: String,
        source: serde_yaml::Error,
    },
    #[error("bind {bind:?} from env: missing SMPP_BIND_{}_{field}", bind.to_uppercase())]
    EnvBindMissing { bind: String, field: &'static str },
    #[error("bind {bind:?} from env: SMPP_BIND_{}_{field} = {value:?} ({source})",
            bind.to_uppercase())]
    EnvBindInvalid {
        bind: String,
        field: &'static str,
        value: String,
        source: std::num::ParseIntError,
    },
}

impl SmppConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let expanded = expand_env_vars(&raw);
        let mut cfg: Self =
            serde_yaml::from_str(&expanded).map_err(|source| ConfigError::Parse {
                path: path.display().to_string(),
                source,
            })?;
        cfg.binds.extend(binds_from_env()?);
        if !cfg.binds.is_empty() {
            let names: Vec<&str> = cfg.binds.iter().map(|t| t.name.as_str()).collect();
            tracing::info!(binds = ?names, "smpp: binds loaded");
        }
        if let Ok(raw) = std::env::var("SMPP_DEFAULT_CHAIN") {
            let chain: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            tracing::info!(chain = ?chain, "smpp: default_chain overridden by SMPP_DEFAULT_CHAIN");
            cfg.routing.default_chain = chain;
        }
        // Inbound per-session throughput cap; mirrors the per-bind
        // SMPP_BIND_<NAME>_MAX_MPS override so the ingress rate can be
        // tuned from the environment without editing the YAML.
        if let Ok(raw) = std::env::var("SMPP_SERVER_MAX_MPS") {
            match raw.trim().parse::<u32>() {
                Ok(mps) => {
                    tracing::info!(
                        max_msg_per_sec = mps,
                        "smpp: server.max_msg_per_sec overridden by SMPP_SERVER_MAX_MPS"
                    );
                    cfg.server.max_msg_per_sec = mps;
                }
                Err(e) => tracing::warn!(value = %raw, error = %e,
                    "smpp: ignoring invalid SMPP_SERVER_MAX_MPS"),
            }
        }
        if let Ok(raw) = std::env::var("SMPP_SERVER_THROTTLE_ACTION") {
            match raw.trim().to_lowercase().as_str() {
                "pace" => cfg.server.throttle_action = ThrottleAction::Pace,
                "reject" => cfg.server.throttle_action = ThrottleAction::Reject,
                other => tracing::warn!(value = %other,
                    "smpp: ignoring invalid SMPP_SERVER_THROTTLE_ACTION (want pace|reject)"),
            }
        }
        Ok(cfg)
    }
}

/// Discover binds declared via `SMPP_BIND_<NAME>_*` env vars.
///
/// `<NAME>` is uppercased in the env var; the bind identity (matched
/// by routing chains as `bind:<name>`) is the lowercase form. The
/// presence of `SMPP_BIND_<NAME>_HOST` is the discovery signal —
/// binds without it are ignored. Names must not contain underscores
/// (the first `_` after `SMPP_BIND_` separates name from field).
fn binds_from_env() -> Result<Vec<BindConfig>, ConfigError> {
    use std::collections::BTreeMap;

    let mut by_name: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (key, value) in std::env::vars() {
        let Some(rest) = key.strip_prefix("SMPP_BIND_") else {
            continue;
        };
        let Some(idx) = rest.find('_') else { continue };
        let name = rest[..idx].to_lowercase();
        let field = rest[idx + 1..].to_string();
        by_name.entry(name).or_default().insert(field, value);
    }

    let mut out = Vec::new();
    for (name, fields) in by_name {
        let Some(host) = fields.get("HOST").filter(|s| !s.is_empty()) else {
            continue;
        };

        let require =
            |f: &'static str| -> Result<String, ConfigError> {
                fields.get(f).filter(|s| !s.is_empty()).cloned().ok_or(
                    ConfigError::EnvBindMissing {
                        bind: name.clone(),
                        field: f,
                    },
                )
            };
        let parse_u = |f: &'static str, default: u64| -> Result<u64, ConfigError> {
            match fields.get(f) {
                Some(v) if !v.is_empty() => {
                    v.parse().map_err(|source| ConfigError::EnvBindInvalid {
                        bind: name.clone(),
                        field: f,
                        value: v.clone(),
                        source,
                    })
                }
                _ => Ok(default),
            }
        };

        let port = parse_u("PORT", 2775)? as u16;
        let max_msg_per_sec = parse_u("MAX_MPS", 0)? as u32;

        let tls = fields.get("TLS").map(|v| v.trim().to_lowercase());
        let tls_on = matches!(tls.as_deref(), Some("true" | "1" | "yes" | "on"));

        out.push(BindConfig {
            name: name.clone(),
            host: host.clone(),
            port,
            system_id: require("SYSTEM_ID")?,
            password: require("PASSWORD")?,
            system_type: fields.get("SYSTEM_TYPE").cloned().unwrap_or_default(),
            bind_type: fields
                .get("BIND_TYPE")
                .cloned()
                .unwrap_or_else(default_bind_type),
            max_msg_per_sec,
            enquire_link_timer_ms: parse_u("ENQUIRE_LINK_MS", default_enquire_link())?,
            response_timer_ms: parse_u("RESPONSE_MS", default_response())?,
            // smpp34's TlsConnector builder uses the system trust
            // store with default server-cert validation; cert_path /
            // key_path / ca_path on TlsConfig are not consumed yet.
            // Extend smpp34::client when private-CA / mTLS is needed.
            tls: if tls_on {
                Some(TlsConfig {
                    cert_path: String::new(),
                    key_path: String::new(),
                    ca_path: None,
                })
            } else {
                None
            },
        });
    }
    Ok(out)
}

/// Expand `${VAR}` and `${VAR:-default}` references against the
/// process environment. Mirrors siphon-core's main-config loader so
/// extension YAML can use the same syntax operators are used to.
fn expand_env_vars(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find("${") {
        // Emit everything before the `${` as a UTF-8-safe slice.
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 2..];
        let end = match after.find('}') {
            Some(off) => off,
            None => {
                // Unclosed — leave the literal `${` and bail.
                out.push_str("${");
                rest = after;
                continue;
            }
        };
        let spec = &after[..end];
        let (name, default) = match spec.split_once(":-") {
            Some((n, d)) => (n, Some(d)),
            None => (spec, None),
        };
        let value = std::env::var(name)
            .ok()
            .or_else(|| default.map(str::to_string))
            .unwrap_or_default();
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialises every test that mutates or reads the global process
    /// environment. `binds_from_env()` scans *all* env vars and
    /// `expand_env_vars` reads named ones, so concurrent env-mutating
    /// tests (cargo runs them in parallel) would otherwise observe each
    /// other's `SMPP_*` vars mid-flight. Each such test takes this lock
    /// for its whole body.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // ── YAML parsing ────────────────────────────────────────────────────

    const SAMPLE_YAML: &str = r#"
server:
  bind_address: "127.0.0.1"
  port: 9999
  session_init_timer_ms: 1000
  enquire_link_timer_ms: 15000
  max_msg_per_sec: 200
  throttle_action: reject

binds:
  - name: alpha
    host: smsc-a.example.net
    port: 2776
    system_id: alpha_esme
    password: secret-a
    bind_type: transmitter
    max_msg_per_sec: 50
  - name: beta
    host: smsc-b.example.org
    port: 2775
    system_id: beta_esme
    password: secret-b

routing:
  default_chain: ["ims", "ss7", "queue"]
  rules:
    - prefix: "3120"
      name: amsterdam
      chain: ["bind:alpha", "queue"]
      options:
        rate_cap: 10
    - prefix: ""
      chain: ["bind:beta"]
"#;

    #[test]
    fn parses_full_config() {
        let cfg: SmppConfig = serde_yaml::from_str(SAMPLE_YAML).expect("valid YAML");

        // server
        assert_eq!(cfg.server.bind_address.as_deref(), Some("127.0.0.1"));
        assert_eq!(cfg.server.port, Some(9999));
        assert_eq!(cfg.server.session_init_timer_ms, 1000);
        assert_eq!(cfg.server.enquire_link_timer_ms, 15000);
        assert_eq!(cfg.server.max_msg_per_sec, 200);
        assert_eq!(cfg.server.throttle_action, ThrottleAction::Reject);
        // server timers not set in YAML fall back to defaults
        assert_eq!(cfg.server.inactivity_timer_ms, default_inactivity());
        assert_eq!(cfg.server.response_timer_ms, default_response());
        assert!(cfg.server.tls.is_none());

        // binds
        assert_eq!(cfg.binds.len(), 2);

        let alpha = &cfg.binds[0];
        assert_eq!(alpha.name, "alpha");
        assert_eq!(alpha.host, "smsc-a.example.net");
        assert_eq!(alpha.port, 2776);
        assert_eq!(alpha.system_id, "alpha_esme");
        assert_eq!(alpha.password, "secret-a");
        assert_eq!(alpha.bind_type, "transmitter");
        assert_eq!(alpha.max_msg_per_sec, 50);
        // unset per-bind fields fall back to defaults
        assert_eq!(alpha.system_type, "");
        assert_eq!(alpha.enquire_link_timer_ms, default_enquire_link());
        assert_eq!(alpha.response_timer_ms, default_response());
        assert!(alpha.tls.is_none());

        let beta = &cfg.binds[1];
        assert_eq!(beta.name, "beta");
        assert_eq!(beta.port, 2775);
        // bind_type defaults to transceiver when omitted
        assert_eq!(beta.bind_type, default_bind_type());
        assert_eq!(beta.max_msg_per_sec, 0);

        // routing
        assert_eq!(cfg.routing.default_chain, vec!["ims", "ss7", "queue"]);
        assert_eq!(cfg.routing.rules.len(), 2);

        let rule0 = &cfg.routing.rules[0];
        assert_eq!(rule0.prefix, "3120");
        assert_eq!(rule0.name, "amsterdam");
        assert_eq!(rule0.chain, vec!["bind:alpha", "queue"]);
        assert!(rule0.options.contains_key("rate_cap"));

        let rule1 = &cfg.routing.rules[1];
        assert_eq!(rule1.prefix, "");
        // name + options default to empty when omitted
        assert_eq!(rule1.name, "");
        assert!(rule1.options.is_empty());
        assert_eq!(rule1.chain, vec!["bind:beta"]);
    }

    #[test]
    fn empty_config_uses_all_defaults() {
        // An entirely empty document must parse — every top-level field
        // has a `#[serde(default)]`.
        let cfg: SmppConfig = serde_yaml::from_str("{}").expect("empty maps to defaults");
        assert!(cfg.binds.is_empty());
        assert!(cfg.routing.default_chain.is_empty());
        assert!(cfg.routing.rules.is_empty());
        assert!(cfg.server.bind_address.is_none());
        assert!(cfg.server.port.is_none());
        assert!(cfg.bind_address.is_none());
        assert!(cfg.port.is_none());
        // A *missing* `server:` key resolves the whole field via
        // `#[serde(default)]` → `ServerConfig::default()`, which is all
        // zeros — the per-field `#[serde(default = …)]` fns only fire
        // when `server:` is present but individual keys are absent (see
        // `server_timer_field_defaults_apply_when_server_present`).
        assert_eq!(cfg.server.session_init_timer_ms, 0);
        assert_eq!(cfg.server.enquire_link_timer_ms, 0);
        assert_eq!(cfg.server.inactivity_timer_ms, 0);
        assert_eq!(cfg.server.response_timer_ms, 0);
    }

    #[test]
    fn server_timer_field_defaults_apply_when_server_present() {
        // When `server:` exists but timer keys are omitted, the
        // field-level serde defaults kick in.
        let cfg: SmppConfig = serde_yaml::from_str("server:\n  port: 2775\n").unwrap();
        assert_eq!(cfg.server.session_init_timer_ms, default_session_init());
        assert_eq!(cfg.server.enquire_link_timer_ms, default_enquire_link());
        assert_eq!(cfg.server.inactivity_timer_ms, default_inactivity());
        assert_eq!(cfg.server.response_timer_ms, default_response());
        // Inbound throttle defaults to unlimited (0) / pace when omitted.
        assert_eq!(cfg.server.max_msg_per_sec, 0);
        assert_eq!(cfg.server.throttle_action, ThrottleAction::Pace);
    }

    #[test]
    fn server_config_default_values() {
        let s = ServerConfig::default();
        // `#[derive(Default)]` does NOT run the serde default fns, so the
        // timer fields are 0 here — this documents that behaviour and is
        // why `#[serde(default = …)]` is required on each field.
        assert_eq!(s.session_init_timer_ms, 0);
        assert!(s.bind_address.is_none());
        assert!(s.tls.is_none());
    }

    #[test]
    fn missing_required_bind_field_is_a_parse_error() {
        // `host` is mandatory (no serde default) — omitting it must error.
        let yaml = r#"
binds:
  - name: broken
    port: 2775
    system_id: x
    password: y
"#;
        let err = serde_yaml::from_str::<SmppConfig>(yaml).unwrap_err();
        assert!(
            err.to_string().contains("host"),
            "error should mention the missing field: {err}"
        );
    }

    // ── listen() precedence ─────────────────────────────────────────────

    #[test]
    fn listen_prefers_nested_server() {
        let cfg: SmppConfig = serde_yaml::from_str(
            "server:\n  bind_address: 10.0.0.1\n  port: 5000\nbind_address: 9.9.9.9\nport: 1234\n",
        )
        .unwrap();
        assert_eq!(cfg.listen(), ("10.0.0.1".to_string(), 5000));
    }

    #[test]
    fn listen_falls_back_to_flat_then_default() {
        // flat-only (back-compat shape)
        let flat: SmppConfig = serde_yaml::from_str("bind_address: 8.8.8.8\nport: 4321\n").unwrap();
        assert_eq!(flat.listen(), ("8.8.8.8".to_string(), 4321));

        // nothing set → hard defaults
        let empty: SmppConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(empty.listen(), ("0.0.0.0".to_string(), 2775));
    }

    // ── ${VAR} expansion ────────────────────────────────────────────────

    #[test]
    fn expand_env_vars_substitutes_and_defaults() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // unique name avoids cross-test env clashes
        std::env::set_var("SMPP_TEST_EXPAND_HOST", "expanded.example.com");
        let out = expand_env_vars(
            "host: ${SMPP_TEST_EXPAND_HOST}\nport: ${SMPP_TEST_EXPAND_MISSING:-2775}\n",
        );
        std::env::remove_var("SMPP_TEST_EXPAND_HOST");

        assert!(out.contains("host: expanded.example.com"));
        assert!(out.contains("port: 2775"));
    }

    #[test]
    fn expand_env_vars_unclosed_is_left_literal() {
        // A `${` with no closing brace is emitted verbatim, not dropped.
        let out = expand_env_vars("a ${UNCLOSED literal");
        assert_eq!(out, "a ${UNCLOSED literal");
    }

    #[test]
    fn expand_env_vars_missing_without_default_is_empty() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("SMPP_TEST_DEFINITELY_UNSET_XYZ");
        let out = expand_env_vars("v=[${SMPP_TEST_DEFINITELY_UNSET_XYZ}]");
        assert_eq!(out, "v=[]");
    }

    // ── binds_from_env discovery ────────────────────────────────────────
    //
    // These tests mutate the global process environment. Each uses a
    // bind NAME unique to the test so the discovered set never collides
    // across tests, and every var is removed before returning.

    /// RAII guard that removes a list of env vars on drop, so a failing
    /// assertion can't leak state into another test.
    struct EnvGuard(Vec<String>);
    impl EnvGuard {
        fn set(pairs: &[(&str, &str)]) -> Self {
            let mut keys = Vec::new();
            for (k, v) in pairs {
                std::env::set_var(k, v);
                keys.push((*k).to_string());
            }
            EnvGuard(keys)
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for k in &self.0 {
                std::env::remove_var(k);
            }
        }
    }

    #[test]
    fn binds_from_env_discovers_full_bind() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g = EnvGuard::set(&[
            ("SMPP_BIND_ENVALPHA_HOST", "env-a.example.net"),
            ("SMPP_BIND_ENVALPHA_PORT", "2779"),
            ("SMPP_BIND_ENVALPHA_SYSTEM_ID", "env_alpha"),
            ("SMPP_BIND_ENVALPHA_PASSWORD", "env-pass"),
            ("SMPP_BIND_ENVALPHA_SYSTEM_TYPE", "VMS"),
            ("SMPP_BIND_ENVALPHA_BIND_TYPE", "receiver"),
            ("SMPP_BIND_ENVALPHA_MAX_MPS", "75"),
            ("SMPP_BIND_ENVALPHA_ENQUIRE_LINK_MS", "12000"),
            ("SMPP_BIND_ENVALPHA_RESPONSE_MS", "8000"),
            ("SMPP_BIND_ENVALPHA_TLS", "true"),
        ]);

        let binds = binds_from_env().expect("discovery succeeds");
        let b = binds
            .iter()
            .find(|b| b.name == "envalpha")
            .expect("envalpha discovered");

        // NAME is lowercased for the bind identity
        assert_eq!(b.name, "envalpha");
        assert_eq!(b.host, "env-a.example.net");
        assert_eq!(b.port, 2779);
        assert_eq!(b.system_id, "env_alpha");
        assert_eq!(b.password, "env-pass");
        assert_eq!(b.system_type, "VMS");
        assert_eq!(b.bind_type, "receiver");
        assert_eq!(b.max_msg_per_sec, 75);
        assert_eq!(b.enquire_link_timer_ms, 12000);
        assert_eq!(b.response_timer_ms, 8000);
        assert!(b.tls.is_some());
    }

    #[test]
    fn binds_from_env_applies_defaults() {
        // Only the discovery signal (HOST) + required fields set; the
        // rest must fall back to defaults.
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g = EnvGuard::set(&[
            ("SMPP_BIND_ENVMIN_HOST", "min.example.net"),
            ("SMPP_BIND_ENVMIN_SYSTEM_ID", "min_id"),
            ("SMPP_BIND_ENVMIN_PASSWORD", "min_pw"),
        ]);

        let binds = binds_from_env().unwrap();
        let b = binds.iter().find(|b| b.name == "envmin").unwrap();

        assert_eq!(b.port, 2775);
        assert_eq!(b.max_msg_per_sec, 0);
        assert_eq!(b.system_type, "");
        assert_eq!(b.bind_type, default_bind_type());
        assert_eq!(b.enquire_link_timer_ms, default_enquire_link());
        assert_eq!(b.response_timer_ms, default_response());
        // TLS unset → no TlsConfig
        assert!(b.tls.is_none());
    }

    #[test]
    fn binds_from_env_without_host_is_ignored() {
        // No HOST → not a discovery signal → bind not produced even
        // though other fields are present.
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g = EnvGuard::set(&[
            ("SMPP_BIND_ENVGHOST_SYSTEM_ID", "ghost"),
            ("SMPP_BIND_ENVGHOST_PASSWORD", "boo"),
        ]);
        let binds = binds_from_env().unwrap();
        assert!(binds.iter().all(|b| b.name != "envghost"));
    }

    #[test]
    fn binds_from_env_missing_required_field_errors() {
        // HOST present (discovery fires) but PASSWORD missing → error.
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g = EnvGuard::set(&[
            ("SMPP_BIND_ENVMISS_HOST", "miss.example.net"),
            ("SMPP_BIND_ENVMISS_SYSTEM_ID", "miss_id"),
            // no PASSWORD
        ]);
        let err = binds_from_env().unwrap_err();
        match err {
            ConfigError::EnvBindMissing { bind, field } => {
                assert_eq!(bind, "envmiss");
                assert_eq!(field, "PASSWORD");
            }
            other => panic!("expected EnvBindMissing, got {other:?}"),
        }
    }

    #[test]
    fn binds_from_env_invalid_numeric_value_errors() {
        // PORT is non-numeric → EnvBindInvalid.
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g = EnvGuard::set(&[
            ("SMPP_BIND_ENVBADPORT_HOST", "bad.example.net"),
            ("SMPP_BIND_ENVBADPORT_SYSTEM_ID", "bad_id"),
            ("SMPP_BIND_ENVBADPORT_PASSWORD", "bad_pw"),
            ("SMPP_BIND_ENVBADPORT_PORT", "not-a-number"),
        ]);
        let err = binds_from_env().unwrap_err();
        match err {
            ConfigError::EnvBindInvalid {
                bind, field, value, ..
            } => {
                assert_eq!(bind, "envbadport");
                assert_eq!(field, "PORT");
                assert_eq!(value, "not-a-number");
            }
            other => panic!("expected EnvBindInvalid, got {other:?}"),
        }
    }

    #[test]
    fn config_error_messages_render() {
        // The Display impls embed the uppercased bind name + field.
        let missing = ConfigError::EnvBindMissing {
            bind: "alpha".into(),
            field: "PASSWORD",
        };
        let msg = missing.to_string();
        assert!(msg.contains("SMPP_BIND_ALPHA_PASSWORD"), "got: {msg}");
    }

    #[test]
    fn server_max_mps_env_overrides_yaml() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // YAML sets 10; the env override must win with 500.
        let path = std::env::temp_dir().join("siphon_smpp_server_max_mps_test.yaml");
        std::fs::write(&path, "server:\n  port: 2775\n  max_msg_per_sec: 10\n").unwrap();
        let _g = EnvGuard::set(&[("SMPP_SERVER_MAX_MPS", "500")]);

        let cfg = SmppConfig::from_file(&path).expect("loads");
        std::fs::remove_file(&path).ok();

        assert_eq!(cfg.server.max_msg_per_sec, 500);
    }

    #[test]
    fn server_max_mps_env_invalid_is_ignored() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // A non-numeric override is warned-and-skipped; the YAML value stands.
        let path = std::env::temp_dir().join("siphon_smpp_server_max_mps_bad_test.yaml");
        std::fs::write(&path, "server:\n  port: 2775\n  max_msg_per_sec: 42\n").unwrap();
        let _g = EnvGuard::set(&[("SMPP_SERVER_MAX_MPS", "not-a-number")]);

        let cfg = SmppConfig::from_file(&path).expect("loads");
        std::fs::remove_file(&path).ok();

        assert_eq!(cfg.server.max_msg_per_sec, 42);
    }

    #[test]
    fn server_throttle_action_env_overrides_yaml() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // YAML omits the action (defaults to pace); env forces reject.
        let path = std::env::temp_dir().join("siphon_smpp_throttle_action_test.yaml");
        std::fs::write(&path, "server:\n  port: 2775\n  max_msg_per_sec: 10\n").unwrap();
        let _g = EnvGuard::set(&[("SMPP_SERVER_THROTTLE_ACTION", "REJECT")]);

        let cfg = SmppConfig::from_file(&path).expect("loads");
        std::fs::remove_file(&path).ok();

        // Case-insensitive.
        assert_eq!(cfg.server.throttle_action, ThrottleAction::Reject);
    }

    #[test]
    fn server_throttle_action_env_invalid_is_ignored() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // A bad action leaves the YAML value (reject) in place.
        let path = std::env::temp_dir().join("siphon_smpp_throttle_action_bad_test.yaml");
        std::fs::write(&path, "server:\n  port: 2775\n  throttle_action: reject\n").unwrap();
        let _g = EnvGuard::set(&[("SMPP_SERVER_THROTTLE_ACTION", "nonsense")]);

        let cfg = SmppConfig::from_file(&path).expect("loads");
        std::fs::remove_file(&path).ok();

        assert_eq!(cfg.server.throttle_action, ThrottleAction::Reject);
    }
}
