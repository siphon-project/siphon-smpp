//! Prometheus metrics for the SMPP addon.
//!
//! Every series is registered into siphon's shared metrics store via
//! [`siphon::metrics::custom_metrics`] — the same registry that backs the
//! host's `/metrics` endpoint. siphon-smpp therefore needs no `prometheus`
//! dependency of its own: values cross the siphon-smpp↔siphon-sip boundary
//! as `&str` / `f64`, never as `prometheus` types, so there is no
//! two-copies-of-a-linked-crate coupling to siphon-sip's `prometheus` (the
//! same hazard class as the siphon-sip-URL and pyo3 pins).
//!
//! When the host has not initialised its metrics engine (e.g. a headless
//! run with no admin server) `custom_metrics()` is `None`: registration and
//! the sampler are skipped with one log line and every `record_*` / gauge
//! helper becomes a no-op — never a panic, never a broken bind path (mirrors
//! the SCTP-feature silent-skip).
//!
//! Two emit patterns:
//! * the `siphon_smpp_binds` gauge is **sampled** on a 10s timer (reading
//!   the authoritative session vecs can't drift or miss a transition);
//! * the counters / histogram are recorded **inline** at the PDU dispatch
//!   and bind sites.

use std::sync::Arc;
use std::time::Duration;

use siphon::metrics::custom::CustomMetrics;
use tokio::sync::Mutex;

use crate::runtime::{BindSession, EsmeSession, State};

// ── Series names ─────────────────────────────────────────────────────────

const BINDS: &str = "siphon_smpp_binds";
const PDUS: &str = "siphon_smpp_pdus_total";
const THROTTLED: &str = "siphon_smpp_throttled_total";
const RECONNECTS: &str = "siphon_smpp_bind_reconnects_total";
const DISPATCH_ERRORS: &str = "siphon_smpp_dispatch_errors_total";
const DISPATCH_DURATION: &str = "siphon_smpp_dispatch_duration_seconds";
const BIND_REQUESTS: &str = "siphon_smpp_bind_requests_total";

// ── Label values (shared so call sites can't typo a label) ───────────────

/// `direction` label: an external ESME bound to *our* listener.
pub(crate) const INBOUND: &str = "inbound";
/// `direction` label: *our* outbound client bind to the trunk.
pub(crate) const EGRESS: &str = "egress";

/// `result` label: the PDU was accepted on the wire.
pub(crate) const ACCEPTED: &str = "accepted";
/// `result` label: the PDU was rejected on the wire (script status,
/// no handler, throttle, or a raised handler).
pub(crate) const REJECTED: &str = "rejected";

/// `state` label. v1 emits `bound` only — `connecting` / `unbound` are not
/// in shared state (a still-connecting bind lives in the reconnect
/// supervisor, not `State.binds`).
const BOUND: &str = "bound";

/// Sampler cadence. The scrape is ~15s; 10s keeps the gauge fresh at the
/// cost of two brief `.len()` reads.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Handler-latency histogram buckets (seconds), matching siphon's own
/// `request_duration_seconds` shape.
const DURATION_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];

// ── Registration ─────────────────────────────────────────────────────────

/// Register every SMPP series into the shared store. Called once at
/// startup. A duplicate registration (a defensive double-call, or a second
/// `runtime::spawn`) returns `Err` from the store and is logged-and-ignored
/// — never fatal.
fn register_all(cm: &CustomMetrics) {
    fn note(result: Result<(), String>, name: &str) {
        if let Err(error) = result {
            tracing::debug!(target: "siphon_smpp", metric = name, error = %error,
                "metric already registered; ignoring");
        }
    }

    note(
        cm.register_gauge(BINDS, "Current SMPP bind sessions", &["direction", "state"]),
        BINDS,
    );
    note(
        cm.register_counter(
            PDUS,
            "SMPP PDUs handled, by direction, command and wire result",
            &["direction", "command", "result"],
        ),
        PDUS,
    );
    note(
        cm.register_counter(
            THROTTLED,
            "SMPP PDUs paced or rejected by a rate limiter",
            &["direction"],
        ),
        THROTTLED,
    );
    note(
        cm.register_counter(
            RECONNECTS,
            "Outbound SMPP bind reconnects (an established session dropped)",
            &["bind"],
        ),
        RECONNECTS,
    );
    note(
        cm.register_counter(
            DISPATCH_ERRORS,
            "SMPP dispatches where the script handler raised",
            &["command"],
        ),
        DISPATCH_ERRORS,
    );
    note(
        cm.register_histogram(
            DISPATCH_DURATION,
            "SMPP script handler dispatch duration in seconds",
            &["command"],
            DURATION_BUCKETS.to_vec(),
        ),
        DISPATCH_DURATION,
    );
    note(
        cm.register_counter(
            BIND_REQUESTS,
            "Inbound SMPP bind requests, by outcome",
            &["result"],
        ),
        BIND_REQUESTS,
    );
}

// ── Inline emit helpers (no-op when the host metrics engine is absent) ────

#[inline]
fn store() -> Option<&'static Arc<CustomMetrics>> {
    siphon::metrics::custom_metrics()
}

/// Whether the host metrics engine is up. One `OnceLock` read; used to skip
/// the dispatch-path clock reads entirely when metrics are disabled.
#[inline]
pub(crate) fn enabled() -> bool {
    store().is_some()
}

/// Count a handled PDU by `direction` (`INBOUND`/`EGRESS`), SMPP `command`
/// name and `result` (`ACCEPTED`/`REJECTED`).
pub(crate) fn record_pdu(direction: &str, command: &str, result: &str) {
    if let Some(cm) = store() {
        let _ = cm.counter_inc(
            PDUS,
            &[
                ("direction", direction),
                ("command", command),
                ("result", result),
            ],
            1.0,
        );
    }
}

/// Count a PDU that was paced (delayed for a token) or rejected
/// (`ESME_RTHROTTLED`) by a rate limiter, by `direction`.
pub(crate) fn record_throttled(direction: &str) {
    if let Some(cm) = store() {
        let _ = cm.counter_inc(THROTTLED, &[("direction", direction)], 1.0);
    }
}

/// Count a reconnect of the named outbound bind (an established session
/// dropped and the supervisor re-established it).
pub(crate) fn record_bind_reconnect(bind: &str) {
    if let Some(cm) = store() {
        let _ = cm.counter_inc(RECONNECTS, &[("bind", bind)], 1.0);
    }
}

/// Count a dispatch where the `@smpp.on_pdu(command)` handler raised.
pub(crate) fn record_dispatch_error(command: &str) {
    if let Some(cm) = store() {
        let _ = cm.counter_inc(DISPATCH_ERRORS, &[("command", command)], 1.0);
    }
}

/// Observe a script handler dispatch duration (seconds) for `command`.
pub(crate) fn observe_dispatch(command: &str, seconds: f64) {
    if let Some(cm) = store() {
        let _ = cm.histogram_observe(DISPATCH_DURATION, &[("command", command)], seconds);
    }
}

/// Count an inbound bind request by `result` (`ACCEPTED`/`REJECTED`).
pub(crate) fn record_bind_request(result: &str) {
    if let Some(cm) = store() {
        let _ = cm.counter_inc(BIND_REQUESTS, &[("result", result)], 1.0);
    }
}

// ── Binds gauge (sampled) ─────────────────────────────────────────────────

/// Read the current bound-session counts as `(egress, inbound)` =
/// `(outbound binds, inbound ESMEs)`. Locks each vec only long enough to
/// read its length. Pure over its two arguments so it is unit-testable
/// without a live socket or a `ScriptHandle`.
pub(crate) async fn sample_binds(
    binds: &Mutex<Vec<BindSession>>,
    esmes: &Mutex<Vec<EsmeSession>>,
) -> (u64, u64) {
    let egress = binds.lock().await.len() as u64;
    let inbound = esmes.lock().await.len() as u64;
    (egress, inbound)
}

/// Set the `siphon_smpp_binds` gauge from a sample.
fn set_binds(cm: &CustomMetrics, egress: u64, inbound: u64) {
    let _ = cm.gauge_set(
        BINDS,
        &[("direction", EGRESS), ("state", BOUND)],
        egress as f64,
    );
    let _ = cm.gauge_set(
        BINDS,
        &[("direction", INBOUND), ("state", BOUND)],
        inbound as f64,
    );
}

/// Install the SMPP metrics: register every series and spawn the binds
/// sampler on `handle`. Skips (with one `warn!`) when the host metrics
/// engine is not initialised; the sampler loop ends with the process.
pub(crate) fn install(handle: &tokio::runtime::Handle, state: &Arc<State>) {
    let Some(cm) = siphon::metrics::custom_metrics() else {
        tracing::warn!(target: "siphon_smpp",
            "host metrics engine not initialised; siphon_smpp_* metrics disabled");
        return;
    };
    register_all(cm);

    let cm = cm.clone();
    let state = state.clone();
    handle.spawn(async move {
        let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
        loop {
            tick.tick().await;
            let (egress, inbound) = sample_binds(&state.binds, &state.esmes).await;
            set_binds(&cm, egress, inbound);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sample_binds_empty_is_zero() {
        // No sockets / ScriptHandle needed: empty vecs → (0, 0). Covers the
        // lock/len/cast path and the `→ 0` baseline the egress-drop alarm
        // relies on. (Non-empty asymmetric counts would need real
        // `SMSC`/`ESME` sessions, which smpp34 exposes no way to fabricate;
        // the egress-vs-inbound mapping is verified by the encode test below
        // and the end-to-end acceptance check.)
        let binds = Mutex::new(Vec::<BindSession>::new());
        let esmes = Mutex::new(Vec::<EsmeSession>::new());
        assert_eq!(sample_binds(&binds, &esmes).await, (0, 0));
    }

    /// The single sole-owner test of the process-global metrics registry:
    /// registration, the guarded double-register no-op, every emit helper,
    /// and the Prometheus text encoding. It is the only test that touches
    /// `siphon_smpp_*` in the shared registry, so the series values are
    /// stable under parallel test execution. Mirrors siphon-sip's own
    /// `metrics_appear_in_encode`.
    #[test]
    fn register_emit_and_encode() {
        siphon::metrics::init().expect("metrics init");
        let cm = siphon::metrics::custom_metrics().expect("custom metrics present after init");

        register_all(cm);
        // Double-register is a guarded no-op: the store rejects a duplicate…
        assert!(cm
            .register_gauge(BINDS, "dup", &["direction", "state"])
            .is_err());
        // …and `register_all` swallows that Err rather than panicking.
        register_all(cm);

        set_binds(cm, 1, 2);
        record_pdu(INBOUND, "submit_sm", ACCEPTED);
        record_pdu(EGRESS, "deliver_sm", REJECTED);
        record_throttled(INBOUND);
        record_bind_reconnect("trunk");
        record_dispatch_error("submit_sm");
        observe_dispatch("submit_sm", 0.012);
        record_bind_request(ACCEPTED);

        let out = siphon::metrics::encode_metrics();
        // Prometheus sorts label names alphabetically in the text encoding.
        assert!(
            out.contains(r#"siphon_smpp_binds{direction="egress",state="bound"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"siphon_smpp_binds{direction="inbound",state="bound"} 2"#),
            "{out}"
        );
        assert!(
            out.contains(
                r#"siphon_smpp_pdus_total{command="submit_sm",direction="inbound",result="accepted"} 1"#
            ),
            "{out}"
        );
        assert!(
            out.contains(
                r#"siphon_smpp_pdus_total{command="deliver_sm",direction="egress",result="rejected"} 1"#
            ),
            "{out}"
        );
        assert!(
            out.contains(r#"siphon_smpp_throttled_total{direction="inbound"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"siphon_smpp_bind_reconnects_total{bind="trunk"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"siphon_smpp_dispatch_errors_total{command="submit_sm"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"siphon_smpp_dispatch_duration_seconds_count{command="submit_sm"} 1"#),
            "{out}"
        );
        assert!(
            out.contains(r#"siphon_smpp_bind_requests_total{result="accepted"} 1"#),
            "{out}"
        );
    }
}
