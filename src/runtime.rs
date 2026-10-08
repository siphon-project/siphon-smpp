//! Tokio-side SMPP runtime.
//!
//! Drives both directions:
//!
//! * **Client / binds** — for each `cfg.binds` entry, an `SmppClient`
//!   with reconnect-with-backoff. The client listener dispatches inbound
//!   `deliver_sm` (incl. delivery receipts), `data_sm` and
//!   `alert_notification` into the script's `@smpp.on_pdu(...)` handlers.
//!   Bound `SMSC` handles are tracked in [`State`] so the outbound send
//!   helpers (`smpp.submit_via(bind=…)` etc.) can reach the right
//!   session.
//! * **Server** — `SmppServer` accepts inbound binds on the configured
//!   listen address. `bind_transmitter` / `bind_receiver` are rejected;
//!   `bind_transceiver` is handed to `@smpp.on_bind` for accept/reject
//!   *with an explicit status + reason*. `submit_sm`, `data_sm` and
//!   `cancel_sm` dispatch into `@smpp.on_pdu(...)`. Bound ESME sessions
//!   are tracked so `smpp.deliver_to(session_id=…)` can MT back to them.
//!   Inbound message PDUs (`submit_sm` / `data_sm` / `submit_sm_multi`)
//!   are rate-limited per session by `server.max_msg_per_sec` — the
//!   ingress mirror of a bind's outbound `max_msg_per_sec` throttle.
//!   `server.throttle_action` selects what happens over the cap: `pace`
//!   (delay the resp) or `reject` (answer `ESME_RTHROTTLED`).
//!
//! Both listeners read the script handler table from the
//! [`siphon::script::ScriptHandle`] on every dispatch (via
//! `handlers_for(...)`), so a hot-reloaded script is picked up on the
//! next PDU.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use pyo3::prelude::*;
use siphon::script::ScriptHandle;
use smpp34::{
    alert_notification, bind_receiver, bind_receiver_resp, bind_transceiver, bind_transceiver_resp,
    bind_transmitter, bind_transmitter_resp, cancel_sm, cancel_sm_resp,
    client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC},
    data_sm, data_sm_resp, deliver_sm, deliver_sm_resp, query_sm, query_sm_resp, replace_sm,
    replace_sm_resp,
    server::ESME,
    submit_sm, submit_sm_multi, submit_sm_multi_resp, submit_sm_resp, SmppConnectionInformation,
    SmppError, SmppServer, SmppServerListener,
};
use tokio::sync::Mutex;

use crate::config::{BindConfig, ThrottleAction};
use crate::metrics;
use crate::pyclasses::{AlertNotification, Bind, BindResult, Pdu, PduReply, Session, SourceKind};
use crate::SmppConfig;

// ── Shared state ────────────────────────────────────────────────────────

pub(crate) struct State {
    pub binds: Mutex<Vec<BindSession>>,
    /// Bound inbound ESME sessions.
    pub esmes: Mutex<Vec<EsmeSession>>,
    /// Inbound throughput cap (msg/s) applied per bound ESME session;
    /// 0 = unlimited. Read once at spawn from `server.max_msg_per_sec`
    /// and used to build each session's limiter in `on_esme_bound`.
    pub inbound_max_mps: u32,
    /// What to do when an inbound submit exceeds `inbound_max_mps`:
    /// pace (delay the resp) or reject with `ESME_RTHROTTLED`.
    pub inbound_throttle_action: ThrottleAction,
    /// `server.response_timer_ms`: how long a request we send to a bound
    /// ESME waits for its response. Copied onto each [`EsmeSession`] so
    /// the send helpers can tell a timeout from a session that closed.
    pub inbound_response_timer: std::time::Duration,
    /// Set by [`SmppServerListener::on_listen_failed`] when the listening
    /// socket could not be bound. Terminal: no connection is ever accepted
    /// after it, so the server task reports it and stops instead of parking
    /// as though it were listening.
    pub listen_failure: OnceLock<String>,
    pub script: ScriptHandle,
}

pub(crate) struct BindSession {
    pub name: String,
    /// `Arc` so the send helpers can clone the handle out of the bind
    /// list, drop the lock, and await the response without blocking
    /// other binds behind a single mutex.
    pub smsc: Arc<SMSC>,
    /// Per-bind outbound rate limiter (`max_msg_per_sec`); `None` when
    /// unlimited.
    pub throttle: Option<Arc<RateLimiter>>,
    /// The bind's `response_timer_ms` — the same value the session was
    /// started with, kept here because `SMSC` does not expose it.
    pub response_timer: std::time::Duration,
}

/// A bound inbound ESME session (an ESME that connected to *us*).
pub(crate) struct EsmeSession {
    /// `Arc` so a send helper can clone the handle out, drop the lock,
    /// and await without blocking other sessions behind the mutex.
    pub esme: Arc<ESME>,
    /// Per-session inbound rate limiter (`server.max_msg_per_sec`);
    /// `None` when unlimited. Gates inbound `submit_sm` / `data_sm` /
    /// `submit_sm_multi` (paced or rejected per `server.throttle_action`)
    /// — the ingress mirror of `BindSession::throttle`.
    pub throttle: Option<Arc<RateLimiter>>,
    /// `server.response_timer_ms`, as the session was started with.
    pub response_timer: std::time::Duration,
}

/// True when `smsc` is no longer a registered outbound session — it was
/// removed by [`SmppClientListener::on_smsc_unbound`]. smpp34 awaits that
/// hook before it releases the requests still outstanding on the session,
/// so a send helper that sees its request fail can ask here whether the
/// session ending is why.
pub(crate) async fn bind_gone(binds: &Mutex<Vec<BindSession>>, smsc: &Arc<SMSC>) -> bool {
    !binds
        .lock()
        .await
        .iter()
        .any(|b| Arc::ptr_eq(&b.smsc, smsc))
}

/// The inbound mirror of [`bind_gone`]: true once
/// [`SmppServerListener::on_esme_unbound`] has dropped `esme`.
pub(crate) async fn esme_gone(esmes: &Mutex<Vec<EsmeSession>>, esme: &Arc<ESME>) -> bool {
    !esmes
        .lock()
        .await
        .iter()
        .any(|e| Arc::ptr_eq(&e.esme, esme))
}

pub(crate) static STATE: OnceLock<Arc<State>> = OnceLock::new();

/// Public accessor used by the send helpers in [`crate::sends`].
pub(crate) fn state() -> Option<Arc<State>> {
    STATE.get().cloned()
}

// ── Rate limiter (token bucket) ─────────────────────────────────────────

/// Simple async token-bucket used to honour `max_msg_per_sec` in both
/// directions: one per outbound bind (paces `submit_via` etc.) and one
/// per inbound ESME session (gates inbound `submit_sm` / `data_sm` /
/// `submit_sm_multi`). [`acquire`](Self::acquire) paces (awaits) — a
/// throughput cap as a speed limit, not an error — while
/// [`try_acquire`](Self::try_acquire) gates without waiting for the
/// inbound `reject` action. Capacity is one second's worth of tokens, so
/// short bursts pass through and sustained load settles at the
/// configured rate.
pub(crate) struct RateLimiter {
    inner: Mutex<Bucket>,
}

struct Bucket {
    tokens: f64,
    max: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl RateLimiter {
    /// `rate` is messages/second; must be > 0 (callers pass `None`
    /// instead of a zero-rate limiter).
    pub(crate) fn new(rate: u32) -> Self {
        let r = f64::from(rate.max(1));
        Self {
            inner: Mutex::new(Bucket {
                tokens: r,
                max: r,
                refill_per_sec: r,
                last: Instant::now(),
            }),
        }
    }

    /// Block until a token is available, then consume it. Returns `true`
    /// if it had to wait for a refill (the caller was paced/throttled),
    /// `false` if a token was free immediately.
    pub(crate) async fn acquire(&self) -> bool {
        let mut waited = false;
        loop {
            let wait = {
                let mut b = self.inner.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.tokens = (b.tokens + elapsed * b.refill_per_sec).min(b.max);
                b.last = now;
                if b.tokens >= 1.0 {
                    b.tokens -= 1.0;
                    return waited;
                }
                let deficit = 1.0 - b.tokens;
                std::time::Duration::from_secs_f64(deficit / b.refill_per_sec)
            };
            waited = true;
            tokio::time::sleep(wait).await;
        }
    }

    /// Non-blocking variant: consume a token if one is available and
    /// return `true`; return `false` (without waiting) when the bucket is
    /// empty. Used by the `reject` throttle action to answer over-rate
    /// submits with `ESME_RTHROTTLED` instead of pacing them.
    pub(crate) async fn try_acquire(&self) -> bool {
        let mut b = self.inner.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * b.refill_per_sec).min(b.max);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// ── Spawn ───────────────────────────────────────────────────────────────

pub fn spawn(cfg: SmppConfig, script: ScriptHandle) {
    let state = Arc::new(State {
        binds: Mutex::new(Vec::new()),
        esmes: Mutex::new(Vec::new()),
        inbound_max_mps: cfg.server.max_msg_per_sec,
        inbound_throttle_action: cfg.server.throttle_action,
        inbound_response_timer: std::time::Duration::from_millis(cfg.server.response_timer_ms),
        listen_failure: OnceLock::new(),
        script: script.clone(),
    });
    if STATE.set(state.clone()).is_err() {
        tracing::warn!(target: "siphon_smpp",
            "runtime::spawn called twice; ignoring second invocation");
        return;
    }
    let handle = script.tokio_handle().clone();

    // Register the SMPP metrics and spawn the binds sampler. Guarded, and
    // a no-op when the host metrics engine is not initialised.
    metrics::install(&handle, &state);

    // ── Server (inbound binds) ──────────────────────────────────────
    let (host, port) = cfg.listen();
    let listen_addr: std::net::IpAddr = host.parse().unwrap_or_else(|_| {
        tracing::error!(target: "siphon_smpp",
            host=%host, "bad bind_address; defaulting to 0.0.0.0");
        std::net::IpAddr::from([0u8, 0, 0, 0])
    });
    let server_listener: Arc<dyn SmppServerListener + Send + Sync> = state.clone();
    let session_init = cfg.server.session_init_timer_ms;
    let enquire_link = cfg.server.enquire_link_timer_ms;
    let inactivity = cfg.server.inactivity_timer_ms;
    let response = cfg.server.response_timer_ms;
    let log_host = host.clone();
    let server_state = state.clone();
    handle.spawn(async move {
        let mut server = SmppServer::new_with_default_timers(
            listen_addr,
            port,
            server_listener,
            session_init,
            enquire_link,
            inactivity,
            response,
            1500,
        );
        server.start().await;
        // As of smpp34 1.4.0 `start()` binds the listening socket before it
        // returns, so by here the server is either accepting or has told us
        // through `on_listen_failed` why it never will. Logging "listening"
        // before the call — as this did — could claim a port we never got.
        if let Some(error) = server_state.listen_failure.get() {
            tracing::error!(target: "siphon_smpp",
                host=%log_host, port=port, error=%error,
                "SMPP server could not listen; inbound binds are DOWN for this process");
            // Terminal: no connection will ever be accepted. Return rather
            // than parking forever pretending to serve.
            return;
        }
        tracing::info!(target: "siphon_smpp",
            host=%log_host, port=port, "SMPP server listening");
        // SmppServer::start spawns its accept loop in a child task and
        // returns; if we drop `server` here its Drop impl stops the
        // accept loop. Keep the wrapper alive for the runtime's
        // lifetime by parking forever.
        std::future::pending::<()>().await;
    });

    // ── Outbound binds (one supervisor task per configured bind) ────
    for bind in cfg.binds.iter().cloned() {
        let st = state.clone();
        handle.spawn(async move {
            run_bind_loop(st, bind).await;
        });
    }
}

/// Should the bind supervisor keep waiting for an attempt to resolve?
///
/// Three ways out, and the middle one is the reason this is a function:
/// the session came up, the connect failed outright, or the deadline ran
/// out. A refused connect starts no session at all, so `is_alive()` can
/// never flip for it — waiting on the deadline alone costs the full
/// `bind_deadline` on every retry against a peer that is simply down.
fn awaiting_bind(
    alive: bool,
    connect_failed: bool,
    waited: std::time::Duration,
    deadline: std::time::Duration,
) -> bool {
    !alive && !connect_failed && waited < deadline
}

/// Per-bind supervisor: bind, run until disconnect, reconnect after
/// exponential backoff (capped at 60s).
///
/// `SmppClient::start` does NOT block until disconnect — it kicks off
/// the connect+bind in a `tokio::spawn(...)` and returns in microseconds
/// (smpp34/src/client/mod.rs). So we drive the lifecycle ourselves:
///
///   1. start() — spawns the I/O task
///   2. wait for `is_alive()` to flip true (the spawned task sets this
///      after a successful bind response) or for the bind deadline to
///      expire
///   3. once alive, poll `is_alive()` until it flips false (peer closed,
///      enquire_link / response timeout fired, etc.)
///   4. log "bind down", apply backoff, loop
async fn run_bind_loop(state: Arc<State>, cfg: BindConfig) {
    let bind_type = match cfg.bind_type.as_str() {
        "transmitter" => BIND_TYPE::TX,
        "receiver" => BIND_TYPE::RX,
        _ => BIND_TYPE::TRX,
    };
    // Shared with the listener: the supervisor clears it before each
    // attempt and `on_connection_failed` raises it.
    let connect_failed = Arc::new(AtomicBool::new(false));
    let listener: Arc<dyn SmppClientListener + Send + Sync> = Arc::new(BindListener {
        state: state.clone(),
        bind_name: cfg.name.clone(),
        max_msg_per_sec: cfg.max_msg_per_sec,
        response_timer: std::time::Duration::from_millis(cfg.response_timer_ms),
        connect_failed: connect_failed.clone(),
    });

    // session_init_timer in the smpp34 client is 5s; give the bind a
    // little extra room for connect + DNS + TLS handshake on top.
    let bind_deadline = std::time::Duration::from_secs(15);
    // If a session stayed up for at least this long we treat the
    // disconnect as "transient blip" and reset backoff to 1s, so a
    // single slow read or ENQUIRE_LINK miss doesn't push us up the
    // exponential.
    let healthy_threshold = std::time::Duration::from_secs(30);
    // Polling cadence for is_alive(); 1s is fast enough for any
    // reconnect SLA we care about and slow enough not to burn CPU.
    let poll_interval = std::time::Duration::from_secs(1);

    let mut backoff_ms: u64 = 1_000;
    loop {
        // Fresh attempt: any failure recorded belongs to this one.
        connect_failed.store(false, Ordering::Relaxed);
        let mut client = SmppClient::new_with_default_timers(
            cfg.host.clone(),
            cfg.port,
            cfg.tls.is_some(),
            bind_type.clone(),
            cfg.system_id.clone(),
            cfg.password.clone(),
            cfg.system_type.clone(),
            1,
            1,
            String::new(),
            listener.clone(),
            5_000,
            cfg.enquire_link_timer_ms,
            60_000,
            cfg.response_timer_ms,
            1_500,
            20,
        );
        tracing::info!(target: "siphon_smpp",
            bind=%cfg.name, host=%cfg.host, port=cfg.port,
            "establishing outbound bind");
        client.start().await;

        // Phase 1: wait for the bind to complete, the connect to fail, or
        // the deadline to run out. Watching `connect_failed` matters
        // because a refused connect starts no session at all, so
        // `is_alive()` would stay false for the whole deadline and every
        // reconnect attempt against a peer that is simply down would cost
        // the full 15s before backing off.
        let bind_started = Instant::now();
        while awaiting_bind(
            client.is_alive(),
            connect_failed.load(Ordering::Relaxed),
            bind_started.elapsed(),
            bind_deadline,
        ) {
            tokio::time::sleep(poll_interval).await;
        }

        // Phase 2: if bound, hold the session until is_alive() flips false.
        let bound_at = if client.is_alive() {
            let now = Instant::now();
            while client.is_alive() {
                tokio::time::sleep(poll_interval).await;
            }
            Some(now)
        } else if connect_failed.load(Ordering::Relaxed) {
            // on_connection_failed already logged the cause at error level;
            // don't restate it as a timeout it never was.
            None
        } else {
            tracing::warn!(target: "siphon_smpp",
                bind=%cfg.name, host=%cfg.host, port=cfg.port,
                "bind did not complete within {:?}", bind_deadline);
            None
        };

        // Reset backoff if we held a healthy session for long enough.
        if bound_at
            .map(|t| t.elapsed() >= healthy_threshold)
            .unwrap_or(false)
        {
            backoff_ms = 1_000;
        }

        // Count only drops of a session that actually came up — an initial
        // connect that never bound is a failed attempt, not a reconnect.
        if bound_at.is_some() {
            metrics::record_bind_reconnect(&cfg.name);
        }
        tracing::warn!(target: "siphon_smpp",
            bind=%cfg.name, "bind down, reconnecting in {}ms", backoff_ms);
        // Drop `client` here so its Drop->stop() aborts any leftover
        // spawned tasks before we sleep + create a fresh SmppClient.
        drop(client);
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(60_000);
    }
}

// ── Server-side listener ────────────────────────────────────────────────

#[async_trait]
impl SmppServerListener for State {
    /// The listening socket could not be bound (port in use, an address
    /// this host does not own, a privileged port). Terminal — smpp34 never
    /// starts an accept loop — so this is recorded for the server task to
    /// act on rather than letting it park as though it were serving.
    /// Before smpp34 1.4.0 this case panicked a tokio worker.
    async fn on_listen_failed(&self, error: &str) {
        tracing::error!(target: "siphon_smpp", error=%error,
            "SMPP listening socket could not be bound");
        if self.listen_failure.set(error.to_string()).is_err() {
            tracing::warn!(target: "siphon_smpp", error=%error,
                "second listen failure reported; keeping the first");
        }
    }

    async fn on_bind_transmitter(
        &self,
        request: bind_transmitter,
        _conn: &SmppConnectionInformation,
        _session: &String,
    ) -> bind_transmitter_resp {
        // TX-only binds intentionally rejected — siphon-smpp only
        // supports transceiver binds (mirrors the reference policy).
        metrics::record_bind_request(metrics::REJECTED);
        request.reject(SmppError::ESME_RINVSYSID)
    }

    async fn on_bind_receiver(
        &self,
        request: bind_receiver,
        _conn: &SmppConnectionInformation,
        _session: &String,
    ) -> bind_receiver_resp {
        metrics::record_bind_request(metrics::REJECTED);
        request.reject(SmppError::ESME_RINVSYSID)
    }

    async fn on_bind_transceiver(
        &self,
        request: bind_transceiver,
        conn: &SmppConnectionInformation,
        _session: &String,
    ) -> bind_transceiver_resp {
        let outcome = dispatch_bind(
            &self.script,
            &request.system_id,
            &request.password,
            &conn.client_address.to_string(),
        )
        .await;

        if !outcome.accept {
            tracing::info!(target: "siphon_smpp",
                from=%conn.client_address, system_id=%request.system_id,
                status=?outcome.status, reason=%outcome.reason,
                "bind_transceiver rejected");
            metrics::record_bind_request(metrics::REJECTED);
            return request.reject(outcome.status);
        }
        tracing::info!(target: "siphon_smpp",
            from=%conn.client_address, system_id=%request.system_id,
            "bind_transceiver accepted");
        metrics::record_bind_request(metrics::ACCEPTED);
        let echo = request.system_id.clone();
        request.accept(echo, Some(0x34))
    }

    // on_unbind uses the trait default (accept).

    async fn on_submit_sm(
        &self,
        request: submit_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> submit_sm_resp {
        if let InboundAdmit::Throttled = self.admit_inbound(session_id).await {
            tracing::debug!(target: "siphon_smpp", session=%session_id,
                "submit_sm throttled → ESME_RTHROTTLED");
            metrics::record_pdu(metrics::INBOUND, "submit_sm", metrics::REJECTED);
            return request.reject(SmppError::ESME_RTHROTTLED);
        }
        let pdu = Pdu::from_submit(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu(&self.script, "submit_sm", pdu, session).await {
            Ok(reply) => {
                let resp = submit_sm_response(request, reply);
                let result = if resp.is_success() {
                    metrics::ACCEPTED
                } else {
                    metrics::REJECTED
                };
                metrics::record_pdu(metrics::INBOUND, "submit_sm", result);
                resp
            }
            Err(e) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(submit_sm) raised");
                metrics::record_pdu(metrics::INBOUND, "submit_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
        }
    }

    async fn on_data_sm(
        &self,
        request: data_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> data_sm_resp {
        if let InboundAdmit::Throttled = self.admit_inbound(session_id).await {
            tracing::debug!(target: "siphon_smpp", session=%session_id,
                "data_sm throttled → ESME_RTHROTTLED");
            metrics::record_pdu(metrics::INBOUND, "data_sm", metrics::REJECTED);
            return request.reject(SmppError::ESME_RTHROTTLED);
        }
        let pdu = Pdu::from_data(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "data_sm", pdu, session).await {
            // No handler → reject (data_sm is opt-in, like the smpp34 default).
            None => {
                metrics::record_pdu(metrics::INBOUND, "data_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
            Some(Ok(reply)) => {
                let resp = data_sm_response(request, reply);
                let result = if resp.is_success() {
                    metrics::ACCEPTED
                } else {
                    metrics::REJECTED
                };
                metrics::record_pdu(metrics::INBOUND, "data_sm", result);
                resp
            }
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(data_sm) raised");
                metrics::record_pdu(metrics::INBOUND, "data_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
        }
    }

    async fn on_cancel_sm(
        &self,
        request: cancel_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> cancel_sm_resp {
        let pdu = Pdu::from_cancel(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "cancel_sm", pdu, session).await {
            None => {
                metrics::record_pdu(metrics::INBOUND, "cancel_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RCANCELFAIL)
            }
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => {
                metrics::record_pdu(metrics::INBOUND, "cancel_sm", metrics::ACCEPTED);
                request.accept()
            }
            Some(Ok(reply)) => {
                metrics::record_pdu(metrics::INBOUND, "cancel_sm", metrics::REJECTED);
                request.reject(reply.command_status)
            }
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(cancel_sm) raised");
                metrics::record_pdu(metrics::INBOUND, "cancel_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RCANCELFAIL)
            }
        }
    }

    async fn on_query_sm(
        &self,
        request: query_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> query_sm_resp {
        let pdu = Pdu::from_query(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "query_sm", pdu, session).await {
            None => {
                metrics::record_pdu(metrics::INBOUND, "query_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RQUERYFAIL)
            }
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => {
                metrics::record_pdu(metrics::INBOUND, "query_sm", metrics::ACCEPTED);
                request.accept(
                    reply.message_id.unwrap_or_default(),
                    reply.final_date,
                    reply.message_state.unwrap_or(0),
                    reply.error_code,
                )
            }
            Some(Ok(reply)) => {
                metrics::record_pdu(metrics::INBOUND, "query_sm", metrics::REJECTED);
                request.reject(reply.command_status)
            }
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(query_sm) raised");
                metrics::record_pdu(metrics::INBOUND, "query_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RQUERYFAIL)
            }
        }
    }

    async fn on_replace_sm(
        &self,
        request: replace_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> replace_sm_resp {
        let pdu = Pdu::from_replace(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "replace_sm", pdu, session).await {
            None => {
                metrics::record_pdu(metrics::INBOUND, "replace_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RREPLACEFAIL)
            }
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => {
                metrics::record_pdu(metrics::INBOUND, "replace_sm", metrics::ACCEPTED);
                request.accept()
            }
            Some(Ok(reply)) => {
                metrics::record_pdu(metrics::INBOUND, "replace_sm", metrics::REJECTED);
                request.reject(reply.command_status)
            }
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(replace_sm) raised");
                metrics::record_pdu(metrics::INBOUND, "replace_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RREPLACEFAIL)
            }
        }
    }

    async fn on_submit_sm_multi(
        &self,
        request: submit_sm_multi,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> submit_sm_multi_resp {
        if let InboundAdmit::Throttled = self.admit_inbound(session_id).await {
            tracing::debug!(target: "siphon_smpp", session=%session_id,
                "submit_sm_multi throttled → ESME_RTHROTTLED");
            metrics::record_pdu(metrics::INBOUND, "submit_sm_multi", metrics::REJECTED);
            return request.reject(SmppError::ESME_RTHROTTLED);
        }
        let pdu = Pdu::from_submit_multi(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "submit_sm_multi", pdu, session).await {
            // Opt-in, like submit_sm's data-path siblings: no handler → reject.
            None => {
                metrics::record_pdu(metrics::INBOUND, "submit_sm_multi", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
            // accept(message_id, unsuccess_sme); we don't surface per-dest
            // unsuccess yet — an all-or-nothing accept covers the common case.
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => {
                metrics::record_pdu(metrics::INBOUND, "submit_sm_multi", metrics::ACCEPTED);
                request.accept(reply.message_id.unwrap_or_default(), Vec::new())
            }
            Some(Ok(reply)) => {
                metrics::record_pdu(metrics::INBOUND, "submit_sm_multi", metrics::REJECTED);
                request.reject(reply.command_status)
            }
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(submit_sm_multi) raised");
                metrics::record_pdu(metrics::INBOUND, "submit_sm_multi", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
        }
    }

    async fn on_timeout(&self, _seq: u32, session_id: &String) {
        let esme = {
            let binding = self.esmes.lock().await;
            binding
                .iter()
                .find(|e| e.esme.session_id == *session_id)
                .map(|e| e.esme.clone())
        };
        if let Some(esme) = esme {
            let _ = esme.send_unbind().await;
        }
    }

    async fn on_esme_bound(&self, esme: ESME, session_id: &String) {
        let session = Session {
            kind: SourceKind::EsmeServer,
            session_id: session_id.clone(),
            system_id: esme.system_id.clone(),
            client_addr: esme.client_address.to_string(),
        };
        // Per-session inbound rate limiter, sized from
        // `server.max_msg_per_sec` (the ingress mirror of a bind's
        // outbound throttle). `None` when unlimited.
        let throttle =
            (self.inbound_max_mps > 0).then(|| Arc::new(RateLimiter::new(self.inbound_max_mps)));
        self.esmes.lock().await.push(EsmeSession {
            esme: Arc::new(esme),
            throttle,
            response_timer: self.inbound_response_timer,
        });
        dispatch_session(&self.script, "bound", session).await;
    }

    async fn on_esme_unbound(&self, session_id: &String) {
        let removed = {
            let mut esmes = self.esmes.lock().await;
            let found = esmes
                .iter()
                .find(|e| e.esme.session_id == *session_id)
                .map(|e| (e.esme.system_id.clone(), e.esme.client_address.to_string()));
            esmes.retain(|e| e.esme.session_id != *session_id);
            found
        };
        let (system_id, client_addr) = removed.unwrap_or_default();
        let session = Session {
            kind: SourceKind::EsmeServer,
            session_id: session_id.clone(),
            system_id,
            client_addr,
        };
        dispatch_session(&self.script, "unbound", session).await;
    }
}

impl State {
    /// Build the `Session` passed to inbound `@smpp.on_pdu` handlers,
    /// resolving `system_id` from the bound ESME list (the per-PDU
    /// connection info doesn't carry it).
    async fn esme_session(&self, session_id: &str, conn: &SmppConnectionInformation) -> Session {
        let system_id = {
            let esmes = self.esmes.lock().await;
            esmes
                .iter()
                .find(|e| e.esme.session_id == session_id)
                .map(|e| e.esme.system_id.clone())
                .unwrap_or_default()
        };
        Session {
            kind: SourceKind::EsmeServer,
            session_id: session_id.to_string(),
            system_id,
            client_addr: conn.client_address.to_string(),
        }
    }

    /// Admit or throttle an inbound message PDU against its session's
    /// rate limiter. Clones the limiter out and **drops the `esmes` lock
    /// before awaiting** (throttling must not hold the lock and stall
    /// other sessions), then applies `server.throttle_action`:
    ///
    /// * `Pace` — block for a token, delaying the `*_resp` so the ESME's
    ///   outstanding-PDU window backpressures its submit rate down to the
    ///   cap, then admit.
    /// * `Reject` — admit if a token is free, else return [`Throttled`]
    ///   so the caller answers with `ESME_RTHROTTLED`.
    ///
    /// Always admits when the session is unlimited (no limiter).
    ///
    /// [`Throttled`]: InboundAdmit::Throttled
    async fn admit_inbound(&self, session_id: &str) -> InboundAdmit {
        let limiter = {
            let esmes = self.esmes.lock().await;
            esmes
                .iter()
                .find(|e| e.esme.session_id == session_id)
                .and_then(|e| e.throttle.clone())
        };
        let Some(limiter) = limiter else {
            return InboundAdmit::Proceed;
        };
        match self.inbound_throttle_action {
            ThrottleAction::Pace => {
                // A pacing wait is an inbound throttle event.
                if limiter.acquire().await {
                    metrics::record_throttled(metrics::INBOUND);
                }
                InboundAdmit::Proceed
            }
            ThrottleAction::Reject => {
                if limiter.try_acquire().await {
                    InboundAdmit::Proceed
                } else {
                    metrics::record_throttled(metrics::INBOUND);
                    InboundAdmit::Throttled
                }
            }
        }
    }
}

/// Outcome of [`State::admit_inbound`] — whether the PDU may be
/// dispatched or must be rejected with `ESME_RTHROTTLED`.
enum InboundAdmit {
    Proceed,
    Throttled,
}

// ── Client-side listener (one per bind) ────────────────────────────────

struct BindListener {
    state: Arc<State>,
    bind_name: String,
    max_msg_per_sec: u32,
    /// The bind's `response_timer_ms`, recorded on each session it brings up.
    response_timer: std::time::Duration,
    /// Raised by [`SmppClientListener::on_connection_failed`] when an
    /// attempt never reached a session. The supervisor clears it before
    /// each attempt and watches it during the bind wait, so a refused
    /// connect backs off immediately instead of sitting out the whole
    /// bind deadline waiting for an `is_alive()` that can never flip.
    connect_failed: Arc<AtomicBool>,
}

#[async_trait]
impl SmppClientListener for BindListener {
    // on_unbind uses the trait default (accept).

    /// The attempt never reached a session: TCP connect, TLS handshake or
    /// socket setup failed, so no bind was sent and `is_alive()` can never
    /// flip. Raise the flag the supervisor watches so it backs off now
    /// instead of sitting out the full bind deadline, and log the real
    /// cause — before smpp34 1.4.0 a refused connect panicked a tokio
    /// worker and surfaced here only as a misleading bind timeout.
    async fn on_connection_failed(&self, error: &str) {
        tracing::error!(target: "siphon_smpp",
            bind=%self.bind_name, error=%error,
            "outbound bind could not connect");
        metrics::record_bind_connect_failure(&self.bind_name);
        self.connect_failed.store(true, Ordering::Relaxed);
    }

    async fn on_deliver_sm(
        &self,
        request: deliver_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> deliver_sm_resp {
        // Dispatch EVERY deliver_sm — including delivery receipts
        // (esm_class & 0x04). The script inspects `pdu.is_dlr` /
        // `pdu.receipt` and routes the DLR back to the originating ESME.
        let pdu = Pdu::from_deliver(&request);
        let session = self.bind_session(session_id, conn);
        match dispatch_pdu(&self.state.script, "deliver_sm", pdu, session).await {
            Ok(reply) if reply.command_status == SmppError::ESME_ROK => {
                metrics::record_pdu(metrics::EGRESS, "deliver_sm", metrics::ACCEPTED);
                request.accept()
            }
            Ok(reply) => {
                metrics::record_pdu(metrics::EGRESS, "deliver_sm", metrics::REJECTED);
                request.reject(reply.command_status)
            }
            Err(e) => {
                tracing::error!(target: "siphon_smpp",
                    bind=%self.bind_name, error=%e,
                    "@smpp.on_pdu(deliver_sm) raised");
                metrics::record_pdu(metrics::EGRESS, "deliver_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
        }
    }

    async fn on_data_sm(
        &self,
        request: data_sm,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) -> data_sm_resp {
        let pdu = Pdu::from_data(&request);
        let session = self.bind_session(session_id, conn);
        match dispatch_pdu_opt(&self.state.script, "data_sm", pdu, session).await {
            None => {
                metrics::record_pdu(metrics::EGRESS, "data_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
            Some(Ok(reply)) => {
                let resp = data_sm_response(request, reply);
                let result = if resp.is_success() {
                    metrics::ACCEPTED
                } else {
                    metrics::REJECTED
                };
                metrics::record_pdu(metrics::EGRESS, "data_sm", result);
                resp
            }
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    bind=%self.bind_name, error=%e,
                    "@smpp.on_pdu(data_sm) raised");
                metrics::record_pdu(metrics::EGRESS, "data_sm", metrics::REJECTED);
                request.reject(SmppError::ESME_RSYSERR)
            }
        }
    }

    async fn on_alert_notification(
        &self,
        request: alert_notification,
        conn: &SmppConnectionInformation,
        session_id: &String,
    ) {
        // Notification only (no wire response): dispatch so the script can
        // react, e.g. flush queued MT for the now-available MS.
        let alert = AlertNotification::from_alert(&request);
        let session = self.bind_session(session_id, conn);
        match dispatch_alert(&self.state.script, alert, session).await {
            Ok(()) => metrics::record_pdu(metrics::EGRESS, "alert_notification", metrics::ACCEPTED),
            Err(e) => {
                tracing::error!(target: "siphon_smpp",
                    bind=%self.bind_name, error=%e,
                    "@smpp.on_pdu(alert_notification) raised");
                // alert_notification bypasses the central dispatch timer, so
                // count the raised handler here.
                metrics::record_dispatch_error("alert_notification");
                metrics::record_pdu(metrics::EGRESS, "alert_notification", metrics::REJECTED);
            }
        }
    }

    async fn on_timeout(&self, _seq: u32, session_id: &String) {
        let smsc = {
            let binding = self.state.binds.lock().await;
            binding
                .iter()
                .find(|t| t.smsc.session_id == *session_id)
                .map(|t| t.smsc.clone())
        };
        if let Some(smsc) = smsc {
            let _ = smsc.send_unbind().await;
        }
    }

    async fn on_smsc_bound(&self, smsc: SMSC, session_id: &String) {
        tracing::info!(target: "siphon_smpp",
            bind=%self.bind_name, system_id=%smsc.system_id,
            "outbound bind up");
        let throttle =
            (self.max_msg_per_sec > 0).then(|| Arc::new(RateLimiter::new(self.max_msg_per_sec)));
        let session = Session {
            kind: SourceKind::Bind,
            session_id: session_id.clone(),
            system_id: self.bind_name.clone(),
            client_addr: smsc.server_address.to_string(),
        };
        self.state.binds.lock().await.push(BindSession {
            name: self.bind_name.clone(),
            smsc: Arc::new(smsc),
            throttle,
            response_timer: self.response_timer,
        });
        dispatch_session(&self.state.script, "bound", session).await;
    }

    async fn on_smsc_unbound(&self, session_id: &String) {
        let removed = {
            let mut binding = self.state.binds.lock().await;
            let before = binding.len();
            binding.retain(|t| t.smsc.session_id != *session_id);
            before > binding.len()
        };
        if removed {
            tracing::warn!(target: "siphon_smpp",
                bind=%self.bind_name, "bind unbound");
            let session = Session {
                kind: SourceKind::Bind,
                session_id: session_id.clone(),
                system_id: self.bind_name.clone(),
                client_addr: String::new(),
            };
            dispatch_session(&self.state.script, "unbound", session).await;
        }
    }
}

impl BindListener {
    fn bind_session(&self, session_id: &str, conn: &SmppConnectionInformation) -> Session {
        Session {
            kind: SourceKind::Bind,
            session_id: session_id.to_string(),
            system_id: self.bind_name.clone(),
            client_addr: conn.server_address.to_string(),
        }
    }
}

// ── Dispatch helpers ────────────────────────────────────────────────────

/// Attach a handler's `reply(tlvs=…)` optional parameters to a
/// `data_sm_resp`. `data_sm_resp` is the only 3.4 response PDU that carries
/// any (§4.2.3), which is why nothing else goes through here.
/// `command_length` is recomputed at encode time, so order doesn't matter.
fn with_resp_tlvs(mut resp: data_sm_resp, tlvs: Vec<smpp34::Tlv>) -> data_sm_resp {
    for tlv in tlvs {
        resp.push_tlv(tlv);
    }
    resp
}

/// Turn a handler's reply to a `submit_sm` into the response we send.
///
/// The reply's `command_status` decides, and nothing else does: a
/// `message_id` handed back alongside a reject status does not turn the
/// rejection into an acceptance. An acceptance always carries a
/// `message_id` body — empty when the handler gave none — because the
/// field is mandatory in an `ESME_ROK` `submit_sm_resp` (§4.4.2) and a
/// bare header is a malformed PDU the ESME cannot decode.
fn submit_sm_response(request: submit_sm, reply: PduReply) -> submit_sm_resp {
    if reply.command_status == SmppError::ESME_ROK {
        request.accept(reply.message_id.unwrap_or_default())
    } else {
        request.reject(reply.command_status)
    }
}

/// Turn a handler's reply to a `data_sm` into the response we send, on
/// either kind of session. As with [`submit_sm_response`] the reply's
/// `command_status` alone decides. `reply(tlvs=…)` rides along either
/// way: the optional parameters of a `data_sm_resp` (§4.7.2) are mostly
/// there to explain a failure.
fn data_sm_response(request: data_sm, reply: PduReply) -> data_sm_resp {
    let resp = if reply.command_status == SmppError::ESME_ROK {
        request.accept(reply.message_id.unwrap_or_default())
    } else {
        request.reject(reply.command_status)
    };
    with_resp_tlvs(resp, reply.tlvs)
}

/// Dispatch a PDU to its `@smpp.on_pdu("<command>")` handler. If no
/// handler matches, default to a soft `ESME_ROK` accept so the wire ack
/// still fires (used for the always-acked paths: submit_sm, deliver_sm).
async fn dispatch_pdu(
    script: &ScriptHandle,
    command: &str,
    pdu: Pdu,
    session: Session,
) -> PyResult<PduReply> {
    match dispatch_pdu_opt(script, command, pdu, session).await {
        Some(r) => r,
        None => Ok(PduReply::default_ok()),
    }
}

/// Like [`dispatch_pdu`] but returns `None` when no handler is
/// registered, so the caller can choose the no-handler default (used for
/// the opt-in paths: data_sm, cancel_sm reject by default).
async fn dispatch_pdu_opt(
    script: &ScriptHandle,
    command: &str,
    pdu: Pdu,
    session: Session,
) -> Option<PyResult<PduReply>> {
    let handler = match pick_pdu_handler(script, command) {
        Ok(Some(h)) => h,
        Ok(None) => return None,
        Err(e) => return Some(Err(e)),
    };
    // Time the handler invocation and count a raised handler once,
    // centrally for every PDU command that dispatches through here
    // (submit/data/cancel/query/replace/submit_multi/deliver). The clock is
    // read only when metrics are enabled — with metrics off the dispatch
    // path adds nothing but a couple of OnceLock loads.
    let started = metrics::enabled().then(Instant::now);
    let result = call_pdu_handler(script, handler, pdu, session).await;
    if let Some(started) = started {
        metrics::observe_dispatch(command, started.elapsed().as_secs_f64());
    }
    if result.is_err() {
        metrics::record_dispatch_error(command);
    }
    Some(result)
}

async fn call_pdu_handler(
    script: &ScriptHandle,
    handler: siphon::script::HandlerHandle,
    pdu: Pdu,
    session: Session,
) -> PyResult<PduReply> {
    let py_args = Python::attach(|py| -> PyResult<Vec<Py<PyAny>>> {
        let pdu_py = Py::new(py, pdu)?.into_any();
        let sess_py = Py::new(py, session)?.into_any();
        Ok(vec![pdu_py, sess_py])
    })?;

    let result = script.call_handler(&handler, py_args).await?;

    Python::attach(|py| -> PyResult<PduReply> {
        let bound = result.bind(py);
        if bound.is_none() {
            return Ok(PduReply::default_ok());
        }
        bound.extract::<PduReply>().or_else(|_| {
            tracing::warn!(target: "siphon_smpp",
                "smpp.on_pdu handler returned a non-PduReply value; defaulting to ESME_ROK");
            Ok(PduReply::default_ok())
        })
    })
}

/// Dispatch an `alert_notification` to `@smpp.on_pdu("alert_notification")`.
/// Notification only — the handler's return value is ignored.
async fn dispatch_alert(
    script: &ScriptHandle,
    alert: AlertNotification,
    session: Session,
) -> PyResult<()> {
    let handler = match pick_pdu_handler(script, "alert_notification")? {
        Some(h) => h,
        None => return Ok(()),
    };
    let py_args = Python::attach(|py| -> PyResult<Vec<Py<PyAny>>> {
        let alert_py = Py::new(py, alert)?.into_any();
        let sess_py = Py::new(py, session)?.into_any();
        Ok(vec![alert_py, sess_py])
    })?;
    let _ = script.call_handler(&handler, py_args).await?;
    Ok(())
}

/// Dispatch a session lifecycle event to every matching
/// `@smpp.on_session("<event>")` handler. Best-effort: handler errors are
/// logged, never propagated (lifecycle hooks must not break the runtime).
async fn dispatch_session(script: &ScriptHandle, event: &str, session: Session) {
    let handlers = pick_session_handlers(script, event);
    for handler in handlers {
        let py_args = Python::attach(|py| -> PyResult<Vec<Py<PyAny>>> {
            Ok(vec![Py::new(py, session.clone())?.into_any()])
        });
        let py_args = match py_args {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(target: "siphon_smpp",
                    event=%event, error=%e, "building on_session args failed");
                continue;
            }
        };
        if let Err(e) = script.call_handler(&handler, py_args).await {
            tracing::error!(target: "siphon_smpp",
                event=%event, error=%e, "@smpp.on_session raised");
        }
    }
}

/// Find the first registered `@smpp.on_pdu` handler whose
/// `options.command` matches. The decorator stores the command name in
/// the handler options (the kind filter is shared `"smpp.on_pdu"`).
fn pick_pdu_handler(
    script: &ScriptHandle,
    command: &str,
) -> PyResult<Option<siphon::script::HandlerHandle>> {
    let handlers = script.handlers_for("smpp.on_pdu");
    Python::attach(|py| -> PyResult<Option<siphon::script::HandlerHandle>> {
        for h in handlers {
            if handler_option_eq(&h, py, "command", command) {
                return Ok(Some(h));
            }
        }
        Ok(None)
    })
}

/// All `@smpp.on_session` handlers whose `options.event` matches.
fn pick_session_handlers(script: &ScriptHandle, event: &str) -> Vec<siphon::script::HandlerHandle> {
    let handlers = script.handlers_for("smpp.on_session");
    Python::attach(|py| {
        handlers
            .into_iter()
            .filter(|h| handler_option_eq(h, py, "event", event))
            .collect()
    })
}

/// True when the handler's `options[key]` equals `want`.
fn handler_option_eq(
    handler: &siphon::script::HandlerHandle,
    py: Python<'_>,
    key: &str,
    want: &str,
) -> bool {
    match handler.options(py) {
        Some(d) => match d.get_item(key) {
            Ok(Some(v)) => v.extract::<String>().ok().as_deref() == Some(want),
            _ => false,
        },
        None => false,
    }
}

/// Outcome of an `@smpp.on_bind` dispatch.
struct BindOutcome {
    accept: bool,
    status: SmppError,
    reason: String,
}

/// Look up a `@smpp.on_bind` handler and call it. The handler returns
/// `bind.accept()` / `bind.reject(status, reason)` (a `BindResult`), or a
/// bare truthy/falsy value. No handler, no return, or a raised exception
/// → reject (closed by default; the script is the authority on
/// credentials).
async fn dispatch_bind(
    script: &ScriptHandle,
    system_id: &str,
    password: &str,
    client_addr: &str,
) -> BindOutcome {
    fn reject(reason: &str) -> BindOutcome {
        BindOutcome {
            accept: false,
            status: SmppError::ESME_RBINDFAIL,
            reason: reason.to_string(),
        }
    }

    let handlers = script.handlers_for("smpp.on_bind");
    let handler = match handlers.into_iter().next() {
        Some(h) => h,
        None => return reject("no @smpp.on_bind handler registered"),
    };

    let bind = Bind {
        system_id: system_id.to_string(),
        password: password.to_string(),
        client_addr: client_addr.to_string(),
    };

    let py_args = match Python::attach(|py| -> PyResult<Vec<Py<PyAny>>> {
        Ok(vec![Py::new(py, bind)?.into_any()])
    }) {
        Ok(a) => a,
        Err(e) => return reject(&format!("building bind args failed: {e}")),
    };

    let result = match script.call_handler(&handler, py_args).await {
        Ok(r) => r,
        Err(e) => return reject(&format!("@smpp.on_bind raised: {e}")),
    };

    Python::attach(|py| {
        let bound = result.bind(py);
        if bound.is_none() {
            // No explicit return ≡ rejection; forces explicit
            // accept/reject in scripts, no accidental open binds.
            return reject("handler returned None");
        }
        if let Ok(br) = bound.extract::<BindResult>() {
            return BindOutcome {
                accept: br.accept,
                status: br.status,
                reason: br.reason,
            };
        }
        // Back-compat: a bare truthy/falsy return.
        match bound.is_truthy() {
            Ok(true) => BindOutcome {
                accept: true,
                status: SmppError::ESME_ROK,
                reason: String::new(),
            },
            _ => reject("handler returned a non-truthy value"),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_data_sm() -> data_sm {
        data_sm::new(
            42,
            String::new(),
            1,
            1,
            "5550100".into(),
            1,
            1,
            "5550199".into(),
            0,
            0,
            0,
        )
    }

    #[test]
    fn reply_tlvs_reach_the_data_sm_resp_wire_image() {
        // A rejection reason that only reaches our own log is worse than
        // useless, so assert the TLV is really in the encoded response —
        // command_length included, or the peer truncates it away.
        let resp = with_resp_tlvs(
            a_data_sm().reject(SmppError::ESME_RSUBMITFAIL),
            vec![smpp34::Tlv::from_u8(
                smpp34::TlvTag::DeliveryFailureReason,
                1,
            )],
        );
        let wire = resp.encode();

        // §3.2.1: tag 0x0425, length 0x0001, value 0x01.
        let expected_tlv = [0x04u8, 0x25, 0x00, 0x01, 0x01];
        assert!(
            wire.windows(expected_tlv.len()).any(|w| w == expected_tlv),
            "delivery_failure_reason TLV missing from {wire:02x?}"
        );
        // command_length must cover it, otherwise the peer stops reading
        // before the optional parameters.
        let command_length = u32::from_be_bytes([wire[0], wire[1], wire[2], wire[3]]);
        assert_eq!(command_length as usize, wire.len());
    }

    // ── The status a handler chose is the status on the wire ────────

    fn a_submit_sm() -> submit_sm {
        submit_sm::new(
            7,
            String::new(),
            1,
            1,
            "5550100".into(),
            1,
            1,
            "5550199".into(),
            0,
            0,
            0,
            String::new(),
            String::new(),
            0,
            0,
            0,
            0,
            b"hello".to_vec(),
        )
    }

    fn reply(command_status: SmppError, message_id: Option<&str>) -> PduReply {
        PduReply {
            command_status,
            message_id: message_id.map(str::to_string),
            ..PduReply::default_ok()
        }
    }

    /// A `submit_sm_resp` for sequence number 7, written out from §4.4.2.
    fn submit_sm_resp_wire(command_status: u32, body: &[u8]) -> Vec<u8> {
        let mut wire = Vec::new();
        wire.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
        wire.extend_from_slice(&0x8000_0004u32.to_be_bytes());
        wire.extend_from_slice(&command_status.to_be_bytes());
        wire.extend_from_slice(&7u32.to_be_bytes());
        wire.extend_from_slice(body);
        wire
    }

    #[test]
    fn an_accepted_submit_sm_is_answered_rok_with_the_message_id() {
        let wire =
            submit_sm_response(a_submit_sm(), reply(SmppError::ESME_ROK, Some("abc"))).encode();
        assert_eq!(wire, submit_sm_resp_wire(0, b"abc\0"));
    }

    #[test]
    fn a_rejected_submit_sm_is_answered_with_the_status_the_handler_chose() {
        let wire =
            submit_sm_response(a_submit_sm(), reply(SmppError::ESME_RINVDSTADR, None)).encode();
        // §4.4.2: no body with a non-zero status.
        assert_eq!(wire, submit_sm_resp_wire(0x0000_000B, &[]));
    }

    #[test]
    fn a_reject_status_is_not_overridden_by_a_message_id() {
        // A handler that allocates its id up front and then decides to
        // throttle hands back both. The status is the verdict; answering
        // ESME_ROK here would tell the ESME its message was taken.
        let wire = submit_sm_response(
            a_submit_sm(),
            reply(SmppError::ESME_RTHROTTLED, Some("abc")),
        )
        .encode();
        assert_eq!(wire, submit_sm_resp_wire(0x0000_0058, &[]));
    }

    #[test]
    fn an_accept_without_a_message_id_still_has_its_mandatory_body() {
        // `pdu.reply()`, a handler returning None and the no-handler
        // default all land here. message_id is mandatory in an ESME_ROK
        // submit_sm_resp (§4.4.2), so it goes out as an empty C-Octet
        // String, not as a header with nothing behind it.
        let wire = submit_sm_response(a_submit_sm(), PduReply::default_ok()).encode();
        assert_eq!(wire, submit_sm_resp_wire(0, &[0x00]));
    }

    /// A `data_sm_resp` for sequence number 42, written out from §4.7.2.
    fn data_sm_resp_wire(command_status: u32, body: &[u8]) -> Vec<u8> {
        let mut wire = Vec::new();
        wire.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
        wire.extend_from_slice(&0x8000_0103u32.to_be_bytes());
        wire.extend_from_slice(&command_status.to_be_bytes());
        wire.extend_from_slice(&42u32.to_be_bytes());
        wire.extend_from_slice(body);
        wire
    }

    #[test]
    fn an_accepted_data_sm_is_answered_rok_with_the_message_id() {
        let wire = data_sm_response(a_data_sm(), reply(SmppError::ESME_ROK, Some("abc"))).encode();
        assert_eq!(wire, data_sm_resp_wire(0, b"abc\0"));
    }

    #[test]
    fn a_data_sm_reject_status_is_not_overridden_by_a_message_id() {
        let wire =
            data_sm_response(a_data_sm(), reply(SmppError::ESME_RX_T_APPN, Some("abc"))).encode();
        assert_eq!(&wire[4..12], &data_sm_resp_wire(0x0000_0064, &[])[4..12]);
        assert!(
            !wire.windows(3).any(|w| w == b"abc"),
            "a rejected data_sm must not carry the id of an accepted one: {wire:02x?}"
        );
    }

    #[test]
    fn a_rejected_data_sm_keeps_the_optional_parameters_that_explain_it() {
        let mut rejection = reply(SmppError::ESME_RDELIVERYFAILURE, None);
        rejection.tlvs = vec![smpp34::Tlv::from_u8(
            smpp34::TlvTag::DeliveryFailureReason,
            1,
        )];
        let wire = data_sm_response(a_data_sm(), rejection).encode();
        assert_eq!(&wire[8..12], &0x0000_00FEu32.to_be_bytes());
        // §5.3.2.33: tag 0x0425, length 1, value 1.
        let expected_tlv = [0x04u8, 0x25, 0x00, 0x01, 0x01];
        assert!(wire.windows(5).any(|w| w == expected_tlv), "{wire:02x?}");
        assert_eq!(
            u32::from_be_bytes([wire[0], wire[1], wire[2], wire[3]]) as usize,
            wire.len()
        );
    }

    #[test]
    fn no_reply_tlvs_leaves_the_response_untouched() {
        let bare = a_data_sm().accept("abc".into()).encode();
        let through = with_resp_tlvs(a_data_sm().accept("abc".into()), Vec::new()).encode();
        assert_eq!(bare, through);
    }

    // ── Bind supervisor: phase-1 wait ───────────────────────────────

    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(15);

    #[test]
    fn awaiting_bind_waits_while_the_attempt_is_still_open() {
        assert!(awaiting_bind(false, false, Duration::ZERO, DEADLINE));
        assert!(awaiting_bind(
            false,
            false,
            Duration::from_secs(14),
            DEADLINE
        ));
    }

    #[test]
    fn awaiting_bind_stops_once_the_session_is_up() {
        assert!(!awaiting_bind(true, false, Duration::ZERO, DEADLINE));
    }

    #[test]
    fn awaiting_bind_stops_immediately_on_a_failed_connect() {
        // The point of the smpp34 1.4.0 hook: a refused connect starts no
        // session, so is_alive() can never flip. Without this term every
        // retry against a downed peer burns the whole deadline first.
        assert!(!awaiting_bind(false, true, Duration::ZERO, DEADLINE));
        assert!(!awaiting_bind(
            false,
            true,
            Duration::from_millis(1),
            DEADLINE
        ));
    }

    #[test]
    fn awaiting_bind_stops_at_the_deadline() {
        assert!(!awaiting_bind(false, false, DEADLINE, DEADLINE));
        assert!(!awaiting_bind(
            false,
            false,
            Duration::from_secs(16),
            DEADLINE
        ));
    }

    #[test]
    fn awaiting_bind_never_outlives_a_live_session_or_a_failure() {
        // Whatever the clock says, alive or failed both end the wait.
        for waited in [Duration::ZERO, Duration::from_secs(3), DEADLINE] {
            assert!(!awaiting_bind(true, false, waited, DEADLINE));
            assert!(!awaiting_bind(false, true, waited, DEADLINE));
            assert!(!awaiting_bind(true, true, waited, DEADLINE));
        }
    }

    #[tokio::test]
    async fn rate_limiter_paces_to_configured_rate() {
        // A 100/s limiter starts full (burst of 100), so the first 100
        // acquires are instant; the 101st must wait ~1 refill interval.
        let rl = RateLimiter::new(100);
        let start = Instant::now();
        for _ in 0..100 {
            rl.acquire().await;
        }
        // Burst drained quickly (well under one refill window).
        assert!(start.elapsed() < std::time::Duration::from_millis(50));

        // The next token has to be refilled (~10ms at 100/s).
        let before = Instant::now();
        rl.acquire().await;
        assert!(before.elapsed() >= std::time::Duration::from_millis(5));
    }

    #[test]
    fn rate_limiter_new_clamps_zero_to_one() {
        // new(0) must not divide-by-zero; it clamps to 1/s.
        let _ = RateLimiter::new(0);
    }

    #[tokio::test]
    async fn try_acquire_drains_burst_then_refuses() {
        // A 5/s limiter starts full: the first 5 non-blocking acquires
        // succeed, the 6th fails immediately (the `reject` action's gate).
        let rl = RateLimiter::new(5);
        for _ in 0..5 {
            assert!(rl.try_acquire().await, "burst token should be available");
        }
        assert!(
            !rl.try_acquire().await,
            "empty bucket must refuse without waiting"
        );
    }
}
