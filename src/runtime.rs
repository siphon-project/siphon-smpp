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

    /// Block until a token is available, then consume it.
    pub(crate) async fn acquire(&self) {
        loop {
            let wait = {
                let mut b = self.inner.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.tokens = (b.tokens + elapsed * b.refill_per_sec).min(b.max);
                b.last = now;
                if b.tokens >= 1.0 {
                    b.tokens -= 1.0;
                    return;
                }
                let deficit = 1.0 - b.tokens;
                std::time::Duration::from_secs_f64(deficit / b.refill_per_sec)
            };
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
        script: script.clone(),
    });
    if STATE.set(state.clone()).is_err() {
        tracing::warn!(target: "siphon_smpp",
            "runtime::spawn called twice; ignoring second invocation");
        return;
    }
    let handle = script.tokio_handle().clone();

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
        tracing::info!(target: "siphon_smpp",
            host=%log_host, port=port, "SMPP server listening");
        server.start().await;
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
    let listener: Arc<dyn SmppClientListener + Send + Sync> = Arc::new(BindListener {
        state: state.clone(),
        bind_name: cfg.name.clone(),
        max_msg_per_sec: cfg.max_msg_per_sec,
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

        // Phase 1: wait for bind to complete (or fail).
        let bind_started = Instant::now();
        while !client.is_alive() && bind_started.elapsed() < bind_deadline {
            tokio::time::sleep(poll_interval).await;
        }

        // Phase 2: if bound, hold the session until is_alive() flips false.
        let bound_at = if client.is_alive() {
            let now = Instant::now();
            while client.is_alive() {
                tokio::time::sleep(poll_interval).await;
            }
            Some(now)
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
    async fn on_bind_transmitter(
        &self,
        request: bind_transmitter,
        _conn: &SmppConnectionInformation,
        _session: &String,
    ) -> bind_transmitter_resp {
        // TX-only binds intentionally rejected — siphon-smpp only
        // supports transceiver binds (mirrors the reference policy).
        request.reject(SmppError::ESME_RINVSYSID)
    }

    async fn on_bind_receiver(
        &self,
        request: bind_receiver,
        _conn: &SmppConnectionInformation,
        _session: &String,
    ) -> bind_receiver_resp {
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
            return request.reject(outcome.status);
        }
        tracing::info!(target: "siphon_smpp",
            from=%conn.client_address, system_id=%request.system_id,
            "bind_transceiver accepted");
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
            return request.reject(SmppError::ESME_RTHROTTLED);
        }
        let pdu = Pdu::from_submit(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu(&self.script, "submit_sm", pdu, session).await {
            Ok(reply) => match reply.message_id {
                Some(id) => request.accept(id),
                None => request.reject(reply.command_status),
            },
            Err(e) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(submit_sm) raised");
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
            return request.reject(SmppError::ESME_RTHROTTLED);
        }
        let pdu = Pdu::from_data(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "data_sm", pdu, session).await {
            // No handler → reject (data_sm is opt-in, like the smpp34 default).
            None => request.reject(SmppError::ESME_RSYSERR),
            Some(Ok(reply)) => match reply.message_id {
                Some(id) => request.accept(id),
                None if reply.command_status == SmppError::ESME_ROK => {
                    request.accept(String::new())
                }
                None => request.reject(reply.command_status),
            },
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(data_sm) raised");
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
            None => request.reject(SmppError::ESME_RCANCELFAIL),
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => request.accept(),
            Some(Ok(reply)) => request.reject(reply.command_status),
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(cancel_sm) raised");
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
            None => request.reject(SmppError::ESME_RQUERYFAIL),
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => request.accept(
                reply.message_id.unwrap_or_default(),
                reply.final_date,
                reply.message_state.unwrap_or(0),
                reply.error_code,
            ),
            Some(Ok(reply)) => request.reject(reply.command_status),
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(query_sm) raised");
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
            None => request.reject(SmppError::ESME_RREPLACEFAIL),
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => request.accept(),
            Some(Ok(reply)) => request.reject(reply.command_status),
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(replace_sm) raised");
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
            return request.reject(SmppError::ESME_RTHROTTLED);
        }
        let pdu = Pdu::from_submit_multi(&request);
        let session = self.esme_session(session_id, conn).await;
        match dispatch_pdu_opt(&self.script, "submit_sm_multi", pdu, session).await {
            // Opt-in, like submit_sm's data-path siblings: no handler → reject.
            None => request.reject(SmppError::ESME_RSYSERR),
            // accept(message_id, unsuccess_sme); we don't surface per-dest
            // unsuccess yet — an all-or-nothing accept covers the common case.
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => {
                request.accept(reply.message_id.unwrap_or_default(), Vec::new())
            }
            Some(Ok(reply)) => request.reject(reply.command_status),
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    error=%e, "@smpp.on_pdu(submit_sm_multi) raised");
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
                limiter.acquire().await;
                InboundAdmit::Proceed
            }
            ThrottleAction::Reject => {
                if limiter.try_acquire().await {
                    InboundAdmit::Proceed
                } else {
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
}

#[async_trait]
impl SmppClientListener for BindListener {
    // on_unbind uses the trait default (accept).

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
            Ok(reply) if reply.command_status == SmppError::ESME_ROK => request.accept(),
            Ok(reply) => request.reject(reply.command_status),
            Err(e) => {
                tracing::error!(target: "siphon_smpp",
                    bind=%self.bind_name, error=%e,
                    "@smpp.on_pdu(deliver_sm) raised");
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
            None => request.reject(SmppError::ESME_RSYSERR),
            Some(Ok(reply)) if reply.command_status == SmppError::ESME_ROK => {
                request.accept(reply.message_id.unwrap_or_default())
            }
            Some(Ok(reply)) => request.reject(reply.command_status),
            Some(Err(e)) => {
                tracing::error!(target: "siphon_smpp",
                    bind=%self.bind_name, error=%e,
                    "@smpp.on_pdu(data_sm) raised");
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
        if let Err(e) = dispatch_alert(&self.state.script, alert, session).await {
            tracing::error!(target: "siphon_smpp",
                bind=%self.bind_name, error=%e,
                "@smpp.on_pdu(alert_notification) raised");
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
    Some(call_pdu_handler(script, handler, pdu, session).await)
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
