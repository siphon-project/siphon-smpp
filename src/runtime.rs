//! Tokio-side SMPP runtime.
//!
//! Drives both directions:
//!
//! * **Client / binds** — for each `cfg.binds` entry, an `SmppClient`
//!   with reconnect-with-backoff. `SmppClientListener::on_deliver_sm`
//!   dispatches inbound MT (from the aggregator) into the script's
//!   `@smpp.on_pdu("deliver_sm")` handlers. Bound `SMSC` handles are
//!   tracked in [`State`] so `smpp.submit_via(bind=…)` can reach the
//!   right session.
//! * **Server** — `SmppServer` accepts inbound binds on the configured
//!   listen address. `bind_transmitter` and `bind_receiver` are
//!   rejected (mirrors the reference policy); `bind_transceiver` is
//!   handed to `@smpp.on_bind` for accept/reject. `on_submit_sm`
//!   dispatches into `@smpp.on_pdu("submit_sm")`.
//!
//! Shared state — both listeners read the script handler table from
//! the [`siphon::script::ScriptHandle`] every dispatch (via
//! `handlers_for("smpp.on_pdu")`), so a hot-reloaded script is picked
//! up on the next PDU.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use pyo3::prelude::*;
use siphon::script::ScriptHandle;
use smpp34::{
    bind_receiver, bind_receiver_resp, bind_transceiver, bind_transceiver_resp, bind_transmitter,
    bind_transmitter_resp,
    client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC},
    deliver_sm, deliver_sm_resp,
    server::ESME,
    submit_sm, submit_sm_resp, SmppConnectionInformation, SmppError, SmppServer,
    SmppServerListener,
};
use tokio::sync::Mutex;

use crate::pyclasses::{Bind, Pdu, PduReply, Session, SourceKind};
use crate::{config::BindConfig, SmppConfig};

// ── Shared state ────────────────────────────────────────────────────────

pub(crate) struct State {
    pub binds: Mutex<Vec<BindSession>>,
    pub esmes: Mutex<Vec<ESME>>,
    pub script: ScriptHandle,
}

pub(crate) struct BindSession {
    pub name: String,
    /// `Arc` so `submit_via` can clone the handle out of the bind
    /// list, drop the lock, and await the response without blocking
    /// other binds behind a single mutex.
    pub smsc: Arc<SMSC>,
}

pub(crate) static STATE: OnceLock<Arc<State>> = OnceLock::new();

/// Public accessor used by the namespace pyclass's `submit_via`.
pub(crate) fn state() -> Option<Arc<State>> {
    STATE.get().cloned()
}

// ── Spawn ───────────────────────────────────────────────────────────────

pub fn spawn(cfg: SmppConfig, script: ScriptHandle) {
    let state = Arc::new(State {
        binds: Mutex::new(Vec::new()),
        esmes: Mutex::new(Vec::new()),
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
/// (smpp34/src/client/mod.rs:367). So we drive the lifecycle ourselves:
///
///   1. start() — spawns the I/O task
///   2. wait for `is_alive()` to flip true (the spawned task sets this
///      after a successful bind response) or for the bind deadline to
///      expire
///   3. once alive, poll `is_alive()` until it flips false (peer closed,
///      enquire_link / response timeout fired, etc.)
///   4. log "bind down", apply backoff, loop
///
/// Treating start() as blocking — which the original code did — caused
/// every iteration to immediately log "bind down" and then race ahead
/// to spawn a fresh `SmppClient` while the previous one's spawned tasks
/// were still alive. The user-visible symptom was multiple concurrent
/// sessions to the same aggregator hostname, each on a different
/// NLB-resolved IP, with the supervisor cycling on its own backoff
/// schedule rather than tracking actual session liveness.
async fn run_bind_loop(state: Arc<State>, cfg: BindConfig) {
    let bind_type = match cfg.bind_type.as_str() {
        "transmitter" => BIND_TYPE::TX,
        "receiver" => BIND_TYPE::RX,
        _ => BIND_TYPE::TRX,
    };
    let listener: Arc<dyn SmppClientListener + Send + Sync> = Arc::new(BindListener {
        state: state.clone(),
        bind_name: cfg.name.clone(),
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
        let bind_started = std::time::Instant::now();
        while !client.is_alive() && bind_started.elapsed() < bind_deadline {
            tokio::time::sleep(poll_interval).await;
        }

        // Phase 2: if bound, hold the session until is_alive() flips false.
        let bound_at = if client.is_alive() {
            let now = std::time::Instant::now();
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
        let approved = match dispatch_bind(
            &self.script,
            &request.system_id,
            &request.password,
            &conn.client_address.to_string(),
        )
        .await
        {
            Ok(ok) => ok,
            Err(e) => {
                tracing::warn!(target: "siphon_smpp",
                    error=%e, system_id=%request.system_id,
                    "@smpp.on_bind raised; rejecting");
                false
            }
        };
        if !approved {
            tracing::info!(target: "siphon_smpp",
                from=%conn.client_address, system_id=%request.system_id,
                "bind_transceiver rejected by script");
            return request.reject(SmppError::ESME_RBINDFAIL);
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
        let pdu = Pdu::from_submit(&request);
        let session = Session {
            kind: SourceKind::EsmeServer,
            session_id: session_id.clone(),
            system_id: String::new(), // populated post-bind in self.esmes
            client_addr: conn.client_address.to_string(),
        };
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

    // on_cancel_sm (reject ESME_RCANCELFAIL) and on_data_sm (reject
    // ESME_RSYSERR) use the trait defaults.

    async fn on_timeout(&self, _seq: u32, session_id: &String) {
        let binding = self.esmes.lock().await;
        if let Some(esme) = binding.iter().find(|e| e.session_id == *session_id) {
            let _ = esme.send_unbind().await;
        }
    }

    async fn on_esme_bound(&self, esme: ESME, _session: &String) {
        self.esmes.lock().await.push(esme);
    }

    async fn on_esme_unbound(&self, session_id: &String) {
        self.esmes
            .lock()
            .await
            .retain(|e| e.session_id != *session_id);
    }
}

// ── Client-side listener (one per bind) ────────────────────────────────

struct BindListener {
    state: Arc<State>,
    bind_name: String,
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
        // Delivery receipt — protocol-level ack only; the script may
        // observe via metrics / log lines but doesn't decide outcome.
        if request.esm_class & 0x04 != 0 {
            return request.accept();
        }

        let pdu = Pdu::from_deliver(&request);
        let session = Session {
            kind: SourceKind::Bind,
            session_id: session_id.clone(),
            system_id: self.bind_name.clone(),
            client_addr: conn.server_address.to_string(),
        };
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

    // on_data_sm (reject ESME_RSYSERR) and on_alert_notification (no-op) use
    // the trait defaults.

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

    async fn on_smsc_bound(&self, smsc: SMSC, _session: &String) {
        tracing::info!(target: "siphon_smpp",
            bind=%self.bind_name, system_id=%smsc.system_id,
            "outbound bind up");
        self.state.binds.lock().await.push(BindSession {
            name: self.bind_name.clone(),
            smsc: Arc::new(smsc),
        });
    }

    async fn on_smsc_unbound(&self, session_id: &String) {
        let mut binding = self.state.binds.lock().await;
        let before = binding.len();
        binding.retain(|t| t.smsc.session_id != *session_id);
        if before > binding.len() {
            tracing::warn!(target: "siphon_smpp",
                bind=%self.bind_name, "bind unbound");
        }
    }
}

// ── Dispatch helpers ────────────────────────────────────────────────────

/// Look up matching `@smpp.on_pdu("<command>")` handlers in the script
/// registry, build a `Pdu` + `Session` pyobject, call the first
/// matching handler, parse its return into a [`PduReply`]. If no
/// handler matches, default to `ESME_ROK` accept (server side) /
/// successful ack (bind side) so the wire ack still fires.
async fn dispatch_pdu(
    script: &ScriptHandle,
    command: &str,
    pdu: Pdu,
    session: Session,
) -> PyResult<PduReply> {
    let handler = pick_pdu_handler(script, command)?;
    let handler = match handler {
        Some(h) => h,
        None => return Ok(PduReply::default_ok()),
    };

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
            // Handler returned something we couldn't cast — leave a
            // log breadcrumb and fall through to a soft accept.
            tracing::warn!(target: "siphon_smpp",
                "smpp.on_pdu handler returned a non-PduReply value; defaulting to ESME_ROK");
            Ok(PduReply::default_ok())
        })
    })
}

/// Find the registered handler whose filter matches `command`. The
/// `@smpp.on_pdu(command)` decorator stores the command in the
/// handler's `filter` slot; we read it back via `options(py)` since
/// `siphon::script::HandlerHandle` doesn't expose `filter()`
/// directly (the filter is what `handlers_for` returns; we pick the
/// first match that names this command).
///
/// `siphon::script::ScriptHandle::handlers_for("smpp.on_pdu")` returns
/// every registered SMPP PDU handler regardless of command — we still
/// have to filter by the per-handler command. The kind is the same;
/// the filter is what differs. The handler list is a snapshot, so
/// it's cheap to walk it on each dispatch.
fn pick_pdu_handler(
    script: &ScriptHandle,
    command: &str,
) -> PyResult<Option<siphon::script::HandlerHandle>> {
    let handlers = script.handlers_for("smpp.on_pdu");
    Python::attach(|py| -> PyResult<Option<siphon::script::HandlerHandle>> {
        for h in handlers {
            // The decorator writes the command name into `filter`;
            // the script registry exposes it through `kind()` /
            // `options()`. Until `HandlerHandle::filter()` lands, the
            // command is mirrored as `options.command`.
            let opts = h.options(py);
            let matches = match opts {
                Some(d) => {
                    if let Ok(Some(cmd)) = d.get_item("command") {
                        cmd.extract::<String>().ok().as_deref() == Some(command)
                    } else {
                        false
                    }
                }
                None => false,
            };
            if matches {
                return Ok(Some(h));
            }
        }
        Ok(None)
    })
}

/// Look up a `@smpp.on_bind` handler and call it. Handler returns
/// truthy → bind accepted. Handler missing → reject (closed by
/// default; the script is the authority on credentials).
async fn dispatch_bind(
    script: &ScriptHandle,
    system_id: &str,
    password: &str,
    client_addr: &str,
) -> PyResult<bool> {
    let handlers = script.handlers_for("smpp.on_bind");
    let handler = match handlers.into_iter().next() {
        Some(h) => h,
        None => return Ok(false),
    };

    let bind = Bind {
        system_id: system_id.to_string(),
        password: password.to_string(),
        client_addr: client_addr.to_string(),
    };

    let py_args = Python::attach(|py| -> PyResult<Vec<Py<PyAny>>> {
        Ok(vec![Py::new(py, bind)?.into_any()])
    })?;

    let result = script.call_handler(&handler, py_args).await?;

    Python::attach(|py| -> PyResult<bool> {
        let bound = result.bind(py);
        if bound.is_none() {
            // No-return ≡ rejection. Forces explicit accept/reject in
            // scripts, no accidental open binds.
            return Ok(false);
        }
        bound.is_truthy()
    })
}
