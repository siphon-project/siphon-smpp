//! Script-facing async **send** helpers.
//!
//! Two families, mirroring the two SMPP directions siphon-smpp drives:
//!
//! * **Outbound binds** (we are the ESME; target by bind name) —
//!   [`submit_via`], [`data_via`], [`cancel_via`], and the
//!   forward-compat stubs [`query_via`] / [`replace_via`].
//! * **Inbound sessions** (we are the SMSC; target a bound ESME by
//!   `session_id`) — [`deliver_to`], [`data_to`], [`alert_to`].
//!
//! Each resolves the session out of [`crate::runtime::state`] (set when
//! the SMPP task starts), clones the handle, **drops the lock before
//! awaiting** so one slow peer can't block another, then awaits the
//! response and returns a typed [`SmppResp`].

use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::*;

use smpp34::client::SMSC;
use smpp34::server::ESME;
use smpp34::DestAddress;

use crate::metrics;
use crate::runtime::{self, RateLimiter, State};
use std::sync::Arc;

/// Response returned by the send helpers.
///
/// `command_status` is the SMPP status name ("ESME_ROK" on success).
/// `message_id` is the SMSC-assigned id when the op returns one
/// (`submit_sm` / `data_sm`); empty otherwise (`deliver_sm`,
/// `cancel_sm`).
#[pyclass(module = "siphon.smpp", name = "SmppResp", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct SmppResp {
    #[pyo3(get)]
    pub command_status: String,
    #[pyo3(get)]
    pub message_id: String,
}

#[pymethods]
impl SmppResp {
    #[getter]
    fn ok(&self) -> bool {
        self.command_status == "ESME_ROK"
    }

    fn __repr__(&self) -> String {
        format!(
            "SmppResp(command_status={:?}, message_id={:?})",
            self.command_status, self.message_id
        )
    }
}

impl SmppResp {
    fn ok_with(message_id: String) -> Self {
        Self {
            command_status: "ESME_ROK".to_string(),
            message_id,
        }
    }
}

/// Response returned by [`query_via`] — the result of a `query_sm`.
///
/// `message_state` is the SMPP message-state code (1=ENROUTE, 2=DELIVERED,
/// 3=EXPIRED, 4=DELETED, 5=UNDELIVERABLE, 6=ACCEPTED, 7=UNKNOWN,
/// 8=REJECTED). `final_date` is the SMPP-format absolute time (empty if
/// not final); `error_code` the network error code.
#[pyclass(module = "siphon.smpp", name = "QueryResp", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct QueryResp {
    #[pyo3(get)]
    pub command_status: String,
    #[pyo3(get)]
    pub message_id: String,
    #[pyo3(get)]
    pub message_state: u8,
    #[pyo3(get)]
    pub final_date: String,
    #[pyo3(get)]
    pub error_code: u8,
}

#[pymethods]
impl QueryResp {
    #[getter]
    fn ok(&self) -> bool {
        self.command_status == "ESME_ROK"
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryResp(command_status={:?}, message_id={:?}, message_state={}, final_date={:?}, error_code={})",
            self.command_status, self.message_id, self.message_state, self.final_date, self.error_code
        )
    }
}

// ── Lookups (clone the handle out, then drop the guard) ─────────────────

/// Resolve an outbound bind by name → its `SMSC` handle + optional
/// rate limiter. Returns a `PyKeyError` if the bind isn't currently
/// bound (it may be mid-reconnect).
async fn bind_handle(
    state: &Arc<State>,
    bind: &str,
) -> PyResult<(Arc<SMSC>, Option<Arc<RateLimiter>>)> {
    let binds = state.binds.lock().await;
    binds
        .iter()
        .find(|b| b.name == bind)
        .map(|b| (b.smsc.clone(), b.throttle.clone()))
        .ok_or_else(|| PyKeyError::new_err(format!("bind {bind:?} not bound")))
}

/// Resolve an inbound ESME session by `session_id`. Returns a
/// `PyKeyError` if no such session is currently bound.
async fn esme_handle(state: &Arc<State>, session_id: &str) -> PyResult<Arc<ESME>> {
    let esmes = state.esmes.lock().await;
    esmes
        .iter()
        .find(|e| e.esme.session_id == session_id)
        .map(|e| e.esme.clone())
        .ok_or_else(|| PyKeyError::new_err(format!("esme session {session_id:?} not bound")))
}

// ── Outbound: target a bind by name ─────────────────────────────────────

/// Submit a `submit_sm` via the named outbound bind. Async — returns an
/// awaitable resolving to an [`SmppResp`] carrying the SMSC message_id.
// Wide by design: the signature mirrors every SMPP submit_sm field so scripts
// can set any of them as a kwarg.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    bind,
    source_addr,
    destination_addr,
    short_message,
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    protocol_id = 0,
    priority_flag = 0,
    schedule_delivery_time = String::new(),
    validity_period = String::new(),
    registered_delivery = 0,
    replace_if_present_flag = 0,
    data_coding = 0,
    sm_default_msg_id = 0,
))]
pub fn submit_via<'py>(
    py: Python<'py>,
    bind: String,
    source_addr: String,
    destination_addr: String,
    short_message: Vec<u8>,
    source_addr_ton: u8,
    source_addr_npi: u8,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    service_type: String,
    esm_class: u8,
    protocol_id: u8,
    priority_flag: u8,
    schedule_delivery_time: String,
    validity_period: String,
    registered_delivery: u8,
    replace_if_present_flag: u8,
    data_coding: u8,
    sm_default_msg_id: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, throttle) = bind_handle(&state, &bind).await?;
        if let Some(limiter) = throttle {
            // A pacing wait is an egress throttle event.
            if limiter.acquire().await {
                metrics::record_throttled(metrics::EGRESS);
            }
        }
        let resp = smsc
            .submit_sm()
            .service_type(service_type)
            .source_addr_ton(source_addr_ton)
            .source_addr_npi(source_addr_npi)
            .source_addr(source_addr)
            .dest_addr_ton(dest_addr_ton)
            .dest_addr_npi(dest_addr_npi)
            .destination_addr(destination_addr)
            .esm_class(esm_class)
            .protocol_id(protocol_id)
            .priority_flag(priority_flag)
            .schedule_delivery_time(schedule_delivery_time)
            .validity_period(validity_period)
            .registered_delivery(registered_delivery)
            .replace_if_present_flag(replace_if_present_flag)
            .data_coding(data_coding)
            .sm_default_msg_id(sm_default_msg_id)
            .short_message(short_message)
            .send()
            .await;
        match resp {
            Ok(r) => Ok(SmppResp::ok_with(r.message_id.unwrap_or_default())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} submit_sm failed: {e:?}"
            ))),
        }
    })
}

/// Submit one message to **many destinations** (`submit_sm_multi`) via the
/// named outbound bind. `destinations` is a list of SME address strings.
/// Resolves to an [`SmppResp`] with the SMSC message_id.
///
/// NOTE: requires a TX-capable bind (transmitter / transceiver).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    bind,
    source_addr,
    destinations,
    short_message,
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    protocol_id = 0,
    priority_flag = 0,
    schedule_delivery_time = String::new(),
    validity_period = String::new(),
    registered_delivery = 0,
    replace_if_present_flag = 0,
    data_coding = 0,
    sm_default_msg_id = 0,
))]
pub fn submit_multi_via<'py>(
    py: Python<'py>,
    bind: String,
    source_addr: String,
    destinations: Vec<String>,
    short_message: Vec<u8>,
    source_addr_ton: u8,
    source_addr_npi: u8,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    service_type: String,
    esm_class: u8,
    protocol_id: u8,
    priority_flag: u8,
    schedule_delivery_time: String,
    validity_period: String,
    registered_delivery: u8,
    replace_if_present_flag: u8,
    data_coding: u8,
    sm_default_msg_id: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    let dest_addresses: Vec<DestAddress> = destinations
        .into_iter()
        .map(|destination_addr| DestAddress::Sme {
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
        })
        .collect();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, throttle) = bind_handle(&state, &bind).await?;
        if let Some(limiter) = throttle {
            // A pacing wait is an egress throttle event.
            if limiter.acquire().await {
                metrics::record_throttled(metrics::EGRESS);
            }
        }
        let resp = smsc
            .send_submit_sm_multi(
                service_type,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                dest_addresses,
                esm_class,
                protocol_id,
                priority_flag,
                schedule_delivery_time,
                validity_period,
                registered_delivery,
                replace_if_present_flag,
                data_coding,
                sm_default_msg_id,
                short_message,
            )
            .await;
        match resp {
            Ok(r) => Ok(SmppResp::ok_with(r.message_id.unwrap_or_default())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} submit_sm_multi failed: {e:?}"
            ))),
        }
    })
}

/// Send a `data_sm` via the named outbound bind. `data_sm` is the
/// TLV-based alternative to `submit_sm`; the message body travels in the
/// `message_payload` TLV (set by the SMSC), so this helper carries the
/// addressing + coding only.
///
/// NOTE: requires a TX-capable bind (transmitter / transceiver).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    bind,
    source_addr,
    destination_addr,
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    registered_delivery = 0,
    data_coding = 0,
))]
pub fn data_via<'py>(
    py: Python<'py>,
    bind: String,
    source_addr: String,
    destination_addr: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    service_type: String,
    esm_class: u8,
    registered_delivery: u8,
    data_coding: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, throttle) = bind_handle(&state, &bind).await?;
        if let Some(limiter) = throttle {
            // A pacing wait is an egress throttle event.
            if limiter.acquire().await {
                metrics::record_throttled(metrics::EGRESS);
            }
        }
        let resp = smsc
            .send_data_sm(
                service_type,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                dest_addr_ton,
                dest_addr_npi,
                destination_addr,
                esm_class,
                registered_delivery,
                data_coding,
            )
            .await;
        match resp {
            Ok(_) => Ok(SmppResp::ok_with(String::new())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} data_sm failed: {e:?}"
            ))),
        }
    })
}

/// Cancel a previously-submitted message via the named outbound bind.
/// Pass the SMSC-assigned `message_id` (and the original source/dest if
/// the SMSC requires them to scope the cancel).
///
/// NOTE: requires a TX-capable bind (transmitter / transceiver).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    bind,
    message_id,
    source_addr = String::new(),
    destination_addr = String::new(),
    service_type = String::new(),
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
))]
pub fn cancel_via<'py>(
    py: Python<'py>,
    bind: String,
    message_id: String,
    source_addr: String,
    destination_addr: String,
    service_type: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, _) = bind_handle(&state, &bind).await?;
        let resp = smsc
            .send_cancel_sm(
                service_type,
                message_id,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                dest_addr_ton,
                dest_addr_npi,
                destination_addr,
            )
            .await;
        match resp {
            Ok(_) => Ok(SmppResp::ok_with(String::new())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} cancel_sm failed: {e:?}"
            ))),
        }
    })
}

/// Query the state of a previously-submitted message via the named
/// outbound bind. Resolves to a [`QueryResp`] carrying `message_state`
/// (1=ENROUTE … 8=REJECTED), `final_date` and `error_code`.
///
/// NOTE: requires a TX-capable bind (transmitter / transceiver).
#[pyfunction]
#[pyo3(signature = (*, bind, message_id, source_addr = String::new(),
                    source_addr_ton = 1, source_addr_npi = 1))]
pub fn query_via<'py>(
    py: Python<'py>,
    bind: String,
    message_id: String,
    source_addr: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, _) = bind_handle(&state, &bind).await?;
        let resp = smsc
            .send_query_sm(message_id, source_addr_ton, source_addr_npi, source_addr)
            .await;
        match resp {
            Ok(r) => Ok(QueryResp {
                command_status: "ESME_ROK".to_string(),
                message_id: r.message_id,
                message_state: r.message_state,
                final_date: r.final_date,
                error_code: r.error_code,
            }),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} query_sm failed: {e:?}"
            ))),
        }
    })
}

/// Replace a previously-submitted message via the named outbound bind.
/// Pass the SMSC-assigned `message_id` and the new `short_message`.
///
/// NOTE: requires a TX-capable bind (transmitter / transceiver).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (*, bind, message_id, source_addr = String::new(),
                    source_addr_ton = 1, source_addr_npi = 1,
                    schedule_delivery_time = String::new(),
                    validity_period = String::new(),
                    registered_delivery = 0, sm_default_msg_id = 0,
                    short_message = Vec::<u8>::new()))]
pub fn replace_via<'py>(
    py: Python<'py>,
    bind: String,
    message_id: String,
    source_addr: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    schedule_delivery_time: String,
    validity_period: String,
    registered_delivery: u8,
    sm_default_msg_id: u8,
    short_message: Vec<u8>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, _) = bind_handle(&state, &bind).await?;
        let resp = smsc
            .send_replace_sm(
                message_id,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                schedule_delivery_time,
                validity_period,
                registered_delivery,
                sm_default_msg_id,
                short_message,
            )
            .await;
        match resp {
            Ok(_) => Ok(SmppResp::ok_with(String::new())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} replace_sm failed: {e:?}"
            ))),
        }
    })
}

// ── Inbound: target a bound ESME by session_id ──────────────────────────

/// Deliver a `deliver_sm` to a bound ESME (identified by `session_id`).
/// This is the SMSC→ESME half: MT/MO content **and** delivery receipts
/// (set `esm_class=0x04` + a receipt body) route back to the originating
/// ESME through here.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    session_id,
    source_addr,
    destination_addr,
    short_message,
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    protocol_id = 0,
    priority_flag = 0,
    schedule_delivery_time = String::new(),
    validity_period = String::new(),
    registered_delivery = 0,
    replace_if_present_flag = 0,
    data_coding = 0,
    sm_default_msg_id = 0,
))]
pub fn deliver_to<'py>(
    py: Python<'py>,
    session_id: String,
    source_addr: String,
    destination_addr: String,
    short_message: Vec<u8>,
    source_addr_ton: u8,
    source_addr_npi: u8,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    service_type: String,
    esm_class: u8,
    protocol_id: u8,
    priority_flag: u8,
    schedule_delivery_time: String,
    validity_period: String,
    registered_delivery: u8,
    replace_if_present_flag: u8,
    data_coding: u8,
    sm_default_msg_id: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let esme = esme_handle(&state, &session_id).await?;
        if !esme.can_receive {
            return Err(PyRuntimeError::new_err(format!(
                "esme session {session_id:?} is not RX/TRX — cannot deliver_sm"
            )));
        }
        let resp = esme
            .send_deliver_sm(
                service_type,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                dest_addr_ton,
                dest_addr_npi,
                destination_addr,
                esm_class,
                protocol_id,
                priority_flag,
                schedule_delivery_time,
                validity_period,
                registered_delivery,
                replace_if_present_flag,
                data_coding,
                sm_default_msg_id,
                short_message,
            )
            .await;
        match resp {
            Ok(_) => Ok(SmppResp::ok_with(String::new())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "deliver_sm to session {session_id:?} failed: {e:?}"
            ))),
        }
    })
}

/// Send a `data_sm` to a bound ESME (identified by `session_id`).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    session_id,
    source_addr,
    destination_addr,
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    registered_delivery = 0,
    data_coding = 0,
))]
pub fn data_to<'py>(
    py: Python<'py>,
    session_id: String,
    source_addr: String,
    destination_addr: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    service_type: String,
    esm_class: u8,
    registered_delivery: u8,
    data_coding: u8,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let esme = esme_handle(&state, &session_id).await?;
        let resp = esme
            .send_data_sm(
                service_type,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                dest_addr_ton,
                dest_addr_npi,
                destination_addr,
                esm_class,
                registered_delivery,
                data_coding,
            )
            .await;
        match resp {
            Ok(_) => Ok(SmppResp::ok_with(String::new())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "data_sm to session {session_id:?} failed: {e:?}"
            ))),
        }
    })
}

/// Send an `alert_notification` to a bound ESME — tell it a previously
/// unavailable MS is reachable again so it can flush queued MT.
/// `alert_notification` is a notification (no response); resolves to an
/// [`SmppResp`] (always `ESME_ROK`) once written.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    session_id,
    source_addr,
    esme_addr,
    source_addr_ton = 1,
    source_addr_npi = 1,
    esme_addr_ton = 1,
    esme_addr_npi = 1,
    ms_availability_status = None,
))]
pub fn alert_to<'py>(
    py: Python<'py>,
    session_id: String,
    source_addr: String,
    esme_addr: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    esme_addr_ton: u8,
    esme_addr_npi: u8,
    ms_availability_status: Option<u8>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let esme = esme_handle(&state, &session_id).await?;
        if !esme.can_receive {
            return Err(PyRuntimeError::new_err(format!(
                "esme session {session_id:?} is not RX/TRX — cannot alert_notification"
            )));
        }
        esme.send_alert_notification(
            source_addr_ton,
            source_addr_npi,
            source_addr,
            esme_addr_ton,
            esme_addr_npi,
            esme_addr,
            ms_availability_status,
        )
        .await;
        Ok(SmppResp::ok_with(String::new()))
    })
}

// ── Shared ──────────────────────────────────────────────────────────────

fn require_state() -> PyResult<Arc<State>> {
    runtime::state().ok_or_else(|| {
        PyRuntimeError::new_err("siphon-smpp runtime not started — the SMPP task is not registered")
    })
}
