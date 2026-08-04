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

use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use smpp34::client::SMSC;
use smpp34::server::ESME;
use smpp34::{data_sm, submit_sm_multi, DestAddress, Tlv, TlvList, TlvTag};

use crate::metrics;
use crate::runtime::{self, RateLimiter, State};
use crate::tlv::tlvs_from_py;
use std::sync::Arc;

/// `short_message` is one length octet plus at most 254 octets of body
/// (§5.2.21). smpp34's PDU constructors `assert!` on that limit, so an
/// unchecked long body from a script would panic the SMPP runtime rather
/// than fail the call — check it here and point at `message_payload`,
/// which is where a body over the limit belongs (§5.3.2.32).
const SHORT_MESSAGE_MAX: usize = 254;

fn check_short_message(command: &str, len: usize, payload_available: bool) -> PyResult<()> {
    if len <= SHORT_MESSAGE_MAX {
        return Ok(());
    }
    let hint = if payload_available {
        " — send it as tlvs={\"MESSAGE_PAYLOAD\": …} with short_message=b\"\" instead"
    } else {
        ""
    };
    Err(PyValueError::new_err(format!(
        "{command} short_message is {len} bytes, over the SMPP 3.4 \
         {SHORT_MESSAGE_MAX}-byte limit{hint}"
    )))
}

/// Fold a helper's `short_message=` into the `message_payload` optional
/// parameter. A `data_sm` has no `short_message` field at all — its body
/// exists only as that TLV (§4.2.2) — so this is the only way one carries
/// a message.
///
/// Supplying both a body and an explicit `MESSAGE_PAYLOAD` is rejected:
/// the intent is ambiguous, and silently picking one is how a message goes
/// missing.
fn fold_message_payload(
    command: &str,
    short_message: Vec<u8>,
    tlvs: &mut Vec<Tlv>,
) -> PyResult<()> {
    if short_message.is_empty() {
        return Ok(());
    }
    if tlvs.message_payload().is_some() {
        return Err(PyValueError::new_err(format!(
            "{command}: pass the body either as short_message= or as \
             tlvs={{\"MESSAGE_PAYLOAD\": …}}, not both"
        )));
    }
    tlvs.push(Tlv::from_tag(TlvTag::MessagePayload, short_message));
    Ok(())
}

/// Build the `data_sm` both `data_via` and `data_to` put on the wire.
/// Sequence number 0 — the session overwrites it in `send_data_sm_pdu`,
/// which owns the sequence space.
#[allow(clippy::too_many_arguments)]
fn build_data_sm(
    service_type: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    source_addr: String,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    destination_addr: String,
    esm_class: u8,
    registered_delivery: u8,
    data_coding: u8,
    short_message: Vec<u8>,
    mut tlvs: Vec<Tlv>,
) -> PyResult<data_sm> {
    fold_message_payload("data_sm", short_message, &mut tlvs)?;
    Ok(data_sm::new(
        0,
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
    .with_tlvs(tlvs))
}

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
    tlvs = None,
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
    tlvs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    check_short_message("submit_sm", short_message.len(), true)?;
    let tlvs = tlvs_from_py(tlvs)?;
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
            .tlvs(tlvs)
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
    tlvs = None,
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
    tlvs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    check_short_message("submit_sm_multi", short_message.len(), true)?;
    let tlvs = tlvs_from_py(tlvs)?;
    let dest_addresses: Vec<DestAddress> = destinations
        .into_iter()
        .map(|destination_addr| DestAddress::Sme {
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
        })
        .collect();
    if dest_addresses.len() > 254 {
        // submit_sm_multi carries a one-octet number_of_dests (§4.5.1) and
        // smpp34 asserts on it; fail the call rather than panic the runtime.
        return Err(PyValueError::new_err(format!(
            "submit_sm_multi addresses {} destinations, over the SMPP 3.4 limit of 254",
            dest_addresses.len()
        )));
    }
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, throttle) = bind_handle(&state, &bind).await?;
        if let Some(limiter) = throttle {
            // A pacing wait is an egress throttle event.
            if limiter.acquire().await {
                metrics::record_throttled(metrics::EGRESS);
            }
        }
        // Sequence number 0: send_submit_sm_multi_pdu owns the sequence
        // space and overwrites it.
        let pdu = submit_sm_multi::new(
            0,
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
        .with_tlvs(tlvs);
        let resp = smsc.send_submit_sm_multi_pdu(pdu).await;
        match resp {
            Ok(r) => Ok(SmppResp::ok_with(r.message_id.unwrap_or_default())),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} submit_sm_multi failed: {e:?}"
            ))),
        }
    })
}

/// Send a `data_sm` via the named outbound bind. `data_sm` is the
/// TLV-based alternative to `submit_sm`: it has **no `short_message`
/// field**, so `short_message=` here is carried in the `message_payload`
/// optional parameter (§4.2.2), which is also what lets it exceed the
/// 254-byte `submit_sm` limit.
///
/// NOTE: requires a TX-capable bind (transmitter / transceiver).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    bind,
    source_addr,
    destination_addr,
    short_message = Vec::<u8>::new(),
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    registered_delivery = 0,
    data_coding = 0,
    tlvs = None,
))]
pub fn data_via<'py>(
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
    registered_delivery: u8,
    data_coding: u8,
    tlvs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    let pdu = build_data_sm(
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
        short_message,
        tlvs_from_py(tlvs)?,
    )?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (smsc, throttle) = bind_handle(&state, &bind).await?;
        if let Some(limiter) = throttle {
            // A pacing wait is an egress throttle event.
            if limiter.acquire().await {
                metrics::record_throttled(metrics::EGRESS);
            }
        }
        let resp = smsc.send_data_sm_pdu(pdu).await;
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
    // replace_sm has no optional parameters in 3.4, so there is no
    // message_payload to fall back on — the body has to fit.
    check_short_message("replace_sm", short_message.len(), false)?;
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
    tlvs = None,
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
    tlvs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    check_short_message("deliver_sm", short_message.len(), true)?;
    let tlvs = tlvs_from_py(tlvs)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let esme = esme_handle(&state, &session_id).await?;
        if !esme.can_receive {
            return Err(PyRuntimeError::new_err(format!(
                "esme session {session_id:?} is not RX/TRX — cannot deliver_sm"
            )));
        }
        let resp = esme
            .deliver_sm()
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
            .tlvs(tlvs)
            .send()
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
/// As with [`data_via`], `short_message=` travels in the `message_payload`
/// optional parameter — a `data_sm` has no `short_message` field (§4.2.2).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    session_id,
    source_addr,
    destination_addr,
    short_message = Vec::<u8>::new(),
    source_addr_ton = 1,
    source_addr_npi = 1,
    dest_addr_ton = 1,
    dest_addr_npi = 1,
    service_type = String::new(),
    esm_class = 0,
    registered_delivery = 0,
    data_coding = 0,
    tlvs = None,
))]
pub fn data_to<'py>(
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
    registered_delivery: u8,
    data_coding: u8,
    tlvs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let state = require_state()?;
    let pdu = build_data_sm(
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
        short_message,
        tlvs_from_py(tlvs)?,
    )?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let esme = esme_handle(&state, &session_id).await?;
        let resp = esme.send_data_sm_pdu(pdu).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    // The `data_sm` a send helper builds is asserted against the SMPP 3.4
    // §4.2.2 / §3.2.1 wire image written out by hand. A round-trip through
    // our own decoder would pass even if both halves shared a bug.

    fn build(short_message: &[u8], tlvs: Vec<Tlv>) -> PyResult<Vec<u8>> {
        Ok(build_data_sm(
            String::new(),    // service_type
            1,                // source_addr_ton
            1,                // source_addr_npi
            "5550100".into(), // source_addr
            1,                // dest_addr_ton
            1,                // dest_addr_npi
            "5550199".into(), // destination_addr
            0,                // esm_class
            1,                // registered_delivery
            0,                // data_coding
            short_message.to_vec(),
            tlvs,
        )?
        .encode())
    }

    /// The full expected wire image, so the test states the layout rather
    /// than deriving it from the code under test.
    fn expected(tlv_bytes: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(0x00); // service_type: empty C-Octet String
        body.push(0x01); // source_addr_ton
        body.push(0x01); // source_addr_npi
        body.extend_from_slice(b"5550100\0");
        body.push(0x01); // dest_addr_ton
        body.push(0x01); // dest_addr_npi
        body.extend_from_slice(b"5550199\0");
        body.push(0x00); // esm_class
        body.push(0x01); // registered_delivery
        body.push(0x00); // data_coding
        body.extend_from_slice(tlv_bytes);

        let mut pdu = Vec::new();
        pdu.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
        pdu.extend_from_slice(&0x0000_0103u32.to_be_bytes()); // command_id: data_sm
        pdu.extend_from_slice(&0u32.to_be_bytes()); // command_status
        pdu.extend_from_slice(&0u32.to_be_bytes()); // sequence_number (session assigns)
        pdu.extend_from_slice(&body);
        pdu
    }

    #[test]
    fn short_message_becomes_the_message_payload_tlv() {
        Python::attach(|_py| {
            // A data_sm has no short_message field: the body only exists as
            // optional parameter 0x0424 (§4.2.2).
            let got = build(b"hello", Vec::new()).expect("builds");
            assert_eq!(
                got,
                expected(&[0x04, 0x24, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o'])
            );
        });
    }

    #[test]
    fn a_body_over_the_short_message_limit_still_fits() {
        Python::attach(|_py| {
            // 300 bytes — impossible in submit_sm, routine in a data_sm.
            let long = vec![b'x'; 300];
            let got = build(&long, Vec::new()).expect("builds");
            let mut tlv = vec![0x04, 0x24, 0x01, 0x2C];
            tlv.extend_from_slice(&long);
            assert_eq!(got, expected(&tlv));
        });
    }

    #[test]
    fn no_body_and_no_tlvs_encodes_no_optional_parameters() {
        Python::attach(|_py| {
            assert_eq!(build(b"", Vec::new()).expect("builds"), expected(&[]));
        });
    }

    #[test]
    fn an_explicit_message_payload_is_carried_verbatim() {
        Python::attach(|_py| {
            let tlvs = vec![Tlv::from_tag(TlvTag::MessagePayload, b"hi".to_vec())];
            let got = build(b"", tlvs).expect("builds");
            assert_eq!(got, expected(&[0x04, 0x24, 0x00, 0x02, b'h', b'i']));
        });
    }

    #[test]
    fn a_body_given_twice_is_rejected_rather_than_silently_halved() {
        Python::attach(|py| {
            let tlvs = vec![Tlv::from_tag(TlvTag::MessagePayload, b"from tlv".to_vec())];
            let err = build(b"from short_message", tlvs).expect_err("ambiguous");
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(err.to_string().contains("not both"));
        });
    }

    #[test]
    fn short_message_limit_is_the_254_byte_spec_maximum() {
        Python::attach(|py| {
            // smpp34's submit_sm/deliver_sm constructors assert on this, so
            // an unchecked long body would panic the runtime, not fail the
            // call.
            assert!(check_short_message("submit_sm", 254, true).is_ok());
            let err = check_short_message("submit_sm", 255, true).expect_err("over the limit");
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(err.to_string().contains("MESSAGE_PAYLOAD"));

            // replace_sm has no message_payload to point at.
            let err = check_short_message("replace_sm", 255, false).expect_err("over the limit");
            assert!(!err.to_string().contains("MESSAGE_PAYLOAD"));
        });
    }
}
