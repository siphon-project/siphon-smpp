//! `siphon.smpp.submit_via(...)` — script-facing async function for
//! submitting a `submit_sm` via a configured outbound bind.
//!
//! Looks up the bind in [`crate::runtime::state`] (set when the SMPP
//! task starts), calls `SMSC::send_submit_sm` on the bound
//! session, awaits the response, returns a typed [`SubmitResp`] back
//! to the script.

use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::*;

use crate::runtime;

/// Response returned by `submit_via` on success.
#[pyclass(module = "siphon.smpp", name = "SubmitResp", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct SubmitResp {
    #[pyo3(get)]
    pub command_status: String,
    #[pyo3(get)]
    pub message_id: String,
}

#[pymethods]
impl SubmitResp {
    fn __repr__(&self) -> String {
        format!(
            "SubmitResp(command_status={:?}, message_id={:?})",
            self.command_status, self.message_id
        )
    }
}

/// Submit a PDU via the named outbound bind. Async — returns an
/// awaitable Python coroutine that resolves to a `SubmitResp`.
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
    let state = runtime::state().ok_or_else(|| {
        PyRuntimeError::new_err("siphon-smpp runtime not started — the SMPP task is not registered")
    })?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        // Snapshot the SMSC handle out of the lock; release before
        // awaiting so other binds aren't blocked behind one mutex.
        let smsc = {
            let binds = state.binds.lock().await;
            binds
                .iter()
                .find(|t| t.name == bind)
                .map(|t| t.smsc.clone())
                .ok_or_else(|| PyKeyError::new_err(format!("bind {bind:?} not bound")))?
        };

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
            Ok(r) => Ok(SubmitResp {
                command_status: "ESME_ROK".into(),
                message_id: r.message_id.unwrap_or_default(),
            }),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "bind {bind:?} submit_sm failed: {e:?}"
            ))),
        }
    })
}
