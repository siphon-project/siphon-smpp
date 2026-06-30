//! Public addon API.
//!
//! Exposes [`namespace`] (the `smpp` Python module) and [`task`] (the SMPP
//! runtime) for a composing siphon binary.
//!
//! The namespace is a Python module loaded from `python/smpp.py`. It
//! defines the decorators (`@smpp.on_pdu("submit_sm")` etc.) which
//! write into the script registry per the "decorator façade in your
//! extension's Python module" pattern. The Rust side never has to ship
//! PyO3 closure plumbing — script authors get a normal Python module.
//!
//! Cfg readouts (`smpp.bind_address()` etc.) read from a `_config` dict
//! that the install closure injects on the module before the script
//! runs.

use std::ffi::CString;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};

use crate::pyclasses::{AlertNotification, Bind, BindResult, Pdu, PduReply, Session};
use crate::sends::{
    alert_to, cancel_via, data_to, data_via, deliver_to, query_via, replace_via, submit_via,
    SmppResp,
};
use crate::SmppConfig;

const NAMESPACE_SOURCE: &str = include_str!("../python/smpp.py");

/// Build the `smpp` namespace-module closure.
///
/// On call, the closure compiles `python/smpp.py` into a Python module,
/// injects the addon config as `_config`, and returns the module —
/// which siphon then attaches as `siphon.smpp`.
pub fn namespace(
    cfg: SmppConfig,
) -> impl FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send + 'static {
    move |py| {
        let source = CString::new(NAMESPACE_SOURCE).expect("python/smpp.py contains no NUL bytes");
        let file = c"siphon_smpp/__init__.py";
        let module_name = c"smpp";
        let module = PyModule::from_code(py, source.as_c_str(), file, module_name)?;

        let cfg_dict = build_config_dict(py, &cfg)?;
        module.setattr("_config", cfg_dict)?;

        // ── Rust pyclasses for handler dispatch ────────────────────
        // The dispatcher in `runtime.rs` constructs `Pdu`, `Session`,
        // `Bind`, `AlertNotification` instances and passes them into
        // script handlers; the script reads fields and calls
        // `pdu.reply(...)` / `bind.accept()` / `bind.reject(...)`,
        // returning a `PduReply` / `BindResult`. Expose all of them on
        // the namespace so scripts can also import the types for
        // static-typing helpers.
        module.add_class::<Pdu>()?;
        module.add_class::<PduReply>()?;
        module.add_class::<Session>()?;
        module.add_class::<Bind>()?;
        module.add_class::<BindResult>()?;
        module.add_class::<AlertNotification>()?;
        module.add_class::<SmppResp>()?;

        // ── Send helpers ──────────────────────────────────────────
        // These need the runtime state (set by the task); each is
        // import-time-safe and returns an awaitable that errors with a
        // clear message if state isn't up yet. Two families:
        //   * outbound, target a bind by name: submit_via / data_via /
        //     cancel_via (+ query_via / replace_via forward-compat stubs)
        //   * inbound, target a bound ESME by session_id: deliver_to /
        //     data_to / alert_to
        module.add_function(wrap_pyfunction!(submit_via, &module)?)?;
        module.add_function(wrap_pyfunction!(data_via, &module)?)?;
        module.add_function(wrap_pyfunction!(cancel_via, &module)?)?;
        module.add_function(wrap_pyfunction!(query_via, &module)?)?;
        module.add_function(wrap_pyfunction!(replace_via, &module)?)?;
        module.add_function(wrap_pyfunction!(deliver_to, &module)?)?;
        module.add_function(wrap_pyfunction!(data_to, &module)?)?;
        module.add_function(wrap_pyfunction!(alert_to, &module)?)?;

        Ok(module.into_any().unbind())
    }
}

/// Build the SMPP runtime task closure.
///
/// Internally:
///   1. Spawns the tokio listener on `script.tokio_handle()` so the
///      per-worker asyncio loop is live on the call thread (per the
///      ScriptHandle contract).
///   2. On each inbound PDU, looks up matching handlers via
///      `script.handlers_for("smpp.on_pdu")` filtered by command, then
///      `script.call_handler(...)` to invoke them.
pub fn task(cfg: SmppConfig) -> impl FnOnce(siphon::script::ScriptHandle) + Send + 'static {
    move |script| {
        crate::runtime::spawn(cfg, script);
    }
}

/// Build the `_config` dict the script reads via `siphon.smpp.config()`.
/// Mirrors the public shape of [`SmppConfig`] — keep them in sync.
fn build_config_dict<'py>(py: Python<'py>, cfg: &SmppConfig) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);

    // Server / inbound listener
    let (host, port) = cfg.listen();
    let server = PyDict::new(py);
    server.set_item("bind_address", host)?;
    server.set_item("port", port)?;
    server.set_item("session_init_timer_ms", cfg.server.session_init_timer_ms)?;
    server.set_item("enquire_link_timer_ms", cfg.server.enquire_link_timer_ms)?;
    server.set_item("inactivity_timer_ms", cfg.server.inactivity_timer_ms)?;
    server.set_item("response_timer_ms", cfg.server.response_timer_ms)?;
    dict.set_item("server", server)?;

    // Outbound binds
    let binds = PyList::empty(py);
    for bind in &cfg.binds {
        let t = PyDict::new(py);
        t.set_item("name", &bind.name)?;
        t.set_item("host", &bind.host)?;
        t.set_item("port", bind.port)?;
        t.set_item("system_id", &bind.system_id)?;
        // We deliberately do NOT expose the password to the script —
        // the addon's runtime side handles bind authentication. If a
        // script needs a bind identity, `name` is the handle.
        t.set_item("system_type", &bind.system_type)?;
        t.set_item("bind_type", &bind.bind_type)?;
        t.set_item("max_msg_per_sec", bind.max_msg_per_sec)?;
        binds.append(t)?;
    }
    dict.set_item("binds", binds)?;

    // Routing
    let routing = PyDict::new(py);
    let default_chain = PyList::new(py, cfg.routing.default_chain.iter())?;
    routing.set_item("default_chain", default_chain)?;

    let rules = PyList::empty(py);
    for rule in &cfg.routing.rules {
        let r = PyDict::new(py);
        r.set_item("prefix", &rule.prefix)?;
        r.set_item("name", &rule.name)?;
        let chain = PyList::new(py, rule.chain.iter())?;
        r.set_item("chain", chain)?;
        let opts = PyDict::new(py);
        for (k, v) in &rule.options {
            // serde_yaml::Value → JSON-ish PyAny. Cheap path: stringify
            // and let the script treat as opaque metadata.
            opts.set_item(k, serde_yaml::to_string(v).unwrap_or_default())?;
        }
        r.set_item("options", opts)?;
        rules.append(r)?;
    }
    routing.set_item("rules", rules)?;
    dict.set_item("routing", routing)?;

    Ok(dict)
}
