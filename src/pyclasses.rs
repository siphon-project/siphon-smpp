//! Pyclasses surfaced to scripts at handler-dispatch time.
//!
//! The runtime constructs these from inbound PDUs and passes them into
//! the script via `ScriptHandle::call_handler`. Scripts read fields,
//! call `pdu.reply(...)` to produce a [`PduReply`], call
//! `bind.accept()` / `bind.reject()` (or simply return truthy) to
//! authorise a bind.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use smpp34::{deliver_sm, submit_sm, SmppError};

// ── Pdu ─────────────────────────────────────────────────────────────────

/// Common surface for `submit_sm` and `deliver_sm`. Field names mirror
/// SMPP 3.4 §5.2; same shape on both directions so the routing layer
/// can treat them uniformly.
#[pyclass(module = "siphon.smpp", name = "Pdu", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct Pdu {
    #[pyo3(get)]
    pub command: String,
    #[pyo3(get)]
    pub service_type: String,
    #[pyo3(get)]
    pub source_addr_ton: u8,
    #[pyo3(get)]
    pub source_addr_npi: u8,
    #[pyo3(get)]
    pub source_addr: String,
    #[pyo3(get)]
    pub dest_addr_ton: u8,
    #[pyo3(get)]
    pub dest_addr_npi: u8,
    #[pyo3(get)]
    pub destination_addr: String,
    #[pyo3(get)]
    pub esm_class: u8,
    #[pyo3(get)]
    pub protocol_id: u8,
    #[pyo3(get)]
    pub priority_flag: u8,
    #[pyo3(get)]
    pub registered_delivery: u8,
    #[pyo3(get)]
    pub data_coding: u8,
    #[pyo3(get)]
    pub sm_length: u8,
    pub short_message: Vec<u8>,
}

#[pymethods]
impl Pdu {
    /// Raw SMS payload bytes — TPDU when `esm_class & 0x40` is set,
    /// otherwise the literal `short_message` field.
    #[getter]
    fn short_message<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.short_message)
    }

    /// True when the `short_message` field carries a TPDU
    /// (`UDHI` flag in `esm_class`). When set, decode the payload with an
    /// SMS-TPDU codec rather than treating it as a literal message.
    #[getter]
    fn is_tpdu(&self) -> bool {
        self.esm_class & 0x40 != 0
    }

    /// Build a reply for the dispatcher. Default is `ESME_ROK` with no
    /// message_id; pass `command_status="ESME_RSUBMITFAIL"` etc. to
    /// reject. Pass `message_id="…"` on success (submit_sm path).
    #[pyo3(signature = (*, command_status = "ESME_ROK".to_string(), message_id = None))]
    fn reply(&self, command_status: String, message_id: Option<String>) -> PyResult<PduReply> {
        let cs = parse_smpp_status(&command_status)?;
        Ok(PduReply {
            command_status: cs,
            message_id,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Pdu(command={}, source_addr={}, destination_addr={}, esm_class=0x{:02x}, dcs=0x{:02x}, len={})",
            self.command, self.source_addr, self.destination_addr,
            self.esm_class, self.data_coding, self.sm_length,
        )
    }
}

impl Pdu {
    pub(crate) fn from_submit(s: &submit_sm) -> Self {
        Self {
            command: "submit_sm".into(),
            service_type: s.service_type.clone(),
            source_addr_ton: s.source_addr_ton,
            source_addr_npi: s.source_addr_npi,
            source_addr: s.source_addr.clone(),
            dest_addr_ton: s.dest_addr_ton,
            dest_addr_npi: s.dest_addr_npi,
            destination_addr: s.destination_addr.clone(),
            esm_class: s.esm_class,
            protocol_id: s.protocol_id,
            priority_flag: s.priority_flag,
            registered_delivery: s.registered_delivery,
            data_coding: s.data_coding,
            sm_length: s.sm_length,
            short_message: s.short_message.clone(),
        }
    }

    pub(crate) fn from_deliver(d: &deliver_sm) -> Self {
        Self {
            command: "deliver_sm".into(),
            service_type: d.service_type.clone(),
            source_addr_ton: d.source_addr_ton,
            source_addr_npi: d.source_addr_npi,
            source_addr: d.source_addr.clone(),
            dest_addr_ton: d.dest_addr_ton,
            dest_addr_npi: d.dest_addr_npi,
            destination_addr: d.destination_addr.clone(),
            esm_class: d.esm_class,
            protocol_id: d.protocol_id,
            priority_flag: d.priority_flag,
            registered_delivery: d.registered_delivery,
            data_coding: d.data_coding,
            sm_length: d.sm_length,
            short_message: d.short_message.clone(),
        }
    }
}

// ── PduReply ────────────────────────────────────────────────────────────

/// What the script's `@smpp.on_pdu` handler returns — either
/// `pdu.reply(message_id="…")` for accept (submit_sm path) or
/// `pdu.reply(command_status="ESME_RSUBMITFAIL")` for reject.
#[pyclass(module = "siphon.smpp", name = "PduReply", from_py_object)]
#[derive(Debug, Clone)]
pub struct PduReply {
    pub command_status: SmppError,
    pub message_id: Option<String>,
}

#[pymethods]
impl PduReply {
    #[new]
    #[pyo3(signature = (*, command_status = "ESME_ROK".to_string(), message_id = None))]
    fn new(command_status: String, message_id: Option<String>) -> PyResult<Self> {
        Ok(Self {
            command_status: parse_smpp_status(&command_status)?,
            message_id,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "PduReply(command_status={:?}, message_id={:?})",
            self.command_status, self.message_id
        )
    }
}

impl PduReply {
    pub(crate) fn default_ok() -> Self {
        Self {
            command_status: SmppError::ESME_ROK,
            message_id: None,
        }
    }
}

// ── Bind ────────────────────────────────────────────────────────────────

/// Argument to `@smpp.on_bind`. Script returns truthy to accept,
/// falsy to reject. Default behaviour with no `@smpp.on_bind` handler
/// is reject (closed by default).
#[pyclass(module = "siphon.smpp", name = "Bind", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct Bind {
    #[pyo3(get)]
    pub system_id: String,
    #[pyo3(get)]
    pub password: String,
    #[pyo3(get)]
    pub client_addr: String,
}

#[pymethods]
impl Bind {
    /// Sugar for `return True`.
    fn accept(&self) -> bool {
        true
    }
    /// Sugar for `return False` (any non-truthy value works too).
    fn reject(&self) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "Bind(system_id={:?}, client_addr={:?})",
            self.system_id, self.client_addr
        )
    }
}

// ── Session ─────────────────────────────────────────────────────────────

/// Per-PDU context — which side delivered this PDU and which session.
#[pyclass(module = "siphon.smpp", name = "Session", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct Session {
    pub kind: SourceKind,
    #[pyo3(get)]
    pub session_id: String,
    #[pyo3(get)]
    pub system_id: String,
    #[pyo3(get)]
    pub client_addr: String,
}

#[pymethods]
impl Session {
    /// "bind" when the PDU arrived via an outbound bind
    /// (we bound to an aggregator), "esme" when an external client
    /// bound to our listener.
    #[getter]
    fn kind(&self) -> &'static str {
        match self.kind {
            SourceKind::Bind => "bind",
            SourceKind::EsmeServer => "esme",
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Session(kind={}, system_id={:?}, client_addr={:?})",
            self.kind(),
            self.system_id,
            self.client_addr
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SourceKind {
    Bind,
    EsmeServer,
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Map an SMPP status string ("ESME_ROK", "ESME_RSUBMITFAIL", …) to
/// the `smpp34::SmppError` enum. Returns a clean `PyValueError` on an
/// unknown spelling so script bugs surface immediately rather than
/// fall through to `ESME_ROK`.
fn parse_smpp_status(s: &str) -> PyResult<SmppError> {
    use SmppError::*;
    Ok(match s {
        "ESME_ROK" => ESME_ROK,
        "ESME_RINVMSGLEN" => ESME_RINVMSGLEN,
        "ESME_RINVCMDLEN" => ESME_RINVCMDLEN,
        "ESME_RINVCMDID" => ESME_RINVCMDID,
        "ESME_RINVBNDSTS" => ESME_RINVBNDSTS,
        "ESME_RALYBND" => ESME_RALYBND,
        "ESME_RINVPRTFLG" => ESME_RINVPRTFLG,
        "ESME_RINVREGDLVFLG" => ESME_RINVREGDLVFLG,
        "ESME_RSYSERR" => ESME_RSYSERR,
        "ESME_RINVSRCADR" => ESME_RINVSRCADR,
        "ESME_RINVDSTADR" => ESME_RINVDSTADR,
        "ESME_RINVMSGID" => ESME_RINVMSGID,
        "ESME_RBINDFAIL" => ESME_RBINDFAIL,
        "ESME_RINVPASWD" => ESME_RINVPASWD,
        "ESME_RINVSYSID" => ESME_RINVSYSID,
        "ESME_RCANCELFAIL" => ESME_RCANCELFAIL,
        "ESME_RREPLACEFAIL" => ESME_RREPLACEFAIL,
        "ESME_RMSGQFUL" => ESME_RMSGQFUL,
        "ESME_RINVSERTYP" => ESME_RINVSERTYP,
        "ESME_RINVNUMDESTS" => ESME_RINVNUMDESTS,
        "ESME_RINVDLNAME" => ESME_RINVDLNAME,
        "ESME_RINVDESTFLAG" => ESME_RINVDESTFLAG,
        "ESME_RINVSUBREP" => ESME_RINVSUBREP,
        "ESME_RINVESMCLASS" => ESME_RINVESMCLASS,
        "ESME_RCNTSUBDL" => ESME_RCNTSUBDL,
        "ESME_RSUBMITFAIL" => ESME_RSUBMITFAIL,
        "ESME_RINVSRCTON" => ESME_RINVSRCTON,
        "ESME_RINVSRCNPI" => ESME_RINVSRCNPI,
        "ESME_RINVDSTTON" => ESME_RINVDSTTON,
        "ESME_RINVDSTNPI" => ESME_RINVDSTNPI,
        "ESME_RINVSYSTYP" => ESME_RINVSYSTYP,
        "ESME_RINVREPFLAG" => ESME_RINVREPFLAG,
        "ESME_RINVNUMMSGS" => ESME_RINVNUMMSGS,
        "ESME_RTHROTTLED" => ESME_RTHROTTLED,
        "ESME_RINVSCHED" => ESME_RINVSCHED,
        "ESME_RINVEXPIRY" => ESME_RINVEXPIRY,
        "ESME_RINVDFTMSGID" => ESME_RINVDFTMSGID,
        "ESME_RX_T_APPN" => ESME_RX_T_APPN,
        "ESME_RX_P_APPN" => ESME_RX_P_APPN,
        "ESME_RX_R_APPN" => ESME_RX_R_APPN,
        "ESME_RQUERYFAIL" => ESME_RQUERYFAIL,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown SMPP status: {other:?}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests touch pyo3 types (PyResult / PyValueError) which need
    // an initialized interpreter. The crate enables pyo3's
    // `auto-initialize`, so `Python::attach` boots CPython on first use.

    fn make_pdu(esm_class: u8) -> Pdu {
        Pdu {
            command: "submit_sm".into(),
            service_type: String::new(),
            source_addr_ton: 1,
            source_addr_npi: 1,
            source_addr: "12345".into(),
            dest_addr_ton: 1,
            dest_addr_npi: 1,
            destination_addr: "67890".into(),
            esm_class,
            protocol_id: 0,
            priority_flag: 0,
            registered_delivery: 0,
            data_coding: 0,
            sm_length: 0,
            short_message: Vec::new(),
        }
    }

    #[test]
    fn parse_smpp_status_known_values() {
        Python::attach(|_py| {
            assert_eq!(parse_smpp_status("ESME_ROK").unwrap(), SmppError::ESME_ROK);
            assert_eq!(
                parse_smpp_status("ESME_RSUBMITFAIL").unwrap(),
                SmppError::ESME_RSUBMITFAIL
            );
            assert_eq!(
                parse_smpp_status("ESME_RINVDSTADR").unwrap(),
                SmppError::ESME_RINVDSTADR
            );
            assert_eq!(
                parse_smpp_status("ESME_RX_T_APPN").unwrap(),
                SmppError::ESME_RX_T_APPN
            );
        });
    }

    #[test]
    fn parse_smpp_status_unknown_is_value_error() {
        Python::attach(|py| {
            let err = parse_smpp_status("ESME_NOPE").unwrap_err();
            // A clean ValueError, not a silent fall-through to ESME_ROK.
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(err.to_string().contains("unknown SMPP status"));
        });
    }

    #[test]
    fn pdu_reply_default_ok() {
        let r = PduReply::default_ok();
        assert_eq!(r.command_status, SmppError::ESME_ROK);
        assert!(r.message_id.is_none());
    }

    #[test]
    fn pdu_reply_new_accept_with_message_id() {
        Python::attach(|_py| {
            let r = PduReply::new("ESME_ROK".to_string(), Some("msg-123".to_string()))
                .expect("ESME_ROK is valid");
            assert_eq!(r.command_status, SmppError::ESME_ROK);
            assert_eq!(r.message_id.as_deref(), Some("msg-123"));
        });
    }

    #[test]
    fn pdu_reply_new_reject_with_status() {
        Python::attach(|_py| {
            let r = PduReply::new("ESME_RSUBMITFAIL".to_string(), None)
                .expect("ESME_RSUBMITFAIL is valid");
            assert_eq!(r.command_status, SmppError::ESME_RSUBMITFAIL);
            assert!(r.message_id.is_none());
        });
    }

    #[test]
    fn pdu_reply_new_rejects_unknown_status() {
        Python::attach(|_py| {
            assert!(PduReply::new("ESME_BOGUS".to_string(), None).is_err());
        });
    }

    #[test]
    fn pdu_reply_default_signature_is_rok() {
        // The `#[new]` default args mirror `reply(...)`: no status given
        // means accept (ESME_ROK), no message_id.
        Python::attach(|_py| {
            let r = PduReply::new("ESME_ROK".to_string(), None).unwrap();
            assert_eq!(r.command_status, SmppError::ESME_ROK);
            assert!(r.message_id.is_none());
        });
    }

    #[test]
    fn pdu_reply_builds_via_pdu_reply_method() {
        Python::attach(|_py| {
            let pdu = make_pdu(0x00);
            // accept path
            let ok = pdu
                .reply("ESME_ROK".to_string(), Some("abc".to_string()))
                .unwrap();
            assert_eq!(ok.command_status, SmppError::ESME_ROK);
            assert_eq!(ok.message_id.as_deref(), Some("abc"));
            // reject path
            let rej = pdu.reply("ESME_RINVDSTADR".to_string(), None).unwrap();
            assert_eq!(rej.command_status, SmppError::ESME_RINVDSTADR);
        });
    }

    #[test]
    fn pdu_is_tpdu_reflects_udhi_bit() {
        // esm_class bit 0x40 = UDHI set → TPDU payload.
        assert!(make_pdu(0x40).is_tpdu());
        assert!(make_pdu(0xC0).is_tpdu()); // 0x40 set alongside other bits
        assert!(!make_pdu(0x00).is_tpdu());
        assert!(!make_pdu(0x01).is_tpdu()); // bit 0 set, 0x40 clear
    }

    #[test]
    fn bind_accept_reject_sugar() {
        let b = Bind {
            system_id: "esme1".into(),
            password: "pw".into(),
            client_addr: "203.0.113.5:5000".into(),
        };
        assert!(b.accept());
        assert!(!b.reject());
    }

    #[test]
    fn session_kind_renders_string() {
        let bind_sess = Session {
            kind: SourceKind::Bind,
            session_id: "s1".into(),
            system_id: "agg".into(),
            client_addr: "198.51.100.7:2775".into(),
        };
        assert_eq!(bind_sess.kind(), "bind");

        let esme_sess = Session {
            kind: SourceKind::EsmeServer,
            session_id: "s2".into(),
            system_id: "client".into(),
            client_addr: "198.51.100.8:1234".into(),
        };
        assert_eq!(esme_sess.kind(), "esme");
    }
}
