//! Pyclasses surfaced to scripts at handler-dispatch time.
//!
//! The runtime constructs these from inbound PDUs and passes them into
//! the script via `ScriptHandle::call_handler`. Scripts read fields,
//! call `pdu.reply(...)` to produce a [`PduReply`], call
//! `bind.accept()` / `bind.reject("ESME_RINVPASWD", "bad password")`
//! to authorise a bind (with an explicit reason).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use smpp34::{
    alert_notification, cancel_sm, deliver_sm, query_sm, replace_sm, submit_sm, SmppError,
};

// ── Pdu ─────────────────────────────────────────────────────────────────

/// Common surface for `submit_sm`, `deliver_sm`, `data_sm`, `cancel_sm`,
/// `query_sm` and `replace_sm`. Field names mirror SMPP 3.4 §5.2; same
/// shape on every direction so the routing layer can treat them
/// uniformly. The `command` field tells you which op produced it; not
/// every field is meaningful for every command (e.g. `message_id` is set
/// only for cancel/query/replace).
#[pyclass(module = "siphon.smpp", name = "Pdu", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct Pdu {
    #[pyo3(get)]
    pub command: String,
    /// SMSC-assigned message id — set for `cancel_sm` / `query_sm` /
    /// `replace_sm` (which target a prior submission); empty for
    /// `submit_sm` / `deliver_sm` / `data_sm`.
    #[pyo3(get)]
    pub message_id: String,
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

    /// True when this `deliver_sm` is a **delivery receipt** (DLR) — the
    /// `esm_class` message-type bits (0x04) flag it as an SMSC delivery
    /// receipt. Route these back to the ESME that originally requested
    /// `registered_delivery`. See [`receipt`](Self::receipt) for the
    /// parsed fields.
    #[getter]
    fn is_dlr(&self) -> bool {
        self.esm_class & 0x04 != 0
    }

    /// Parsed delivery-receipt fields, or `None` when this PDU is not a
    /// DLR / the body doesn't follow the de-facto receipt format.
    ///
    /// Returns a dict with the keys that were present:
    /// `id`, `sub`, `dlvrd`, `submit_date`, `done_date`, `stat`, `err`,
    /// `text`, plus `raw` (the undecoded receipt body). The format is
    /// not standardised across SMSCs, so this is best-effort — always
    /// keep `raw` as the source of truth.
    #[getter]
    fn receipt<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyDict>> {
        if !self.is_dlr() {
            return None;
        }
        let parsed = Receipt::parse(&self.short_message)?;
        let d = PyDict::new(py);
        // set_item on a fresh dict can't fail in practice; ignore the
        // Result to keep the getter infallible.
        let _ = parsed.fill_dict(&d);
        Some(d)
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
            message_state: None,
            final_date: String::new(),
            error_code: 0,
        })
    }

    /// Build a successful `query_sm_resp` reply. `message_state` is the
    /// SMPP message-state code (1=ENROUTE, 2=DELIVERED, 3=EXPIRED,
    /// 4=DELETED, 5=UNDELIVERABLE, 6=ACCEPTED, 7=UNKNOWN, 8=REJECTED).
    /// `final_date` is the SMPP-format absolute time (or empty if not
    /// final), `error_code` the network error code. To reject a query,
    /// use `pdu.reply(command_status="ESME_RQUERYFAIL")` instead.
    #[pyo3(signature = (*, message_state, message_id = None, final_date = String::new(), error_code = 0))]
    fn reply_query(
        &self,
        message_state: u8,
        message_id: Option<String>,
        final_date: String,
        error_code: u8,
    ) -> PduReply {
        PduReply {
            command_status: SmppError::ESME_ROK,
            // default the echoed id to the queried message_id
            message_id: message_id.or_else(|| Some(self.message_id.clone())),
            message_state: Some(message_state),
            final_date,
            error_code,
        }
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
    pub fn from_submit(s: &submit_sm) -> Self {
        Self {
            command: "submit_sm".into(),
            message_id: String::new(),
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

    pub fn from_deliver(d: &deliver_sm) -> Self {
        Self {
            command: "deliver_sm".into(),
            message_id: String::new(),
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

    /// Build a `Pdu` from a `data_sm`. `data_sm` carries its payload in
    /// the `message_payload` TLV rather than `short_message`, so the
    /// body is empty here; the addressing + coding fields are the useful
    /// surface for routing.
    pub fn from_data(d: &smpp34::data_sm) -> Self {
        Self {
            command: "data_sm".into(),
            message_id: String::new(),
            service_type: d.service_type.clone(),
            source_addr_ton: d.source_addr_ton,
            source_addr_npi: d.source_addr_npi,
            source_addr: d.source_addr.clone(),
            dest_addr_ton: d.dest_addr_ton,
            dest_addr_npi: d.dest_addr_npi,
            destination_addr: d.destination_addr.clone(),
            esm_class: d.esm_class,
            protocol_id: 0,
            priority_flag: 0,
            registered_delivery: d.registered_delivery,
            data_coding: d.data_coding,
            sm_length: 0,
            short_message: Vec::new(),
        }
    }

    /// Build a `Pdu` from an inbound `cancel_sm` — `message_id` +
    /// addressing identify the message(s) to cancel.
    pub fn from_cancel(c: &cancel_sm) -> Self {
        Self {
            command: "cancel_sm".into(),
            message_id: c.message_id.clone(),
            service_type: c.service_type.clone(),
            source_addr_ton: c.source_addr_ton,
            source_addr_npi: c.source_addr_npi,
            source_addr: c.source_addr.clone(),
            dest_addr_ton: c.dest_addr_ton,
            dest_addr_npi: c.dest_addr_npi,
            destination_addr: c.destination_addr.clone(),
            esm_class: 0,
            protocol_id: 0,
            priority_flag: 0,
            registered_delivery: 0,
            data_coding: 0,
            sm_length: 0,
            short_message: Vec::new(),
        }
    }

    /// Build a `Pdu` from an inbound `query_sm` — `message_id` + source
    /// address identify the message whose state is being queried. Reply
    /// with [`reply_query`](Self::reply_query).
    pub fn from_query(q: &query_sm) -> Self {
        Self {
            command: "query_sm".into(),
            message_id: q.message_id.clone(),
            service_type: String::new(),
            source_addr_ton: q.source_addr_ton,
            source_addr_npi: q.source_addr_npi,
            source_addr: q.source_addr.clone(),
            dest_addr_ton: 0,
            dest_addr_npi: 0,
            destination_addr: String::new(),
            esm_class: 0,
            protocol_id: 0,
            priority_flag: 0,
            registered_delivery: 0,
            data_coding: 0,
            sm_length: 0,
            short_message: Vec::new(),
        }
    }

    /// Build a `Pdu` from an inbound `replace_sm` — `message_id` + source
    /// address identify the message to replace; `short_message` is the new
    /// body. (`schedule_delivery_time` / `validity_period` are carried on
    /// the wire but not surfaced on `Pdu`.)
    pub fn from_replace(r: &replace_sm) -> Self {
        Self {
            command: "replace_sm".into(),
            message_id: r.message_id.clone(),
            service_type: String::new(),
            source_addr_ton: r.source_addr_ton,
            source_addr_npi: r.source_addr_npi,
            source_addr: r.source_addr.clone(),
            dest_addr_ton: 0,
            dest_addr_npi: 0,
            destination_addr: String::new(),
            esm_class: 0,
            protocol_id: 0,
            priority_flag: 0,
            registered_delivery: r.registered_delivery,
            data_coding: 0,
            sm_length: r.sm_length,
            short_message: r.short_message.clone(),
        }
    }
}

// ── Delivery-receipt parser ─────────────────────────────────────────────

/// Best-effort parse of the de-facto SMSC delivery-receipt body, e.g.
/// `id:0a1b2 sub:001 dlvrd:001 submit date:2401011200 done
/// date:2401011201 stat:DELIVRD err:000 text:Hello`.
///
/// The format is not standardised (it predates any SMPP version that
/// tried to formalise it), so the parser recognises the canonical key
/// set and tolerates missing keys / extra whitespace. The two-word keys
/// (`submit date`, `done date`) are handled explicitly. `text` always
/// runs to the end of the body.
#[derive(Debug, Default, PartialEq)]
pub struct Receipt {
    pub id: Option<String>,
    pub sub: Option<String>,
    pub dlvrd: Option<String>,
    pub submit_date: Option<String>,
    pub done_date: Option<String>,
    pub stat: Option<String>,
    pub err: Option<String>,
    pub text: Option<String>,
    pub raw: String,
}

impl Receipt {
    /// Canonical receipt keys, longest-first so `submit date` is matched
    /// before a hypothetical `submit`. The output field name is the
    /// snake_case form exposed to Python.
    const KEYS: &'static [(&'static str, &'static str)] = &[
        ("submit date", "submit_date"),
        ("done date", "done_date"),
        ("dlvrd", "dlvrd"),
        ("stat", "stat"),
        ("text", "text"),
        ("sub", "sub"),
        ("err", "err"),
        ("id", "id"),
    ];

    pub fn parse(sm: &[u8]) -> Option<Receipt> {
        let raw = String::from_utf8_lossy(sm).into_owned();
        let hay = raw.to_ascii_lowercase();

        // Locate every `<key>:` occurrence; record (byte_pos, key_len,
        // output_field). A field is found at most once (first wins).
        let mut hits: Vec<(usize, usize, &'static str)> = Vec::new();
        for (key, field) in Self::KEYS {
            let needle = format!("{key}:");
            if let Some(pos) = hay.find(&needle) {
                // Skip if this position is already claimed by a longer
                // key (e.g. the `date:` inside `submit date:`).
                if hits.iter().any(|(p, l, _)| pos >= *p && pos < p + l) {
                    continue;
                }
                hits.push((pos, needle.len(), field));
            }
        }
        if hits.is_empty() {
            return None;
        }
        hits.sort_by_key(|(p, _, _)| *p);

        let mut out = Receipt {
            raw: raw.clone(),
            ..Default::default()
        };
        for (i, (pos, key_len, field)) in hits.iter().enumerate() {
            let val_start = pos + key_len;
            let val_end = hits.get(i + 1).map(|(p, _, _)| *p).unwrap_or(raw.len());
            let value = raw[val_start..val_end].trim().to_string();
            out.set(field, value);
        }
        Some(out)
    }

    fn set(&mut self, field: &str, value: String) {
        let slot = match field {
            "id" => &mut self.id,
            "sub" => &mut self.sub,
            "dlvrd" => &mut self.dlvrd,
            "submit_date" => &mut self.submit_date,
            "done_date" => &mut self.done_date,
            "stat" => &mut self.stat,
            "err" => &mut self.err,
            "text" => &mut self.text,
            _ => return,
        };
        *slot = Some(value);
    }

    fn fill_dict(&self, d: &Bound<'_, PyDict>) -> PyResult<()> {
        macro_rules! put {
            ($k:literal, $v:expr) => {
                if let Some(v) = &$v {
                    d.set_item($k, v)?;
                }
            };
        }
        put!("id", self.id);
        put!("sub", self.sub);
        put!("dlvrd", self.dlvrd);
        put!("submit_date", self.submit_date);
        put!("done_date", self.done_date);
        put!("stat", self.stat);
        put!("err", self.err);
        put!("text", self.text);
        d.set_item("raw", &self.raw)?;
        Ok(())
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
    /// query_sm path only — set by [`Pdu::reply_query`].
    pub message_state: Option<u8>,
    pub final_date: String,
    pub error_code: u8,
}

#[pymethods]
impl PduReply {
    #[new]
    #[pyo3(signature = (*, command_status = "ESME_ROK".to_string(), message_id = None))]
    fn new(command_status: String, message_id: Option<String>) -> PyResult<Self> {
        Ok(Self {
            command_status: parse_smpp_status(&command_status)?,
            message_id,
            message_state: None,
            final_date: String::new(),
            error_code: 0,
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
            message_state: None,
            final_date: String::new(),
            error_code: 0,
        }
    }
}

// ── Bind / BindResult ───────────────────────────────────────────────────

/// Argument to `@smpp.on_bind`. The handler authorises the bind by
/// returning `bind.accept()` or `bind.reject("ESME_RINVPASWD", "why")`.
/// Bare truthy/falsy returns still work (truthy = accept, falsy/None =
/// reject). With no `@smpp.on_bind` handler the default is reject —
/// binds are closed by default.
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
    /// Accept the bind. `return bind.accept()`.
    fn accept(&self) -> BindResult {
        BindResult {
            accept: true,
            status: SmppError::ESME_ROK,
            reason: String::new(),
        }
    }

    /// Reject the bind with an explicit SMPP status and an operator-facing
    /// reason (logged on the reject). Defaults: `ESME_RBINDFAIL`, no
    /// reason. Common statuses: `ESME_RINVPASWD` (bad password),
    /// `ESME_RINVSYSID` (unknown system_id), `ESME_RBINDFAIL` (generic),
    /// `ESME_RTHROTTLED` (rate-limited).
    #[pyo3(signature = (status = "ESME_RBINDFAIL".to_string(), reason = String::new()))]
    fn reject(&self, status: String, reason: String) -> PyResult<BindResult> {
        Ok(BindResult {
            accept: false,
            status: parse_smpp_status(&status)?,
            reason,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Bind(system_id={:?}, client_addr={:?})",
            self.system_id, self.client_addr
        )
    }
}

/// Outcome of `@smpp.on_bind` — what `bind.accept()` / `bind.reject(...)`
/// return. The runtime reads `accept`, and on reject maps `status` onto
/// the wire `bind_*_resp` and logs `reason`.
#[pyclass(module = "siphon.smpp", name = "BindResult", from_py_object)]
#[derive(Debug, Clone)]
pub struct BindResult {
    pub accept: bool,
    pub status: SmppError,
    pub reason: String,
}

#[pymethods]
impl BindResult {
    fn __bool__(&self) -> bool {
        self.accept
    }

    fn __repr__(&self) -> String {
        if self.accept {
            "BindResult(accept)".to_string()
        } else {
            format!(
                "BindResult(reject, status={:?}, reason={:?})",
                self.status, self.reason
            )
        }
    }
}

// ── AlertNotification ───────────────────────────────────────────────────

/// Payload for `@smpp.on_pdu("alert_notification")` — an SMSC telling us
/// (on an outbound bind) that a previously-unavailable MS is now
/// reachable, so queued MT can be flushed. `source_addr` is the MS,
/// `esme_addr` the ESME the alert is destined for, and
/// `ms_availability_status` the availability state (0=available,
/// 1=denied, 2=unavailable) when present.
#[pyclass(
    module = "siphon.smpp",
    name = "AlertNotification",
    skip_from_py_object
)]
#[derive(Debug, Clone, Default)]
pub struct AlertNotification {
    #[pyo3(get)]
    pub source_addr: String,
    #[pyo3(get)]
    pub esme_addr: String,
    #[pyo3(get)]
    pub ms_availability_status: Option<u8>,
}

impl AlertNotification {
    pub fn from_alert(a: &alert_notification) -> Self {
        Self {
            source_addr: a.source_addr.clone(),
            esme_addr: a.esme_addr.clone(),
            ms_availability_status: a.ms_availability_status,
        }
    }
}

#[pymethods]
impl AlertNotification {
    #[getter]
    fn command(&self) -> &'static str {
        "alert_notification"
    }

    fn __repr__(&self) -> String {
        format!(
            "AlertNotification(source_addr={:?}, esme_addr={:?}, ms_availability_status={:?})",
            self.source_addr, self.esme_addr, self.ms_availability_status
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
            "Session(kind={}, system_id={:?}, session_id={:?}, client_addr={:?})",
            self.kind(),
            self.system_id,
            self.session_id,
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
pub(crate) fn parse_smpp_status(s: &str) -> PyResult<SmppError> {
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
            message_id: String::new(),
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
    fn pdu_reply_query_builds_query_reply() {
        Python::attach(|_py| {
            let mut pdu = make_pdu(0x00);
            pdu.command = "query_sm".into();
            pdu.message_id = "msg-42".into();
            // message_state required; message_id defaults to the queried id.
            let r = pdu.reply_query(2, None, "2401011200".to_string(), 0);
            assert_eq!(r.command_status, SmppError::ESME_ROK);
            assert_eq!(r.message_state, Some(2));
            assert_eq!(r.message_id.as_deref(), Some("msg-42"));
            assert_eq!(r.final_date, "2401011200");
            // explicit message_id override
            let r2 = pdu.reply_query(7, Some("other".to_string()), String::new(), 9);
            assert_eq!(r2.message_id.as_deref(), Some("other"));
            assert_eq!(r2.error_code, 9);
        });
    }

    #[test]
    fn pdu_from_query_maps_fields() {
        let q = smpp34::query_sm::new(1, "qid-7".to_string(), 1, 1, "15550101".to_string());
        let pdu = Pdu::from_query(&q);
        assert_eq!(pdu.command, "query_sm");
        assert_eq!(pdu.message_id, "qid-7");
        assert_eq!(pdu.source_addr, "15550101");
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
    fn pdu_is_dlr_reflects_receipt_bit() {
        // esm_class bit 0x04 = SMSC delivery receipt.
        assert!(make_pdu(0x04).is_dlr());
        assert!(make_pdu(0x44).is_dlr()); // 0x04 alongside UDHI
        assert!(!make_pdu(0x00).is_dlr());
        assert!(!make_pdu(0x40).is_dlr()); // UDHI but not a receipt
    }

    #[test]
    fn receipt_parses_canonical_form() {
        let body = b"id:0a1b2c3d sub:001 dlvrd:001 submit date:2401011200 \
                     done date:2401011201 stat:DELIVRD err:000 text:Hello world";
        let r = Receipt::parse(body).expect("a canonical receipt parses");
        assert_eq!(r.id.as_deref(), Some("0a1b2c3d"));
        assert_eq!(r.sub.as_deref(), Some("001"));
        assert_eq!(r.dlvrd.as_deref(), Some("001"));
        assert_eq!(r.submit_date.as_deref(), Some("2401011200"));
        assert_eq!(r.done_date.as_deref(), Some("2401011201"));
        assert_eq!(r.stat.as_deref(), Some("DELIVRD"));
        assert_eq!(r.err.as_deref(), Some("000"));
        assert_eq!(r.text.as_deref(), Some("Hello world"));
    }

    #[test]
    fn receipt_tolerates_missing_keys() {
        // A minimal receipt — just id + stat — still parses.
        let r = Receipt::parse(b"id:XYZ stat:EXPIRED").expect("minimal receipt parses");
        assert_eq!(r.id.as_deref(), Some("XYZ"));
        assert_eq!(r.stat.as_deref(), Some("EXPIRED"));
        assert!(r.sub.is_none());
        assert!(r.text.is_none());
    }

    #[test]
    fn receipt_non_receipt_body_is_none() {
        // A normal MO message with no key:value structure → not a receipt.
        assert!(Receipt::parse(b"hey are we still on for lunch?").is_none());
    }

    #[test]
    fn receipt_getter_only_fires_for_dlr() {
        Python::attach(|py| {
            // is_dlr false → receipt() is None even if the body looks
            // receipt-shaped.
            let mut pdu = make_pdu(0x00);
            pdu.short_message = b"id:1 stat:DELIVRD".to_vec();
            assert!(pdu.receipt(py).is_none());

            // is_dlr true + receipt body → dict with the parsed fields.
            let mut dlr = make_pdu(0x04);
            dlr.short_message = b"id:1 stat:DELIVRD err:000".to_vec();
            let d = dlr.receipt(py).expect("dlr with body yields a dict");
            let id: String = d.get_item("id").unwrap().unwrap().extract().unwrap();
            assert_eq!(id, "1");
            let stat: String = d.get_item("stat").unwrap().unwrap().extract().unwrap();
            assert_eq!(stat, "DELIVRD");
        });
    }

    #[test]
    fn bind_accept_yields_accept_result() {
        let b = Bind {
            system_id: "esme1".into(),
            password: "pw".into(),
            client_addr: "203.0.113.5:5000".into(),
        };
        let r = b.accept();
        assert!(r.accept);
        assert_eq!(r.status, SmppError::ESME_ROK);
    }

    #[test]
    fn bind_reject_carries_status_and_reason() {
        Python::attach(|_py| {
            let b = Bind {
                system_id: "esme1".into(),
                password: "pw".into(),
                client_addr: "203.0.113.5:5000".into(),
            };
            // explicit status + reason
            let r = b
                .reject("ESME_RINVPASWD".to_string(), "bad password".to_string())
                .unwrap();
            assert!(!r.accept);
            assert_eq!(r.status, SmppError::ESME_RINVPASWD);
            assert_eq!(r.reason, "bad password");

            // defaults: generic bind failure, no reason
            let d = b
                .reject("ESME_RBINDFAIL".to_string(), String::new())
                .unwrap();
            assert_eq!(d.status, SmppError::ESME_RBINDFAIL);
            assert_eq!(d.reason, "");
        });
    }

    #[test]
    fn bind_reject_unknown_status_errors() {
        Python::attach(|_py| {
            let b = Bind {
                system_id: "x".into(),
                password: "y".into(),
                client_addr: "z".into(),
            };
            assert!(b.reject("ESME_BOGUS".to_string(), String::new()).is_err());
        });
    }

    #[test]
    fn bind_result_bool_reflects_accept() {
        let yes = BindResult {
            accept: true,
            status: SmppError::ESME_ROK,
            reason: String::new(),
        };
        let no = BindResult {
            accept: false,
            status: SmppError::ESME_RBINDFAIL,
            reason: "nope".into(),
        };
        assert!(yes.__bool__());
        assert!(!no.__bool__());
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
