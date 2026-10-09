//! What came back for a request we originated.
//!
//! Every send helper puts one request on a session and waits. Three things
//! can happen, and a script has to be able to tell them apart:
//!
//! * the peer answered with the operation's own response PDU. Its
//!   `command_status` is the verdict (§3.2, §5.1.3) and is handed to the
//!   script unchanged, whatever it is — a response that arrives is **not**
//!   the same thing as an acceptance;
//! * the peer answered with a `generic_nack` (§4.3), which carries a
//!   status but is not the operation's response;
//! * nothing usable came back: the response timer ran out, or the session
//!   ended while the request was outstanding.
//!
//! The first is a return value ([`crate::sends::SmppResp`] /
//! [`crate::sends::QueryResp`]), falsy unless the status is `ESME_ROK`.
//! The rest raise [`SmppSendError`], because there is no response to
//! report and nothing a script could mistake for one.
//!
//! ## What is known exactly, and what is inferred
//!
//! `smpp34` returns `Ok(response)` for a response PDU of the right type
//! and `Err(SmppError)` for everything else. A `generic_nack` comes back
//! as its own status. A timeout, a session that closed, a failed socket
//! write and a response of the wrong type under our sequence number all
//! come back as the same `Err(ESME_RSYSERR)`, so those are told apart
//! here from what else is observable:
//!
//! * the codec's response timer cannot fire before the timer has elapsed,
//!   so an `ESME_RSYSERR` after at least that long is a timeout;
//! * the session is dropped from the runtime's registry (by the unbound
//!   hook) before the codec releases its outstanding requests, so an
//!   earlier `ESME_RSYSERR` on a session that is no longer registered is a
//!   close;
//! * an earlier `ESME_RSYSERR` on a session that is still registered is
//!   reported as [`SendFailure::Unanswered`] — most likely a `generic_nack`
//!   that itself carried `ESME_RSYSERR`, but not provably so.
//!
//! One more loss sits in the codec's `Err`: a `generic_nack` comes back as
//! an `SmppError`, so one carrying a status outside Table 5-2 arrives here
//! as `ESME_RUNKNOWNERR` with its number gone. Response PDUs do not have
//! that problem — their raw `command_status` is read directly.

use std::future::Future;
use std::time::{Duration, Instant};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use smpp34::{
    cancel_sm_resp, data_sm_resp, deliver_sm_resp, query_sm_resp, replace_sm_resp,
    submit_sm_multi_resp, submit_sm_resp, SmppError,
};

pyo3::create_exception!(
    siphon.smpp,
    SmppSendError,
    PyRuntimeError,
    "A send helper got no response PDU for its request.\n\n\
     Attributes:\n\
     \x20 reason: \"timeout\" (no response within the response timer),\n\
     \x20         \"closed\" (the session ended while the request was\n\
     \x20         outstanding), \"nack\" (the peer answered generic_nack) or\n\
     \x20         \"unanswered\" (the session reported a failure without a\n\
     \x20         response; see the docs).\n\
     \x20 command: the request, e.g. \"submit_sm\".\n\
     \x20 command_status: status name carried by a generic_nack, else None.\n\
     \x20 command_status_code: its numeric value, else None.\n\n\
     Subclasses RuntimeError, which is what these failures raised before."
);

/// `ESME_RTHROTTLED` (§5.1.3): the peer says we exceeded its message rate.
pub(crate) const ESME_RTHROTTLED: u32 = 0x0000_0058;
/// `ESME_RMSGQFUL` (§5.1.3): the peer's message queue is full.
pub(crate) const ESME_RMSGQFUL: u32 = 0x0000_0014;

/// The spec name of a `command_status` (Table 5-2), or the value in hex
/// for one outside it. §5.1.3 reserves ranges and leaves
/// `0x00000400`–`0x000004FF` to SMSC vendors, so a peer is entitled to
/// send a status with no name; inventing one would hide which it was.
pub(crate) fn status_name(status: u32) -> String {
    let known = SmppError::from_command_status(status);
    if known as u32 == status {
        format!("{known:?}")
    } else {
        format!("0x{status:08X}")
    }
}

/// True for the two statuses that ask the sender to slow down.
pub(crate) fn is_throttle(status: u32) -> bool {
    status == ESME_RTHROTTLED || status == ESME_RMSGQFUL
}

/// The parts of a response PDU every send helper reports.
pub(crate) trait Reply {
    /// `command_status` from the response header, as received.
    fn status(&self) -> u32;
    /// The SMSC-assigned id, when this response carries one and the
    /// request succeeded. A `submit_sm_resp` with a non-zero status has no
    /// body (§4.4.2), so there is nothing to report then.
    fn message_id(&self) -> String {
        String::new()
    }
}

impl Reply for submit_sm_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
    fn message_id(&self) -> String {
        self.message_id.clone().unwrap_or_default()
    }
}

impl Reply for submit_sm_multi_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
    fn message_id(&self) -> String {
        self.message_id.clone().unwrap_or_default()
    }
}

impl Reply for query_sm_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
    fn message_id(&self) -> String {
        self.message_id.clone()
    }
}

impl Reply for deliver_sm_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
}

impl Reply for data_sm_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
}

impl Reply for cancel_sm_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
}

impl Reply for replace_sm_resp {
    fn status(&self) -> u32 {
        self.command_status()
    }
}

/// Why a request produced no response PDU of its own.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SendFailure {
    /// The peer answered `generic_nack` with this status.
    Nack(SmppError),
    /// No response within the response timer.
    Timeout(Duration),
    /// The session ended while the request was outstanding.
    Closed,
    /// The session reported a failure without a response, on a session
    /// that is still up and before the response timer ran out.
    Unanswered,
}

impl SendFailure {
    /// The `reason` attribute a script reads off the exception.
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            SendFailure::Nack(_) => "nack",
            SendFailure::Timeout(_) => "timeout",
            SendFailure::Closed => "closed",
            SendFailure::Unanswered => "unanswered",
        }
    }

    fn describe(&self) -> String {
        match self {
            SendFailure::Nack(status) => format!(
                "peer answered generic_nack {status:?} (0x{:08X})",
                *status as u32
            ),
            SendFailure::Timeout(timer) => {
                format!("no response within {} ms", timer.as_millis())
            }
            SendFailure::Closed => "session closed before a response arrived".to_string(),
            SendFailure::Unanswered => "failed without a response (the session reported \
                 ESME_RSYSERR: a generic_nack carrying it, a response of another type, \
                 or a failed write)"
                .to_string(),
        }
    }

    /// Build the [`SmppSendError`] a send helper raises. `target` names
    /// the session (`bind "x"` / `session "y"`), `command` the request.
    pub(crate) fn into_pyerr(self, command: &str, target: &str) -> PyErr {
        let message = format!("{target} {command}: {}", self.describe());
        Python::attach(|py| {
            let error = PyErr::new::<SmppSendError, _>(message);
            let value = error.value(py);
            let annotated = (|| -> PyResult<()> {
                value.setattr("reason", self.reason())?;
                value.setattr("command", command)?;
                match self {
                    SendFailure::Nack(status) => {
                        value.setattr("command_status", format!("{status:?}"))?;
                        value.setattr("command_status_code", status as u32)?;
                    }
                    _ => {
                        value.setattr("command_status", py.None())?;
                        value.setattr("command_status_code", py.None())?;
                    }
                }
                Ok(())
            })();
            match annotated {
                Ok(()) => error,
                // Could not attach the attributes; raise that instead of an
                // exception whose `reason` would read as missing.
                Err(attach_error) => attach_error,
            }
        })
    }
}

/// Decide what an `Err` from a `smpp34` send call means. See the module
/// docs for why each branch is sound.
pub(crate) fn classify(
    error: SmppError,
    waited: Duration,
    response_timer: Duration,
    session_gone: bool,
) -> SendFailure {
    if error != SmppError::ESME_RSYSERR {
        return SendFailure::Nack(error);
    }
    if waited >= response_timer {
        SendFailure::Timeout(response_timer)
    } else if session_gone {
        SendFailure::Closed
    } else {
        SendFailure::Unanswered
    }
}

/// Await a `smpp34` send call and report what came back: the response PDU,
/// whatever its status, or the reason there was none. `session_gone` is
/// only polled on failure.
pub(crate) async fn settle<R>(
    send: impl Future<Output = Result<R, SmppError>>,
    response_timer: Duration,
    session_gone: impl Future<Output = bool>,
) -> Result<R, SendFailure> {
    let started = Instant::now();
    match send.await {
        Ok(response) => Ok(response),
        Err(error) => {
            let waited = started.elapsed();
            Err(classify(error, waited, response_timer, session_gone.await))
        }
    }
}

/// Table 5-2 of SMPP 3.4, copied from the specification rather than
/// from the codec's enum, so a wrong value on either side shows up.
#[cfg(test)]
pub(crate) const TABLE_5_2: &[(&str, u32)] = &[
    ("ESME_ROK", 0x0000_0000),
    ("ESME_RINVMSGLEN", 0x0000_0001),
    ("ESME_RINVCMDLEN", 0x0000_0002),
    ("ESME_RINVCMDID", 0x0000_0003),
    ("ESME_RINVBNDSTS", 0x0000_0004),
    ("ESME_RALYBND", 0x0000_0005),
    ("ESME_RINVPRTFLG", 0x0000_0006),
    ("ESME_RINVREGDLVFLG", 0x0000_0007),
    ("ESME_RSYSERR", 0x0000_0008),
    ("ESME_RINVSRCADR", 0x0000_000A),
    ("ESME_RINVDSTADR", 0x0000_000B),
    ("ESME_RINVMSGID", 0x0000_000C),
    ("ESME_RBINDFAIL", 0x0000_000D),
    ("ESME_RINVPASWD", 0x0000_000E),
    ("ESME_RINVSYSID", 0x0000_000F),
    ("ESME_RCANCELFAIL", 0x0000_0011),
    ("ESME_RREPLACEFAIL", 0x0000_0013),
    ("ESME_RMSGQFUL", 0x0000_0014),
    ("ESME_RINVSERTYP", 0x0000_0015),
    ("ESME_RINVNUMDESTS", 0x0000_0033),
    ("ESME_RINVDLNAME", 0x0000_0034),
    ("ESME_RINVDESTFLAG", 0x0000_0040),
    ("ESME_RINVSUBREP", 0x0000_0042),
    ("ESME_RINVESMCLASS", 0x0000_0043),
    ("ESME_RCNTSUBDL", 0x0000_0044),
    ("ESME_RSUBMITFAIL", 0x0000_0045),
    ("ESME_RINVSRCTON", 0x0000_0048),
    ("ESME_RINVSRCNPI", 0x0000_0049),
    ("ESME_RINVDSTTON", 0x0000_0050),
    ("ESME_RINVDSTNPI", 0x0000_0051),
    ("ESME_RINVSYSTYP", 0x0000_0053),
    ("ESME_RINVREPFLAG", 0x0000_0054),
    ("ESME_RINVNUMMSGS", 0x0000_0055),
    ("ESME_RTHROTTLED", 0x0000_0058),
    ("ESME_RINVSCHED", 0x0000_0061),
    ("ESME_RINVEXPIRY", 0x0000_0062),
    ("ESME_RINVDFTMSGID", 0x0000_0063),
    ("ESME_RX_T_APPN", 0x0000_0064),
    ("ESME_RX_P_APPN", 0x0000_0065),
    ("ESME_RX_R_APPN", 0x0000_0066),
    ("ESME_RQUERYFAIL", 0x0000_0067),
    ("ESME_RINVOPTPARSTREAM", 0x0000_00C0),
    ("ESME_ROPTPARNOTALLWD", 0x0000_00C1),
    ("ESME_RINVPARLEN", 0x0000_00C2),
    ("ESME_RMISSINGOPTPARAM", 0x0000_00C3),
    ("ESME_RINVOPTPARAMVAL", 0x0000_00C4),
    ("ESME_RDELIVERYFAILURE", 0x0000_00FE),
    ("ESME_RUNKNOWNERR", 0x0000_00FF),
];

#[cfg(test)]
mod tests {
    use super::*;

    const TIMER: Duration = Duration::from_secs(30);

    #[test]
    fn every_table_5_2_status_is_named_as_the_specification_names_it() {
        for (name, code) in TABLE_5_2 {
            assert_eq!(status_name(*code), *name, "0x{code:08X}");
        }
    }

    #[test]
    fn a_status_outside_the_table_keeps_its_value_instead_of_borrowing_a_name() {
        // Vendor range (§5.1.3) and a reserved value.
        assert_eq!(status_name(0x0000_0401), "0x00000401");
        assert_eq!(status_name(0x0000_0009), "0x00000009");
        // Not to be confused with the real ESME_RUNKNOWNERR.
        assert_eq!(status_name(0x0000_00FF), "ESME_RUNKNOWNERR");
    }

    #[test]
    fn only_the_two_back_off_statuses_count_as_throttling() {
        for (name, code) in TABLE_5_2 {
            let expected = matches!(*name, "ESME_RTHROTTLED" | "ESME_RMSGQFUL");
            assert_eq!(is_throttle(*code), expected, "{name}");
        }
    }

    #[test]
    fn a_generic_nack_status_is_reported_as_the_nack_it_was() {
        let got = classify(SmppError::ESME_RINVCMDID, Duration::ZERO, TIMER, false);
        assert_eq!(got, SendFailure::Nack(SmppError::ESME_RINVCMDID));
        // The elapsed time and the registry do not turn a nack into
        // something else.
        let got = classify(SmppError::ESME_RTHROTTLED, TIMER, TIMER, true);
        assert_eq!(got, SendFailure::Nack(SmppError::ESME_RTHROTTLED));
    }

    #[test]
    fn a_failure_after_the_response_timer_is_a_timeout() {
        assert_eq!(
            classify(SmppError::ESME_RSYSERR, TIMER, TIMER, false),
            SendFailure::Timeout(TIMER)
        );
        // A timed-out request usually takes the session down with it; it
        // is still the timeout that the script should hear about.
        assert_eq!(
            classify(
                SmppError::ESME_RSYSERR,
                TIMER + Duration::from_millis(3),
                TIMER,
                true
            ),
            SendFailure::Timeout(TIMER)
        );
    }

    #[test]
    fn an_early_failure_on_a_session_that_is_gone_is_a_close() {
        assert_eq!(
            classify(
                SmppError::ESME_RSYSERR,
                Duration::from_millis(20),
                TIMER,
                true
            ),
            SendFailure::Closed
        );
    }

    #[test]
    fn an_early_failure_on_a_live_session_is_not_given_a_cause_it_may_not_have() {
        assert_eq!(
            classify(
                SmppError::ESME_RSYSERR,
                Duration::from_millis(20),
                TIMER,
                false
            ),
            SendFailure::Unanswered
        );
    }

    #[test]
    fn the_exception_carries_the_reason_and_the_nack_status() {
        Python::attach(|py| {
            let error = SendFailure::Nack(SmppError::ESME_RINVCMDLEN)
                .into_pyerr("submit_sm", "bind \"upstream\"");
            // What these failures raised before, so an existing
            // `except RuntimeError` still catches them.
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(error.is_instance_of::<SmppSendError>(py));
            let value = error.value(py);
            let text = |name: &str| -> String {
                value
                    .getattr(name)
                    .and_then(|v| v.extract())
                    .expect("attribute")
            };
            assert_eq!(text("reason"), "nack");
            assert_eq!(text("command"), "submit_sm");
            assert_eq!(text("command_status"), "ESME_RINVCMDLEN");
            let code: u32 = value
                .getattr("command_status_code")
                .and_then(|v| v.extract())
                .expect("code");
            assert_eq!(code, 0x0000_0002);
            assert!(error.to_string().contains("generic_nack ESME_RINVCMDLEN"));
        });
    }

    #[test]
    fn a_failure_with_no_response_has_no_status_to_report() {
        Python::attach(|py| {
            for (failure, reason) in [
                (SendFailure::Timeout(TIMER), "timeout"),
                (SendFailure::Closed, "closed"),
                (SendFailure::Unanswered, "unanswered"),
            ] {
                let error = failure.into_pyerr("deliver_sm", "session \"abc\"");
                let value = error.value(py);
                let got: String = value
                    .getattr("reason")
                    .and_then(|v| v.extract())
                    .expect("reason");
                assert_eq!(got, reason);
                assert!(value.getattr("command_status").expect("attr").is_none());
                assert!(value
                    .getattr("command_status_code")
                    .expect("attr")
                    .is_none());
            }
            let timeout = SendFailure::Timeout(TIMER).into_pyerr("deliver_sm", "session \"abc\"");
            assert!(timeout.to_string().contains("no response within 30000 ms"));
        });
    }
}
