//! TLV (SMPP 3.4 **optional parameter**, §3.2.1 / §5.3.2) conversion between
//! the Python script surface and `smpp34`'s [`Tlv`].
//!
//! Scripts address optional parameters with a plain dict, keyed by the spec
//! name or by a raw `u16` tag for vendor-defined ones:
//!
//! ```python
//! tlvs={"MESSAGE_PAYLOAD": b"...", "SAR_MSG_REF_NUM": 42, 0x1400: b"\x01"}
//! ```
//!
//! The names come straight from `smpp34`'s [`TlvTag::ALL`], so a name that
//! works here is exactly a tag the codec knows.
//!
//! **Integer values take their width from the spec, never from the value.**
//! `message_state` is one octet and `sar_msg_ref_num` is two whatever integer
//! you hand in; a tag that isn't integer-typed rejects an `int` outright rather
//! than guessing a width and putting a malformed TLV on the wire.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyInt, PyString};

use smpp34::{Tlv, TlvTag};

/// Integer-typed optional parameters and their fixed on-the-wire width in
/// octets (§5.3.2). Every other tag is an octet string / C-octet string (or
/// `alert_on_message_delivery`, which carries no value at all) and only accepts
/// `bytes` / `str` from a script.
const INT_WIDTHS: &[(TlvTag, usize)] = &[
    (TlvTag::DestAddrSubunit, 1),
    (TlvTag::DestNetworkType, 1),
    (TlvTag::DestBearerType, 1),
    (TlvTag::DestTelematicsId, 2),
    (TlvTag::SourceAddrSubunit, 1),
    (TlvTag::SourceNetworkType, 1),
    (TlvTag::SourceBearerType, 1),
    (TlvTag::SourceTelematicsId, 1),
    (TlvTag::QosTimeToLive, 4),
    (TlvTag::PayloadType, 1),
    (TlvTag::MsMsgWaitFacilities, 1),
    (TlvTag::PrivacyIndicator, 1),
    (TlvTag::UserMessageReference, 2),
    (TlvTag::UserResponseCode, 1),
    (TlvTag::SourcePort, 2),
    (TlvTag::DestinationPort, 2),
    (TlvTag::SarMsgRefNum, 2),
    (TlvTag::LanguageIndicator, 1),
    (TlvTag::SarTotalSegments, 1),
    (TlvTag::SarSegmentSeqnum, 1),
    (TlvTag::ScInterfaceVersion, 1),
    (TlvTag::CallbackNumPresInd, 1),
    (TlvTag::NumberOfMessages, 1),
    (TlvTag::DpfResult, 1),
    (TlvTag::SetDpf, 1),
    (TlvTag::MsAvailabilityStatus, 1),
    (TlvTag::DeliveryFailureReason, 1),
    (TlvTag::MoreMessagesToSend, 1),
    (TlvTag::MessageStateTlv, 1),
    (TlvTag::UssdServiceOp, 1),
    (TlvTag::DisplayTime, 1),
    (TlvTag::SmsSignal, 2),
    (TlvTag::MsValidity, 1),
    (TlvTag::ItsReplyType, 1),
];

/// Spec width for an integer-typed tag, or `None` when the tag isn't one.
fn int_width(tag: u16) -> Option<usize> {
    INT_WIDTHS
        .iter()
        .find(|(t, _)| *t as u16 == tag)
        .map(|(_, w)| *w)
}

/// Resolve a spec name to its tag. Case-insensitive, and an optional `TLV_`
/// prefix is accepted so the smpp34 Python constant names work verbatim.
pub fn tag_from_name(name: &str) -> Option<u16> {
    let wanted = name.trim().to_ascii_uppercase();
    let wanted = wanted.strip_prefix("TLV_").unwrap_or(&wanted);
    TlvTag::ALL
        .iter()
        .find(|(n, _)| *n == wanted)
        .map(|(_, t)| *t as u16)
}

/// The spec name for a tag, when it is one of the 44 standard ones.
pub fn name_for_tag(tag: u16) -> Option<&'static str> {
    TlvTag::ALL
        .iter()
        .find(|(_, t)| *t as u16 == tag)
        .map(|(n, _)| *n)
}

/// A dict key — spec name (`"MESSAGE_PAYLOAD"`) or raw tag (`0x1400`).
pub fn tag_from_py(key: &Bound<'_, PyAny>) -> PyResult<u16> {
    if let Ok(name) = key.cast::<PyString>() {
        let name = name.to_cow()?;
        return tag_from_name(&name).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown TLV name {name:?} — use an SMPP 3.4 optional-parameter \
                 name (e.g. \"MESSAGE_PAYLOAD\") or a raw integer tag for a \
                 vendor-specific parameter"
            ))
        });
    }
    if key.is_instance_of::<PyInt>() {
        let raw: i64 = key.extract()?;
        return u16::try_from(raw).map_err(|_| {
            PyValueError::new_err(format!(
                "TLV tag {raw} out of range — a tag is a 16-bit unsigned integer"
            ))
        });
    }
    Err(PyTypeError::new_err(format!(
        "TLV key must be a name or an integer tag, got {}",
        key.get_type().name()?
    )))
}

/// A dict value. `bytes`/`bytearray` go on the wire verbatim, `str` becomes a
/// NUL-terminated C-Octet-String (§3.2.1.1, what `receipted_message_id` and
/// friends require), and `int` is encoded at the tag's spec width.
pub fn value_to_bytes(tag: u16, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = value.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(b) = value.cast::<PyByteArray>() {
        return Ok(b.to_vec());
    }
    if let Ok(s) = value.cast::<PyString>() {
        let mut bytes = s.to_cow()?.as_bytes().to_vec();
        bytes.push(0x00);
        return Ok(bytes);
    }
    // `bool` is a subclass of `int` in Python; treat it as the 0/1 it is.
    if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyBool>() {
        let raw: u64 = value.extract().map_err(|_| {
            PyValueError::new_err(format!(
                "TLV {} value must be a non-negative integer",
                describe_tag(tag)
            ))
        })?;
        let width = int_width(tag).ok_or_else(|| {
            PyValueError::new_err(format!(
                "TLV {} is not an integer-typed optional parameter — pass bytes \
                 (or a str for a C-octet-string parameter) so the encoding is \
                 explicit",
                describe_tag(tag)
            ))
        })?;
        let max = if width >= 8 {
            u64::MAX
        } else {
            (1u64 << (width * 8)) - 1
        };
        if raw > max {
            return Err(PyValueError::new_err(format!(
                "TLV {} value {raw} does not fit in its {width}-octet spec width",
                describe_tag(tag)
            )));
        }
        return Ok(raw.to_be_bytes()[8 - width..].to_vec());
    }
    Err(PyTypeError::new_err(format!(
        "TLV {} value must be bytes, str or int, got {}",
        describe_tag(tag),
        value.get_type().name()?
    )))
}

/// `MESSAGE_PAYLOAD (0x0424)` / `0x1400` — for error messages.
fn describe_tag(tag: u16) -> String {
    match name_for_tag(tag) {
        Some(name) => format!("{name} (0x{tag:04X})"),
        None => format!("0x{tag:04X}"),
    }
}

/// Convert a script's `tlvs={...}` into the codec's list.
///
/// Two keys resolving to the same tag (say `"MESSAGE_STATE"` and `0x0427`) is
/// an error: the dict cannot express which one wins and putting both on the
/// wire is malformed.
pub fn tlvs_from_py(dict: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<Tlv>> {
    let Some(dict) = dict else {
        return Ok(Vec::new());
    };
    let mut out: Vec<Tlv> = Vec::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let tag = tag_from_py(&key)?;
        if out.iter().any(|t| t.tag == tag) {
            return Err(PyValueError::new_err(format!(
                "TLV {} given twice",
                describe_tag(tag)
            )));
        }
        out.push(Tlv::new(tag, value_to_bytes(tag, &value)?));
    }
    Ok(out)
}

/// Surface decoded optional parameters to a script as `{tag: bytes}`.
///
/// Keyed by the raw tag rather than the name so vendor-specific parameters —
/// which have no name — read the same way as the standard ones.
pub fn tlvs_to_py<'py>(py: Python<'py>, tlvs: &[Tlv]) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    for tlv in tlvs {
        d.set_item(tlv.tag, PyBytes::new(py, &tlv.value))?;
    }
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyList;

    // TLV values are asserted against hand-written SMPP 3.4 §3.2.1 byte
    // layouts, not against our own decoder — a shared encode/decode bug would
    // sail straight through a round-trip.

    fn encode(tag: &str, value: &Bound<'_, PyAny>) -> Vec<u8> {
        let tag = tag_from_name(tag).expect("known tag");
        Tlv::new(tag, value_to_bytes(tag, value).expect("encodes")).encode()
    }

    #[test]
    fn message_payload_bytes_encode_to_spec_layout() {
        Python::attach(|py| {
            let v = PyBytes::new(py, b"hi").into_any();
            // tag 0x0424, length 0x0002, value "hi"
            assert_eq!(
                encode("MESSAGE_PAYLOAD", &v),
                vec![0x04, 0x24, 0x00, 0x02, 0x68, 0x69]
            );
        });
    }

    #[test]
    fn receipted_message_id_str_is_nul_terminated() {
        Python::attach(|py| {
            let v = PyString::new(py, "0a1b2").into_any();
            // §3.2.1.1: C-Octet String, so 5 characters plus the NUL = length 6.
            assert_eq!(
                encode("RECEIPTED_MESSAGE_ID", &v),
                vec![0x00, 0x1E, 0x00, 0x06, 0x30, 0x61, 0x31, 0x62, 0x32, 0x00]
            );
        });
    }

    #[test]
    fn integer_tags_use_their_spec_width_not_the_value() {
        Python::attach(|py| {
            let two = 2u8.into_pyobject(py).expect("int").into_any();
            // message_state: 1 octet.
            assert_eq!(
                encode("MESSAGE_STATE", &two),
                vec![0x04, 0x27, 0x00, 0x01, 0x02]
            );
            // sar_msg_ref_num: 2 octets, same input value.
            assert_eq!(
                encode("SAR_MSG_REF_NUM", &two),
                vec![0x02, 0x0C, 0x00, 0x02, 0x00, 0x02]
            );
            // qos_time_to_live: 4 octets, same input value.
            assert_eq!(
                encode("QOS_TIME_TO_LIVE", &two),
                vec![0x00, 0x17, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02]
            );
        });
    }

    #[test]
    fn sar_msg_ref_num_is_big_endian() {
        Python::attach(|py| {
            let v = 4660u16.into_pyobject(py).expect("int").into_any();
            assert_eq!(
                encode("SAR_MSG_REF_NUM", &v),
                vec![0x02, 0x0C, 0x00, 0x02, 0x12, 0x34]
            );
        });
    }

    #[test]
    fn tag_names_are_case_insensitive_and_accept_the_tlv_prefix() {
        assert_eq!(tag_from_name("MESSAGE_PAYLOAD"), Some(0x0424));
        assert_eq!(tag_from_name("message_payload"), Some(0x0424));
        assert_eq!(tag_from_name("TLV_MESSAGE_PAYLOAD"), Some(0x0424));
        assert_eq!(tag_from_name("  MESSAGE_STATE  "), Some(0x0427));
        assert_eq!(tag_from_name("NOPE"), None);
    }

    #[test]
    fn every_int_width_tag_is_a_known_tag() {
        // Guards the table against a tag that TlvTag knows but ALL doesn't.
        for (tag, width) in INT_WIDTHS {
            assert!(
                name_for_tag(*tag as u16).is_some(),
                "0x{:04X} missing from TlvTag::ALL",
                *tag as u16
            );
            assert!(matches!(width, 1 | 2 | 4), "odd width {width}");
        }
    }

    #[test]
    fn unknown_tag_name_is_a_value_error() {
        Python::attach(|py| {
            let key = PyString::new(py, "NOT_A_TAG").into_any();
            let err = tag_from_py(&key).expect_err("unknown name rejected");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn raw_integer_tags_pass_through_and_range_is_checked() {
        Python::attach(|py| {
            let key = 0x1400u32.into_pyobject(py).expect("int").into_any();
            assert_eq!(tag_from_py(&key).expect("in range"), 0x1400);

            let too_big = 70000u32.into_pyobject(py).expect("int").into_any();
            let err = tag_from_py(&too_big).expect_err("out of range rejected");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn non_integer_tag_rejects_an_int_value() {
        Python::attach(|py| {
            // message_payload is an octet string; an int has no obvious width.
            let v = 2u8.into_pyobject(py).expect("int").into_any();
            let err = value_to_bytes(0x0424, &v).expect_err("int rejected");
            assert!(err.is_instance_of::<PyValueError>(py));

            // Same for a vendor tag we know nothing about.
            let err = value_to_bytes(0x1400, &v).expect_err("int rejected");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn integer_value_must_fit_the_spec_width() {
        Python::attach(|py| {
            let v = 300u32.into_pyobject(py).expect("int").into_any();
            let err = value_to_bytes(0x0427, &v).expect_err("300 does not fit one octet");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn unsupported_value_type_is_a_type_error() {
        Python::attach(|py| {
            let v = PyList::empty(py).into_any();
            let err = value_to_bytes(0x0424, &v).expect_err("list rejected");
            assert!(err.is_instance_of::<PyTypeError>(py));
        });
    }

    #[test]
    fn duplicate_tags_are_rejected() {
        Python::attach(|py| {
            let d = PyDict::new(py);
            d.set_item("MESSAGE_STATE", 2u8).expect("set");
            d.set_item(0x0427u16, PyBytes::new(py, b"\x02"))
                .expect("set");
            let err = tlvs_from_py(Some(&d)).expect_err("same tag twice rejected");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn round_trip_through_the_python_dict_shape() {
        Python::attach(|py| {
            let d = PyDict::new(py);
            d.set_item("MESSAGE_PAYLOAD", PyBytes::new(py, b"hi"))
                .expect("set");
            d.set_item(0x1400u16, PyBytes::new(py, b"\x01"))
                .expect("set");

            let tlvs = tlvs_from_py(Some(&d)).expect("converts");
            assert_eq!(tlvs.len(), 2);

            let back = tlvs_to_py(py, &tlvs).expect("converts back");
            let payload: Vec<u8> = back
                .get_item(0x0424u16)
                .expect("lookup")
                .expect("present")
                .extract()
                .expect("bytes");
            assert_eq!(payload, b"hi");
            let vendor: Vec<u8> = back
                .get_item(0x1400u16)
                .expect("lookup")
                .expect("present")
                .extract()
                .expect("bytes");
            assert_eq!(vendor, b"\x01");
        });
    }

    #[test]
    fn no_tlvs_is_an_empty_list() {
        Python::attach(|_py| {
            assert!(tlvs_from_py(None).expect("converts").is_empty());
        });
    }
}
