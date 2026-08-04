//! Inbound wire known-answer tests.
//!
//! Every PDU here is written out byte by byte against SMPP 3.4 (§4.2.2
//! `data_sm`, §4.12.1 `alert_notification`, §3.2.1 optional parameters),
//! decoded with `smpp34`, and read back through the *Python* attributes a
//! script actually sees. Nothing round-trips through our own encoder: a
//! bug shared by an encode/decode pair passes a round-trip unnoticed.
//!
//! Addresses are from the synthetic 555-01xx range.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use siphon_smpp::pyclasses::{AlertNotification, Pdu};
use smpp34::{alert_notification, data_sm, CommandHeader};

/// Wrap a PDU body in the 16-octet header every SMPP PDU carries
/// (§3.2): command_length (of the whole PDU), command_id, command_status,
/// sequence_number.
fn with_header(command_id: u32, sequence_number: u32, body: &[u8]) -> Vec<u8> {
    let mut pdu = Vec::with_capacity(16 + body.len());
    pdu.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    pdu.extend_from_slice(&command_id.to_be_bytes());
    pdu.extend_from_slice(&0u32.to_be_bytes());
    pdu.extend_from_slice(&sequence_number.to_be_bytes());
    pdu.extend_from_slice(body);
    pdu
}

/// One optional parameter: tag, length, value (§3.2.1).
fn tlv(tag: u16, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + value.len());
    out.extend_from_slice(&tag.to_be_bytes());
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// `data_sm` mandatory parameters in wire order (§4.2.2). Note the absence
/// of `sm_length` / `short_message` — a `data_sm` has neither.
fn data_sm_body_with(esm_class: u8, extra: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0x00); // service_type: empty C-Octet String
    body.push(0x01); // source_addr_ton: international
    body.push(0x01); // source_addr_npi: ISDN
    body.extend_from_slice(b"5550100\0");
    body.push(0x01); // dest_addr_ton
    body.push(0x01); // dest_addr_npi
    body.extend_from_slice(b"5550199\0");
    body.push(esm_class);
    body.push(0x01); // registered_delivery: receipt requested
    body.push(0x00); // data_coding: SMSC default
    body.extend_from_slice(extra);
    body
}

fn data_sm_body(extra: &[u8]) -> Vec<u8> {
    data_sm_body_with(0x00, extra)
}

fn decode_data_sm(pdu: &[u8]) -> data_sm {
    let header = CommandHeader::decode(pdu).expect("header decodes");
    data_sm::decode(header, pdu).expect("data_sm decodes")
}

/// Hand a `Pdu` to Python so the assertions read the same attributes a
/// script would, not the Rust fields behind them.
fn py_pdu<'py>(py: Python<'py>, pdu: &Pdu) -> Bound<'py, PyAny> {
    Py::new(py, pdu.clone())
        .expect("pyclass")
        .into_bound(py)
        .into_any()
}

macro_rules! attr {
    ($obj:expr, $name:literal, $ty:ty) => {{
        let value = $obj.getattr($name).expect(concat!("getattr ", $name));
        value.extract::<$ty>().expect(concat!("extract ", $name))
    }};
}

#[test]
fn inbound_data_sm_body_survives_as_message_payload() {
    // The whole point of the 1.3.0 bump: before it, `data_sm` had no
    // `tlvs` field, so every inbound one reached a handler bodyless.
    let pdu = with_header(0x0000_0103, 7, &data_sm_body(&tlv(0x0424, b"hello, world")));
    let decoded = decode_data_sm(&pdu);
    let converted = Pdu::from_data(&decoded);

    Python::attach(|py| {
        let pdu = py_pdu(py, &converted);
        assert_eq!(attr!(pdu, "body", Vec<u8>), b"hello, world");
        assert_eq!(attr!(pdu, "message_payload", Vec<u8>), b"hello, world");

        // A data_sm has no short_message field on the wire; we don't
        // invent one.
        assert!(attr!(pdu, "short_message", Vec<u8>).is_empty());

        assert_eq!(attr!(pdu, "command", String), "data_sm");
        assert_eq!(attr!(pdu, "source_addr", String), "5550100");
        assert_eq!(attr!(pdu, "destination_addr", String), "5550199");
    });
}

#[test]
fn inbound_data_sm_without_a_payload_has_an_empty_body() {
    let pdu = with_header(0x0000_0103, 8, &data_sm_body(&[]));
    let converted = Pdu::from_data(&decode_data_sm(&pdu));

    Python::attach(|py| {
        let pdu = py_pdu(py, &converted);
        assert!(attr!(pdu, "body", Vec<u8>).is_empty());
        assert!(pdu.getattr("message_payload").expect("getattr").is_none());
    });
}

#[test]
fn inbound_data_sm_surfaces_every_optional_parameter() {
    // Segment 2 of 3 of a concatenated message, plus a vendor tag.
    let mut extra = tlv(0x0424, b"part two");
    extra.extend_from_slice(&tlv(0x020C, &[0x12, 0x34])); // sar_msg_ref_num
    extra.extend_from_slice(&tlv(0x020E, &[0x03])); // sar_total_segments
    extra.extend_from_slice(&tlv(0x020F, &[0x02])); // sar_segment_seqnum
    extra.extend_from_slice(&tlv(0x1400, &[0xAB])); // vendor-specific

    let pdu = with_header(0x0000_0103, 9, &data_sm_body(&extra));
    let converted = Pdu::from_data(&decode_data_sm(&pdu));

    Python::attach(|py| {
        let pdu = py_pdu(py, &converted);
        assert_eq!(attr!(pdu, "sar_msg_ref_num", u16), 0x1234);
        assert_eq!(attr!(pdu, "sar_total_segments", u8), 3);
        assert_eq!(attr!(pdu, "sar_segment_seqnum", u8), 2);

        // A tag with no spec name is still reachable, by number.
        let vendor = pdu.call_method1("tlv", (0x1400u16,)).expect("tlv()");
        assert_eq!(vendor.extract::<Vec<u8>>().expect("bytes"), vec![0xAB]);

        // …and by name for the standard ones.
        let payload = pdu
            .call_method1("tlv", ("MESSAGE_PAYLOAD",))
            .expect("tlv()");
        assert_eq!(payload.extract::<Vec<u8>>().expect("bytes"), b"part two");
    });
}

#[test]
fn inbound_delivery_receipt_reads_the_spec_optional_parameters() {
    // A receipt carried entirely in the TLVs: receipted_message_id is a
    // C-Octet String (§5.3.2.26), message_state a single octet (§5.3.2.35).
    let mut extra = tlv(0x001E, b"0a1b2\0");
    extra.extend_from_slice(&tlv(0x0427, &[0x02])); // DELIVERED

    // esm_class 0x04: SMSC delivery receipt.
    let pdu = with_header(0x0000_0103, 10, &data_sm_body_with(0x04, &extra));
    let converted = Pdu::from_data(&decode_data_sm(&pdu));

    Python::attach(|py| {
        let pdu = py_pdu(py, &converted);
        assert!(attr!(pdu, "is_dlr", bool));
        assert_eq!(attr!(pdu, "receipted_message_id", String), "0a1b2");
        assert_eq!(attr!(pdu, "message_state", u8), 2);

        let receipt = pdu.getattr("receipt").expect("getattr");
        let id = receipt.get_item("id").expect("id");
        assert_eq!(id.extract::<String>().expect("str"), "0a1b2");
        let stat = receipt.get_item("stat").expect("stat");
        assert_eq!(stat.extract::<String>().expect("str"), "DELIVRD");
    });
}

#[test]
fn inbound_alert_notification_decodes_from_the_right_offset() {
    // Regression guard for the bug smpp34 1.3.0 fixed: alert_notification's
    // decode parsed from byte 0 while the read loop hands it a whole PDU,
    // so every field landed 16 bytes off — source_addr_ton came out of
    // command_length — and it did so without erroring.
    let mut body = Vec::new();
    body.push(0x01); // source_addr_ton
    body.push(0x01); // source_addr_npi
    body.extend_from_slice(b"5550123\0"); // the MS that became available
    body.push(0x01); // esme_addr_ton
    body.push(0x01); // esme_addr_npi
    body.extend_from_slice(b"5550188\0"); // the ESME to alert
    body.extend_from_slice(&tlv(0x0422, &[0x00])); // ms_availability_status: available

    let pdu = with_header(0x0000_0102, 11, &body);
    let header = CommandHeader::decode(&pdu).expect("header decodes");
    let decoded = alert_notification::decode(header, &pdu).expect("alert decodes");
    let alert = AlertNotification::from_alert(&decoded);

    assert_eq!(alert.source_addr, "5550123");
    assert_eq!(alert.esme_addr, "5550188");
    assert_eq!(alert.ms_availability_status, Some(0));
}

#[test]
fn inbound_alert_notification_accepts_the_pre_1_3_bare_octet() {
    // smpp34 <= 1.2.1 wrote ms_availability_status as a lone trailing
    // octet instead of TLV 0x0422. Peers running it are still out there and
    // the two forms can't be confused, a TLV being at least four bytes.
    let mut body = Vec::new();
    body.push(0x01);
    body.push(0x01);
    body.extend_from_slice(b"5550123\0");
    body.push(0x01);
    body.push(0x01);
    body.extend_from_slice(b"5550188\0");
    body.push(0x02); // bare octet: unavailable

    let pdu = with_header(0x0000_0102, 12, &body);
    let header = CommandHeader::decode(&pdu).expect("header decodes");
    let decoded = alert_notification::decode(header, &pdu).expect("alert decodes");
    let alert = AlertNotification::from_alert(&decoded);

    assert_eq!(alert.source_addr, "5550123");
    assert_eq!(alert.esme_addr, "5550188");
    assert_eq!(alert.ms_availability_status, Some(2));
}

#[test]
fn alert_notification_without_a_status_is_none() {
    let mut body = Vec::new();
    body.push(0x01);
    body.push(0x01);
    body.extend_from_slice(b"5550123\0");
    body.push(0x01);
    body.push(0x01);
    body.extend_from_slice(b"5550188\0");

    let pdu = with_header(0x0000_0102, 13, &body);
    let header = CommandHeader::decode(&pdu).expect("header decodes");
    let decoded = alert_notification::decode(header, &pdu).expect("alert decodes");
    let alert = AlertNotification::from_alert(&decoded);

    assert_eq!(alert.ms_availability_status, None);
}

/// Keeps the unused-import lint honest about `PyBytes` if the assertions
/// above ever stop extracting to `Vec<u8>`.
#[test]
fn py_bytes_is_the_body_type() {
    let pdu = with_header(0x0000_0103, 14, &data_sm_body(&tlv(0x0424, b"x")));
    let converted = Pdu::from_data(&decode_data_sm(&pdu));
    Python::attach(|py| {
        let body = py_pdu(py, &converted).getattr("body").expect("getattr");
        assert!(body.is_instance_of::<PyBytes>());
    });
}
