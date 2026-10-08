//! What a send helper reports for each thing a peer can do with a request.
//!
//! A real `smpp34` session on a loopback socket, against a peer that is
//! nothing but bytes written out by hand from the SMPP 3.4 PDU layouts
//! (§3.2 header, §4.x bodies). The peer does not use the codec, so a
//! response the codec would never build — a vendor status, a header-only
//! `submit_sm_resp` — is exactly as easy to send as a well-behaved one,
//! and a bug shared by the codec's encoder and decoder cannot hide.
//!
//! Each test drives the request the way the helper does (`settle` around
//! the same `smpp34` call, then `SmppResp::from_reply`) and asserts on
//! what a script would read off the result, through Python.
//!
//! Set `SMPP_TEST_CAPTURE_DIR` to have every PDU the test peer sent
//! written out as hex, one file per test, for decoding with an
//! independent dissector.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pyo3::prelude::*;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::server::ESME;
use smpp34::{
    bind_transceiver, bind_transceiver_resp, SmppConnectionInformation, SmppError, SmppServer,
    SmppServerListener,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::outcome::{settle, SendFailure};
use crate::runtime::{bind_gone, esme_gone, BindSession, EsmeSession};
use crate::sends::{QueryResp, SmppResp};

// ── SMPP 3.4 command ids (§5.1.2.1), written out rather than imported ──

const GENERIC_NACK: u32 = 0x8000_0000;
const QUERY_SM_RESP: u32 = 0x8000_0003;
const SUBMIT_SM_RESP: u32 = 0x8000_0004;
const DELIVER_SM_RESP: u32 = 0x8000_0005;
const UNBIND: u32 = 0x0000_0006;
const UNBIND_RESP: u32 = 0x8000_0006;
const REPLACE_SM_RESP: u32 = 0x8000_0007;
const CANCEL_SM_RESP: u32 = 0x8000_0008;
const BIND_TRANSCEIVER: u32 = 0x0000_0009;
const BIND_TRANSCEIVER_RESP: u32 = 0x8000_0009;
const ENQUIRE_LINK: u32 = 0x0000_0015;
const ENQUIRE_LINK_RESP: u32 = 0x8000_0015;
const SUBMIT_MULTI_RESP: u32 = 0x8000_0021;
const DATA_SM_RESP: u32 = 0x8000_0103;

/// Long enough that no test waits on it by accident.
const LONG_TIMER: Duration = Duration::from_secs(20);
/// Short enough to wait out in a test.
const SHORT_TIMER: Duration = Duration::from_millis(400);
/// A failure that is not a timeout has to arrive well inside `LONG_TIMER`.
const PROMPT: Duration = Duration::from_secs(5);

// ── The hand-written peer ──────────────────────────────────────────────

/// One PDU: the 16-octet header of §3.2 followed by `body`.
fn pdu(command_id: u32, command_status: u32, sequence_number: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + body.len());
    out.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    out.extend_from_slice(&command_id.to_be_bytes());
    out.extend_from_slice(&command_status.to_be_bytes());
    out.extend_from_slice(&sequence_number.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// What the peer does with the request under test.
#[derive(Clone)]
enum Answer {
    /// The operation's own response PDU, with this status and body.
    Respond {
        command_id: u32,
        status: u32,
        body: Vec<u8>,
    },
    /// A `generic_nack` (§4.3) with this status.
    Nack(u32),
    /// Read the request and say nothing.
    Silent,
    /// Read the request and close the connection.
    Close,
}

impl Answer {
    /// A response with a non-zero status and, as the specification
    /// requires of most of them, no body.
    fn reject(command_id: u32, status: u32) -> Self {
        Answer::Respond {
            command_id,
            status,
            body: Vec::new(),
        }
    }
}

/// Everything the peer wrote, for an independent decoder to look at.
type Sent = Arc<StdMutex<Vec<Vec<u8>>>>;

async fn write_pdu(stream: &mut TcpStream, sent: &Sent, bytes: Vec<u8>) {
    stream.write_all(&bytes).await.expect("peer write");
    sent.lock().expect("sent").push(bytes);
}

/// Read one whole PDU; `None` once the other side has closed.
async fn read_pdu(stream: &mut TcpStream) -> Option<(u32, u32)> {
    let mut header = [0u8; 16];
    stream.read_exact(&mut header).await.ok()?;
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let command_id = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    let sequence = u32::from_be_bytes([header[12], header[13], header[14], header[15]]);
    let mut body = vec![0u8; length.saturating_sub(16)];
    stream.read_exact(&mut body).await.ok()?;
    Some((command_id, sequence))
}

/// Serve one session after the bind: keep the link alive, and give the
/// first real request `answer`.
async fn serve(mut stream: TcpStream, sent: Sent, answer: Answer) {
    while let Some((command_id, sequence)) = read_pdu(&mut stream).await {
        match command_id {
            ENQUIRE_LINK => {
                write_pdu(&mut stream, &sent, pdu(ENQUIRE_LINK_RESP, 0, sequence, &[])).await;
            }
            UNBIND => {
                write_pdu(&mut stream, &sent, pdu(UNBIND_RESP, 0, sequence, &[])).await;
                return;
            }
            _ => match &answer {
                Answer::Respond {
                    command_id,
                    status,
                    body,
                } => {
                    write_pdu(
                        &mut stream,
                        &sent,
                        pdu(*command_id, *status, sequence, body),
                    )
                    .await;
                }
                Answer::Nack(status) => {
                    write_pdu(
                        &mut stream,
                        &sent,
                        pdu(GENERIC_NACK, *status, sequence, &[]),
                    )
                    .await;
                }
                Answer::Silent => {}
                Answer::Close => return,
            },
        }
    }
}

/// A peer in the SMSC role: accepts one `bind_transceiver`, then serves.
async fn fake_smsc(answer: Answer) -> (u16, Sent) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("listen");
    let port = listener.local_addr().expect("addr").port();
    let sent: Sent = Arc::default();
    let peer_sent = sent.clone();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let (command_id, sequence) = read_pdu(&mut stream).await.expect("bind");
        assert_eq!(command_id, BIND_TRANSCEIVER);
        // §4.1.6: system_id, then nothing else we need.
        let bound = pdu(BIND_TRANSCEIVER_RESP, 0, sequence, b"peer\0");
        write_pdu(&mut stream, &peer_sent, bound).await;
        serve(stream, peer_sent, answer).await;
    });
    (port, sent)
}

/// A peer in the ESME role: binds as a transceiver to `port`, then serves.
async fn fake_esme(port: u16, answer: Answer) -> Sent {
    let sent: Sent = Arc::default();
    let peer_sent = sent.clone();
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("connect");
    // §4.1.5 bind_transceiver body.
    let mut body = Vec::new();
    body.extend_from_slice(b"esme\0"); // system_id
    body.extend_from_slice(b"secret\0"); // password
    body.push(0x00); // system_type
    body.push(0x34); // interface_version
    body.push(0x00); // addr_ton
    body.push(0x00); // addr_npi
    body.push(0x00); // address_range
    write_pdu(&mut stream, &peer_sent, pdu(BIND_TRANSCEIVER, 0, 1, &body)).await;
    let (command_id, _) = read_pdu(&mut stream).await.expect("bind response");
    assert_eq!(command_id, BIND_TRANSCEIVER_RESP);
    tokio::spawn(serve(stream, peer_sent, answer));
    sent
}

/// Write what the peer sent to `$SMPP_TEST_CAPTURE_DIR/<name>.hex`, one
/// PDU per line, when that variable is set.
fn capture(name: &str, sent: &Sent) {
    let Ok(dir) = std::env::var("SMPP_TEST_CAPTURE_DIR") else {
        return;
    };
    let lines: Vec<String> = sent
        .lock()
        .expect("sent")
        .iter()
        .map(|pdu| pdu.iter().map(|b| format!("{b:02x}")).collect::<String>())
        .collect();
    std::fs::write(
        std::path::Path::new(&dir).join(format!("{name}.hex")),
        lines.join("\n") + "\n",
    )
    .expect("write capture");
}

// ── Our side: real smpp34 sessions, registered as the runtime does ─────

/// Keeps the outbound-session registry the way the runtime's listener
/// does: pushed on bound, removed on unbound.
struct Binds {
    binds: Mutex<Vec<BindSession>>,
    response_timer: Duration,
}

#[async_trait]
impl SmppClientListener for Binds {
    async fn on_smsc_bound(&self, smsc: SMSC, _session_id: &String) {
        self.binds.lock().await.push(BindSession {
            name: "upstream".to_string(),
            smsc: Arc::new(smsc),
            throttle: None,
            response_timer: self.response_timer,
        });
    }

    async fn on_smsc_unbound(&self, session_id: &String) {
        self.binds
            .lock()
            .await
            .retain(|b| b.smsc.session_id != *session_id);
    }
}

/// The inbound mirror of [`Binds`].
struct Esmes {
    esmes: Mutex<Vec<EsmeSession>>,
    response_timer: Duration,
}

#[async_trait]
impl SmppServerListener for Esmes {
    async fn on_bind_transceiver(
        &self,
        request: bind_transceiver,
        _conn: &SmppConnectionInformation,
        _session: &String,
    ) -> bind_transceiver_resp {
        let system_id = request.system_id.clone();
        request.accept(system_id, Some(0x34))
    }

    async fn on_esme_bound(&self, esme: ESME, _session_id: &String) {
        self.esmes.lock().await.push(EsmeSession {
            esme: Arc::new(esme),
            throttle: None,
            response_timer: self.response_timer,
        });
    }

    async fn on_esme_unbound(&self, session_id: &String) {
        self.esmes
            .lock()
            .await
            .retain(|e| e.esme.session_id != *session_id);
    }
}

/// An outbound bind to a fake SMSC, up and registered.
struct Outbound {
    registry: Arc<Binds>,
    smsc: Arc<SMSC>,
    sent: Sent,
    _client: SmppClient,
}

async fn outbound(answer: Answer, response_timer: Duration) -> Outbound {
    let (port, sent) = fake_smsc(answer).await;
    let registry = Arc::new(Binds {
        binds: Mutex::new(Vec::new()),
        response_timer,
    });
    let listener: Arc<dyn SmppClientListener + Send + Sync> = registry.clone();
    let mut client = SmppClient::new_with_default_timers(
        Ipv4Addr::LOCALHOST.to_string(),
        port,
        false,
        BIND_TYPE::TRX,
        "esme".to_string(),
        "secret".to_string(),
        String::new(),
        1,
        1,
        String::new(),
        listener,
        5_000,
        // No enquire_link during a test.
        600_000,
        600_000,
        response_timer.as_millis() as u64,
        1_500,
        20,
    );
    client.start().await;
    let smsc = wait_for(|| async {
        registry
            .binds
            .lock()
            .await
            .first()
            .map(|bind| bind.smsc.clone())
    })
    .await;
    Outbound {
        registry,
        smsc,
        sent,
        _client: client,
    }
}

/// An inbound session from a fake ESME, up and registered.
struct Inbound {
    registry: Arc<Esmes>,
    esme: Arc<ESME>,
    sent: Sent,
    _server: SmppServer,
}

async fn inbound(answer: Answer, response_timer: Duration) -> Inbound {
    // SmppServer takes a port number, not a socket: find a free one.
    let port = {
        let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("probe");
        probe.local_addr().expect("addr").port()
    };
    let registry = Arc::new(Esmes {
        esmes: Mutex::new(Vec::new()),
        response_timer,
    });
    let listener: Arc<dyn SmppServerListener + Send + Sync> = registry.clone();
    let mut server = SmppServer::new_with_default_timers(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        port,
        listener,
        5_000,
        600_000,
        600_000,
        response_timer.as_millis() as u64,
        1_500,
    );
    server.start().await;
    let sent = fake_esme(port, answer).await;
    let esme = wait_for(|| async {
        registry
            .esmes
            .lock()
            .await
            .first()
            .map(|session| session.esme.clone())
    })
    .await;
    Inbound {
        registry,
        esme,
        sent,
        _server: server,
    }
}

async fn wait_for<T, F, Fut>(mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        assert!(Instant::now() < deadline, "session did not come up");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ── The requests, sent the way the helpers send them ───────────────────

impl Outbound {
    async fn submit_sm(&self) -> Result<SmppResp, SendFailure> {
        let send = self
            .smsc
            .submit_sm()
            .source_addr("5550100")
            .destination_addr("5550199")
            .short_message(b"hello".to_vec())
            .send();
        self.settled(send)
            .await
            .map(|resp| SmppResp::from_reply(&resp))
    }

    async fn settled<R>(
        &self,
        send: impl std::future::Future<Output = Result<R, SmppError>>,
    ) -> Result<R, SendFailure> {
        let timer = self.registry.response_timer;
        settle(send, timer, bind_gone(&self.registry.binds, &self.smsc)).await
    }
}

impl Inbound {
    async fn deliver_sm(&self) -> Result<SmppResp, SendFailure> {
        let send = self
            .esme
            .deliver_sm()
            .source_addr("5550199")
            .destination_addr("5550100")
            .short_message(b"hello".to_vec())
            .send();
        self.settled(send)
            .await
            .map(|resp| SmppResp::from_reply(&resp))
    }

    async fn settled<R>(
        &self,
        send: impl std::future::Future<Output = Result<R, SmppError>>,
    ) -> Result<R, SendFailure> {
        let timer = self.registry.response_timer;
        settle(send, timer, esme_gone(&self.registry.esmes, &self.esme)).await
    }
}

/// What a script reads off the result object.
#[derive(Debug, PartialEq)]
struct Seen {
    truthy: bool,
    ok: bool,
    throttled: bool,
    command_status: String,
    command_status_code: u32,
    message_id: String,
}

fn seen<T: pyo3::PyClass + Into<pyo3::PyClassInitializer<T>>>(resp: T) -> Seen {
    Python::attach(|py| {
        let object = Py::new(py, resp).expect("to python").into_any();
        let object = object.bind(py);
        let attr = |name: &str| object.getattr(name).expect("attribute");
        Seen {
            truthy: object.is_truthy().expect("truthiness"),
            ok: attr("ok").extract().expect("ok"),
            throttled: attr("throttled").extract().expect("throttled"),
            command_status: attr("command_status").extract().expect("name"),
            command_status_code: attr("command_status_code").extract().expect("code"),
            message_id: attr("message_id").extract().expect("message_id"),
        }
    })
}

fn rejected(name: &str, code: u32) -> Seen {
    Seen {
        truthy: false,
        ok: false,
        throttled: false,
        command_status: name.to_string(),
        command_status_code: code,
        message_id: String::new(),
    }
}

// ── submit_sm on an outbound bind ──────────────────────────────────────

#[tokio::test]
async fn an_accepted_submit_sm_is_truthy_and_carries_the_message_id() {
    let session = outbound(
        Answer::Respond {
            command_id: SUBMIT_SM_RESP,
            status: 0,
            body: b"id-1\0".to_vec(),
        },
        LONG_TIMER,
    )
    .await;
    let resp = session.submit_sm().await.expect("a response");
    capture("submit_sm_accepted", &session.sent);
    assert_eq!(
        seen(resp),
        Seen {
            truthy: true,
            ok: true,
            throttled: false,
            command_status: "ESME_ROK".to_string(),
            command_status_code: 0,
            message_id: "id-1".to_string(),
        }
    );
}

#[tokio::test]
async fn a_rejected_submit_sm_reports_the_status_the_peer_sent() {
    // §4.4.2: no body when the status is non-zero.
    for (name, code) in [
        ("ESME_RINVDSTADR", 0x0000_000Bu32),
        ("ESME_RSUBMITFAIL", 0x0000_0045),
        ("ESME_RINVSRCADR", 0x0000_000A),
        ("ESME_RSYSERR", 0x0000_0008),
    ] {
        let session = outbound(Answer::reject(SUBMIT_SM_RESP, code), LONG_TIMER).await;
        let resp = session.submit_sm().await.expect("a response");
        capture(&format!("submit_sm_{name}"), &session.sent);
        assert_eq!(seen(resp), rejected(name, code), "{name}");
    }
}

#[tokio::test]
async fn a_throttled_submit_sm_is_not_an_accepted_one() {
    for (name, code) in [
        ("ESME_RTHROTTLED", 0x0000_0058u32),
        ("ESME_RMSGQFUL", 0x0000_0014),
    ] {
        let session = outbound(Answer::reject(SUBMIT_SM_RESP, code), LONG_TIMER).await;
        let resp = session.submit_sm().await.expect("a response");
        capture(&format!("submit_sm_{name}"), &session.sent);
        assert_eq!(
            seen(resp),
            Seen {
                throttled: true,
                ..rejected(name, code)
            },
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_vendor_status_keeps_its_number() {
    // §5.1.3 leaves 0x400-0x4FF to the SMSC vendor.
    let session = outbound(Answer::reject(SUBMIT_SM_RESP, 0x0000_0401), LONG_TIMER).await;
    let resp = session.submit_sm().await.expect("a response");
    capture("submit_sm_vendor_status", &session.sent);
    assert_eq!(seen(resp), rejected("0x00000401", 0x0000_0401));
}

#[tokio::test]
async fn a_generic_nack_to_submit_sm_is_not_a_response() {
    let session = outbound(Answer::Nack(0x0000_0003), LONG_TIMER).await;
    let started = Instant::now();
    let failure = session.submit_sm().await.expect_err("no response PDU");
    capture("submit_sm_generic_nack", &session.sent);
    assert_eq!(failure, SendFailure::Nack(SmppError::ESME_RINVCMDID));
    assert!(started.elapsed() < PROMPT);
}

#[tokio::test]
async fn a_generic_nack_carrying_rsyserr_is_not_given_a_cause() {
    // The one nack the codec reports with the status it also uses for
    // its own failures. It must not be passed off as a timeout or a close.
    let session = outbound(Answer::Nack(0x0000_0008), LONG_TIMER).await;
    let failure = session.submit_sm().await.expect_err("no response PDU");
    assert_eq!(failure, SendFailure::Unanswered);
}

#[tokio::test]
async fn a_submit_sm_nobody_answers_is_a_timeout() {
    let session = outbound(Answer::Silent, SHORT_TIMER).await;
    let started = Instant::now();
    let failure = session.submit_sm().await.expect_err("no response PDU");
    assert_eq!(failure, SendFailure::Timeout(SHORT_TIMER));
    assert!(started.elapsed() >= SHORT_TIMER);
}

#[tokio::test]
async fn a_peer_that_closes_mid_request_is_a_close_not_a_timeout() {
    let session = outbound(Answer::Close, LONG_TIMER).await;
    let started = Instant::now();
    let failure = session.submit_sm().await.expect_err("no response PDU");
    assert_eq!(failure, SendFailure::Closed);
    assert!(
        started.elapsed() < PROMPT,
        "a close must not be sat out as a timeout"
    );
}

// ── the other requests an outbound bind originates ─────────────────────

#[tokio::test]
async fn a_rejected_data_sm_reports_its_status() {
    let session = outbound(Answer::reject(DATA_SM_RESP, 0x0000_00FE), LONG_TIMER).await;
    let pdu = smpp34::data_sm::new(
        0,
        String::new(),
        1,
        1,
        "5550100".into(),
        1,
        1,
        "5550199".into(),
        0,
        0,
        0,
    );
    let resp = session
        .settled(session.smsc.send_data_sm_pdu(pdu))
        .await
        .expect("a response");
    capture("data_sm_rejected", &session.sent);
    assert_eq!(
        seen(SmppResp::from_reply(&resp)),
        rejected("ESME_RDELIVERYFAILURE", 0x0000_00FE)
    );
}

#[tokio::test]
async fn a_rejected_cancel_sm_reports_its_status() {
    let session = outbound(Answer::reject(CANCEL_SM_RESP, 0x0000_0011), LONG_TIMER).await;
    let send = session.smsc.send_cancel_sm(
        String::new(),
        "id-1".into(),
        1,
        1,
        "5550100".into(),
        1,
        1,
        "5550199".into(),
    );
    let resp = session.settled(send).await.expect("a response");
    capture("cancel_sm_rejected", &session.sent);
    assert_eq!(
        seen(SmppResp::from_reply(&resp)),
        rejected("ESME_RCANCELFAIL", 0x0000_0011)
    );
}

#[tokio::test]
async fn a_rejected_replace_sm_reports_its_status() {
    let session = outbound(Answer::reject(REPLACE_SM_RESP, 0x0000_0013), LONG_TIMER).await;
    let send = session.smsc.send_replace_sm(
        "id-1".into(),
        1,
        1,
        "5550100".into(),
        String::new(),
        String::new(),
        0,
        0,
        b"new".to_vec(),
    );
    let resp = session.settled(send).await.expect("a response");
    capture("replace_sm_rejected", &session.sent);
    assert_eq!(
        seen(SmppResp::from_reply(&resp)),
        rejected("ESME_RREPLACEFAIL", 0x0000_0013)
    );
}

#[tokio::test]
async fn a_rejected_submit_multi_reports_its_status() {
    let session = outbound(Answer::reject(SUBMIT_MULTI_RESP, 0x0000_0033), LONG_TIMER).await;
    let pdu = smpp34::submit_sm_multi::new(
        0,
        String::new(),
        1,
        1,
        "5550100".into(),
        vec![smpp34::DestAddress::sme("5550199")],
        0,
        0,
        0,
        String::new(),
        String::new(),
        0,
        0,
        0,
        0,
        b"hello".to_vec(),
    );
    let resp = session
        .settled(session.smsc.send_submit_sm_multi_pdu(pdu))
        .await
        .expect("a response");
    capture("submit_multi_rejected", &session.sent);
    assert_eq!(
        seen(SmppResp::from_reply(&resp)),
        rejected("ESME_RINVNUMDESTS", 0x0000_0033)
    );
}

#[tokio::test]
async fn a_rejected_query_sm_reports_its_status_and_no_message_state() {
    let session = outbound(Answer::reject(QUERY_SM_RESP, 0x0000_0067), LONG_TIMER).await;
    let send = session
        .smsc
        .send_query_sm("id-1".into(), 1, 1, "5550100".into());
    let resp = session.settled(send).await.expect("a response");
    capture("query_sm_rejected", &session.sent);
    let resp = QueryResp::from_reply(&resp);
    assert_eq!(resp.message_state, 0);
    assert_eq!(seen(resp), rejected("ESME_RQUERYFAIL", 0x0000_0067));
}

#[tokio::test]
async fn an_answered_query_sm_is_truthy_and_carries_the_state() {
    // §4.8.2 body: message_id, final_date, message_state, error_code.
    let mut body = b"id-1\0".to_vec();
    body.push(0x00); // final_date: NULL
    body.push(0x02); // message_state: DELIVERED
    body.push(0x00); // error_code
    let session = outbound(
        Answer::Respond {
            command_id: QUERY_SM_RESP,
            status: 0,
            body,
        },
        LONG_TIMER,
    )
    .await;
    let send = session
        .smsc
        .send_query_sm("id-1".into(), 1, 1, "5550100".into());
    let resp = QueryResp::from_reply(&session.settled(send).await.expect("a response"));
    capture("query_sm_answered", &session.sent);
    assert_eq!(resp.message_state, 2);
    let seen = seen(resp);
    assert!(seen.truthy && seen.ok);
    assert_eq!(seen.message_id, "id-1");
}

// ── deliver_sm / data_sm to a bound ESME ───────────────────────────────

#[tokio::test]
async fn an_accepted_deliver_sm_is_truthy() {
    // §4.6.2: the body is a NULL message_id.
    let session = inbound(
        Answer::Respond {
            command_id: DELIVER_SM_RESP,
            status: 0,
            body: vec![0x00],
        },
        LONG_TIMER,
    )
    .await;
    let resp = session.deliver_sm().await.expect("a response");
    capture("deliver_sm_accepted", &session.sent);
    let seen = seen(resp);
    assert!(seen.truthy && seen.ok);
    assert_eq!(seen.command_status, "ESME_ROK");
    assert_eq!(seen.message_id, "");
}

#[tokio::test]
async fn a_rejected_deliver_sm_reports_the_status_the_esme_sent() {
    for (name, code) in [
        ("ESME_RX_T_APPN", 0x0000_0064u32),
        ("ESME_RX_P_APPN", 0x0000_0065),
        ("ESME_RX_R_APPN", 0x0000_0066),
        ("ESME_RDELIVERYFAILURE", 0x0000_00FE),
    ] {
        let session = inbound(Answer::reject(DELIVER_SM_RESP, code), LONG_TIMER).await;
        let resp = session.deliver_sm().await.expect("a response");
        capture(&format!("deliver_sm_{name}"), &session.sent);
        assert_eq!(seen(resp), rejected(name, code), "{name}");
    }
}

#[tokio::test]
async fn a_throttled_deliver_sm_is_not_an_accepted_one() {
    let session = inbound(Answer::reject(DELIVER_SM_RESP, 0x0000_0058), LONG_TIMER).await;
    let resp = session.deliver_sm().await.expect("a response");
    capture("deliver_sm_ESME_RTHROTTLED", &session.sent);
    assert_eq!(
        seen(resp),
        Seen {
            throttled: true,
            ..rejected("ESME_RTHROTTLED", 0x0000_0058)
        }
    );
}

#[tokio::test]
async fn a_generic_nack_to_deliver_sm_is_not_a_response() {
    let session = inbound(Answer::Nack(0x0000_0002), LONG_TIMER).await;
    let failure = session.deliver_sm().await.expect_err("no response PDU");
    capture("deliver_sm_generic_nack", &session.sent);
    assert_eq!(failure, SendFailure::Nack(SmppError::ESME_RINVCMDLEN));
}

#[tokio::test]
async fn a_deliver_sm_nobody_answers_is_a_timeout() {
    let session = inbound(Answer::Silent, SHORT_TIMER).await;
    let failure = session.deliver_sm().await.expect_err("no response PDU");
    assert_eq!(failure, SendFailure::Timeout(SHORT_TIMER));
}

#[tokio::test]
async fn an_esme_that_closes_mid_request_is_a_close_not_a_timeout() {
    let session = inbound(Answer::Close, LONG_TIMER).await;
    let started = Instant::now();
    let failure = session.deliver_sm().await.expect_err("no response PDU");
    assert_eq!(failure, SendFailure::Closed);
    assert!(
        started.elapsed() < PROMPT,
        "a close must not be sat out as a timeout"
    );
}

#[tokio::test]
async fn a_rejected_data_sm_to_an_esme_reports_its_status() {
    let session = inbound(Answer::reject(DATA_SM_RESP, 0x0000_0064), LONG_TIMER).await;
    let pdu = smpp34::data_sm::new(
        0,
        String::new(),
        1,
        1,
        "5550199".into(),
        1,
        1,
        "5550100".into(),
        0,
        0,
        0,
    );
    let resp = session
        .settled(session.esme.send_data_sm_pdu(pdu))
        .await
        .expect("a response");
    capture("data_sm_to_esme_rejected", &session.sent);
    assert_eq!(
        seen(SmppResp::from_reply(&resp)),
        rejected("ESME_RX_T_APPN", 0x0000_0064)
    );
}
