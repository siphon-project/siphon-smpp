//! Criterion benches for siphon-smpp's per-PDU hot paths.
//!
//! The live runtime is socket- and Python-bound, and the SMPP wire codec
//! is benched in smpp34's own suite. What's measured here is the Rust work
//! siphon-smpp adds on top of the codec, on every message:
//!
//!   * `from_deliver` / `from_submit` — wire PDU → script-facing `Pdu`
//!   * `Receipt::parse`               — delivery-receipt body parsing
//!   * `SmppConfig` YAML parse        — boot / hot-reload config load
//!
//! Run: `cargo bench`

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use siphon_smpp::pyclasses::Pdu;
use siphon_smpp::{Receipt, SmppConfig};
use smpp34::{deliver_sm, submit_sm};

const RECEIPT: &[u8] = b"id:0a1b2c3d4e sub:001 dlvrd:001 submit date:2401011200 \
                         done date:2401011201 stat:DELIVRD err:000 text:hello world";

const SAMPLE_YAML: &str = r#"
server:
  bind_address: "0.0.0.0"
  port: 2775
binds:
  - name: alpha
    host: smsc-a.example.net
    port: 2775
    system_id: alpha_esme
    password: secret-a
    max_msg_per_sec: 50
  - name: beta
    host: smsc-b.example.org
    port: 2775
    system_id: beta_esme
    password: secret-b
routing:
  default_chain: ["bind:beta"]
  rules:
    - prefix: "1555"
      name: na
      chain: ["bind:alpha"]
"#;

fn deliver(esm_class: u8, body: &[u8]) -> deliver_sm {
    deliver_sm::new(
        1,
        String::new(),
        1,
        1,
        "15550101".to_string(),
        1,
        1,
        "15550199".to_string(),
        esm_class,
        0,
        0,
        String::new(),
        String::new(),
        0,
        0,
        0,
        0,
        body.to_vec(),
    )
}

fn submit(body: &[u8]) -> submit_sm {
    // submit_sm::decode from a hand-built body keeps the bench free of any
    // private constructor; this mirrors a typical MO submit.
    let mut b = Vec::new();
    b.push(0); // service_type ""
    b.extend_from_slice(&[1, 1]);
    b.extend_from_slice(b"15550101\0");
    b.extend_from_slice(&[1, 1]);
    b.extend_from_slice(b"15550199\0");
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0]);
    b.push(body.len() as u8);
    b.extend_from_slice(body);
    let cmd_len = (16 + b.len()) as u32;
    let mut pdu = Vec::with_capacity(cmd_len as usize);
    pdu.extend_from_slice(&cmd_len.to_be_bytes());
    pdu.extend_from_slice(&0x0000_0004u32.to_be_bytes());
    pdu.extend_from_slice(&0u32.to_be_bytes());
    pdu.extend_from_slice(&1u32.to_be_bytes());
    pdu.extend_from_slice(&b);
    let h = smpp34::CommandHeader::decode(&pdu).unwrap();
    submit_sm::decode(h, &pdu).unwrap()
}

fn bench_mapping(c: &mut Criterion) {
    let mut g = c.benchmark_group("mapping");
    g.throughput(Throughput::Elements(1));

    let d = deliver(0x00, b"a typical mobile-originated message body");
    g.bench_function("from_deliver", |b| {
        b.iter(|| black_box(Pdu::from_deliver(black_box(&d))))
    });

    let s = submit(b"a typical mobile-originated message body");
    g.bench_function("from_submit", |b| {
        b.iter(|| black_box(Pdu::from_submit(black_box(&s))))
    });

    g.finish();
}

fn bench_receipt(c: &mut Criterion) {
    let mut g = c.benchmark_group("receipt");
    g.throughput(Throughput::Elements(1));

    g.bench_function("parse_canonical", |b| {
        b.iter(|| black_box(Receipt::parse(black_box(RECEIPT))))
    });

    // The end-to-end DLR path: map the wire PDU, then parse the receipt.
    let dlr = deliver(0x04, RECEIPT);
    g.bench_function("from_deliver_then_parse", |b| {
        b.iter(|| {
            let p = Pdu::from_deliver(black_box(&dlr));
            black_box(Receipt::parse(&p.short_message))
        })
    });

    g.finish();
}

fn bench_config(c: &mut Criterion) {
    let mut g = c.benchmark_group("config");
    g.throughput(Throughput::Elements(1));
    g.bench_function("parse_yaml", |b| {
        b.iter(|| black_box(serde_yaml::from_str::<SmppConfig>(black_box(SAMPLE_YAML)).unwrap()))
    });
    g.finish();
}

criterion_group!(benches, bench_mapping, bench_receipt, bench_config);
criterion_main!(benches);
