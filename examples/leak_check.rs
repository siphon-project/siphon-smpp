//! Memory-leak check for siphon-smpp's per-PDU hot paths.
//!
//! A counting global allocator tracks **live bytes** (allocated − freed) —
//! RSS is too noisy (the OS/allocator retains freed pages), but live bytes
//! are exact, so a real leak shows up as monotonic growth.
//!
//! The live SMPP runtime is socket- and Python-bound (and smpp34 has its
//! own `leak_check`), so this focuses on the allocation surface siphon-smpp
//! itself owns and touches on every message:
//!
//!   1. **mapping** — wire PDU → script-facing `Pdu` (`from_submit` /
//!      `from_deliver`), the conversion done for every inbound PDU.
//!   2. **receipt** — parse a delivery-receipt body (`Receipt::parse`),
//!      done for every DLR.
//!   3. **config** — parse the `smpp.yaml` schema, done at boot/reload.
//!
//! Each phase warms up, then asserts live bytes return to a flat baseline
//! over many cycles. Exits non-zero on a leak. Driven by
//! `scripts/mem_leak_test.sh`.
//!
//! Run: `cargo run --release --example leak_check`

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use siphon_smpp::pyclasses::Pdu;
use siphon_smpp::{Receipt, SmppConfig};
use smpp34::deliver_sm;

// ── Counting allocator ──────────────────────────────────────────────────────
static LIVE: AtomicI64 = AtomicI64::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            LIVE.fetch_add(l.size() as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        LIVE.fetch_sub(l.size() as i64, Ordering::Relaxed);
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(l);
        if !p.is_null() {
            LIVE.fetch_add(l.size() as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn realloc(&self, ptr: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, l, new_size);
        if !p.is_null() {
            LIVE.fetch_add(new_size as i64 - l.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> i64 {
    LIVE.load(Ordering::Relaxed)
}

// ── Workload ────────────────────────────────────────────────────────────────

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

/// A delivery-receipt deliver_sm (esm_class 0x04) carrying a canonical
/// receipt body — the DLR fast path.
fn dlr() -> deliver_sm {
    deliver_sm::new(
        1,
        String::new(),
        1,
        1,
        "15550101".to_string(),
        1,
        1,
        "15550199".to_string(),
        0x04, // delivery receipt
        0,
        0,
        String::new(),
        String::new(),
        0,
        0,
        0,
        0,
        b"id:0a1b2c3d4e sub:001 dlvrd:001 submit date:2401011200 \
          done date:2401011201 stat:DELIVRD err:000 text:leak check"
            .to_vec(),
    )
}

/// A plain MO deliver_sm (no receipt) — the content fast path.
fn mo() -> deliver_sm {
    deliver_sm::new(
        2,
        String::new(),
        1,
        1,
        "15550101".to_string(),
        1,
        1,
        "15550199".to_string(),
        0,
        0,
        0,
        String::new(),
        String::new(),
        0,
        0,
        0,
        0,
        b"a typical mobile-originated message body".to_vec(),
    )
}

fn mapping_cycle(iters: usize) {
    let d_dlr = dlr();
    let d_mo = mo();
    for _ in 0..iters {
        let p1 = Pdu::from_deliver(&d_dlr);
        std::hint::black_box(p1.esm_class & 0x04 != 0);
        std::hint::black_box(Receipt::parse(&p1.short_message));
        let p2 = Pdu::from_deliver(&d_mo);
        std::hint::black_box(Receipt::parse(&p2.short_message));
        std::hint::black_box(p2);
    }
}

fn config_cycle(iters: usize) {
    for _ in 0..iters {
        let cfg: SmppConfig = serde_yaml::from_str(SAMPLE_YAML).unwrap();
        std::hint::black_box(cfg.listen());
        std::hint::black_box(cfg);
    }
}

fn report(phase: &str, base: i64) -> i64 {
    let growth = live() - base;
    println!("  {phase}: live = {} bytes (Δ {:+})", live(), growth);
    growth
}

fn main() {
    const MAP_ITERS: usize = 200_000;
    const MAP_CYCLES: usize = 10;
    const MAP_BUDGET: i64 = 256 * 1024;
    const CFG_ITERS: usize = 20_000;
    const CFG_CYCLES: usize = 10;
    const CFG_BUDGET: i64 = 256 * 1024;

    println!("[mapping] {MAP_CYCLES} x {MAP_ITERS} from_deliver + receipt parse");
    mapping_cycle(MAP_ITERS); // warm up
    let map_base = live();
    for c in 1..=MAP_CYCLES {
        mapping_cycle(MAP_ITERS);
        report(&format!("cycle {c:>2}/{MAP_CYCLES}"), map_base);
    }
    let map_growth = live() - map_base;

    println!("\n[config] {CFG_CYCLES} x {CFG_ITERS} smpp.yaml parses");
    config_cycle(CFG_ITERS); // warm up
    let cfg_base = live();
    for c in 1..=CFG_CYCLES {
        config_cycle(CFG_ITERS);
        report(&format!("cycle {c:>2}/{CFG_CYCLES}"), cfg_base);
    }
    let cfg_growth = live() - cfg_base;

    println!();
    let mut ok = true;
    if map_growth > MAP_BUDGET {
        eprintln!("FAIL: mapping live bytes grew {map_growth} (> {MAP_BUDGET})");
        ok = false;
    }
    if cfg_growth > CFG_BUDGET {
        eprintln!("FAIL: config live bytes grew {cfg_growth} (> {CFG_BUDGET})");
        ok = false;
    }
    if !ok {
        std::process::exit(1);
    }
    println!("PASS: mapping Δ {map_growth} ≤ {MAP_BUDGET}; config Δ {cfg_growth} ≤ {CFG_BUDGET}");
}
