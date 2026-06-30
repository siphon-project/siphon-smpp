//! SMPP load harness for a siphon-smpp SMSC.
//!
//! Two modes, both built on `smpp34` (the same wire codec siphon-smpp uses):
//!
//!   * `drive`  — bind a transceiver ESME and flood `submit_sm`, then report
//!                throughput + submit→resp latency percentiles. Point it at a
//!                real `siphon-bin --features smpp` running `examples/echo.py`,
//!                or at this harness's own `serve` mock.
//!   * `serve`  — a minimal mock SMSC (accept any bind, ack every `submit_sm`
//!                with a generated message_id). Lets you smoke-test the driver
//!                — and CI — without standing up siphon.
//!
//! Self-test (no siphon):
//!   smpp-load serve --port 2775 &
//!   smpp-load drive --port 2775 --count 50000 --window 64
//!
//! Real load test (the thing this exists for):
//!   # term 1: your siphon-bin with the smpp feature + echo.py
//!   smpp-load drive --host <siphon-host> --port 2775 --count 1000000 --window 128

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::server::{SmppServer, SmppServerListener};
use smpp34::{bind_transceiver, bind_transceiver_resp, submit_sm, submit_sm_resp};
use smpp34::SmppConnectionInformation;
use async_trait::async_trait;
use tokio::sync::{oneshot, Mutex, Semaphore};

#[derive(Parser)]
#[command(name = "smpp-load", about = "SMPP load driver + mock SMSC for siphon-smpp")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Flood submit_sm at a bound SMSC and report throughput + latency.
    Drive(DriveArgs),
    /// Run a mock SMSC that acks every submit_sm (for self-test / CI).
    Serve(ServeArgs),
}

#[derive(Parser)]
struct DriveArgs {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 2775)]
    port: u16,
    #[arg(long, default_value = "load")]
    system_id: String,
    #[arg(long, default_value = "load")]
    password: String,
    /// Total submit_sm to send.
    #[arg(long, default_value_t = 50_000)]
    count: usize,
    /// Max in-flight submits (the SMPP window).
    #[arg(long, default_value_t = 64)]
    window: usize,
    /// Source address (synthetic).
    #[arg(long, default_value = "15550100")]
    source_addr: String,
    /// Destination address (synthetic).
    #[arg(long, default_value = "15550199")]
    destination_addr: String,
    /// Message body length in bytes.
    #[arg(long, default_value_t = 32)]
    body_len: usize,
}

#[derive(Parser)]
struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 2775)]
    port: u16,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    match Cli::parse().cmd {
        Cmd::Serve(a) => serve(a).await,
        Cmd::Drive(a) => drive(a).await,
    }
}

// ── Mock SMSC ───────────────────────────────────────────────────────────────

struct MockSmsc {
    next_id: AtomicU64,
}

#[async_trait]
impl SmppServerListener for MockSmsc {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        req.accept("MOCK-SMSC".to_string(), Some(0x34))
    }

    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_resp {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        req.accept(format!("{id:x}"))
    }
}

async fn serve(a: ServeArgs) {
    let addr: IpAddr = a.host.parse().expect("valid --host");
    let handler = Arc::new(MockSmsc {
        next_id: AtomicU64::new(1),
    });
    let mut server = SmppServer::new(addr, a.port, handler);
    println!("mock SMSC listening on {}:{} (Ctrl-C to stop)", a.host, a.port);
    server.start().await;
    // Park forever — start() spawns the accept loop in a child task.
    std::future::pending::<()>().await;
}

// ── Load driver ─────────────────────────────────────────────────────────────

struct Binder {
    tx: Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
}

#[async_trait]
impl SmppClientListener for Binder {
    async fn on_smsc_bound(&self, smsc: SMSC, _s: &String) {
        if let Some(tx) = self.tx.lock().await.take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }
}

async fn drive(a: DriveArgs) {
    let (tx, rx) = oneshot::channel();
    let binder = Arc::new(Binder {
        tx: Mutex::new(Some(tx)),
    });
    let mut client = SmppClient::new(
        a.host.clone(),
        a.port,
        false,
        BIND_TYPE::TRX,
        a.system_id.clone(),
        a.password.clone(),
        String::new(),
        1,
        1,
        String::new(),
        binder,
        a.window.max(8),
    );
    client.start().await;

    let smsc = match tokio::time::timeout(Duration::from_secs(10), rx).await {
        Ok(Ok(smsc)) => smsc,
        _ => {
            eprintln!(
                "failed to bind to {}:{} within 10s — is the SMSC up and the \
                 system_id/password accepted?",
                a.host, a.port
            );
            std::process::exit(1);
        }
    };
    println!(
        "bound to {}:{} as {:?}; sending {} submit_sm (window {})",
        a.host, a.port, a.system_id, a.count, a.window
    );

    let body = vec![b'x'; a.body_len];
    let sem = Arc::new(Semaphore::new(a.window));
    let latencies = Arc::new(Mutex::new(Vec::<u64>::with_capacity(a.count)));
    let errors = Arc::new(AtomicU64::new(0));

    // Warm up (excluded from the measurement window).
    for _ in 0..a.window.min(a.count) {
        let _ = smsc
            .submit_sm()
            .source_addr(a.source_addr.clone())
            .destination_addr(a.destination_addr.clone())
            .short_message(body.clone())
            .send()
            .await;
    }

    let started = Instant::now();
    let mut handles = Vec::with_capacity(a.count);
    for _ in 0..a.count {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let smsc = smsc.clone();
        let body = body.clone();
        let src = a.source_addr.clone();
        let dst = a.destination_addr.clone();
        let lat = latencies.clone();
        let errs = errors.clone();
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            let r = smsc
                .submit_sm()
                .source_addr(src)
                .destination_addr(dst)
                .short_message(body)
                .send()
                .await;
            match r {
                Ok(_) => lat.lock().await.push(t0.elapsed().as_micros() as u64),
                Err(_) => {
                    errs.fetch_add(1, Ordering::Relaxed);
                }
            }
            drop(permit);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = started.elapsed();

    let _ = smsc.send_unbind().await;
    client.stop().await;

    report(&latencies.lock().await, errors.load(Ordering::Relaxed), elapsed);
}

fn report(latencies: &[u64], errors: u64, elapsed: Duration) {
    let ok = latencies.len() as u64;
    let total = ok + errors;
    let secs = elapsed.as_secs_f64().max(1e-9);
    let cps = ok as f64 / secs;

    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let pct = |p: f64| -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    };

    println!("\n── results ──────────────────────────────");
    println!("  submitted : {total}  ok {ok}  errors {errors}");
    println!("  elapsed   : {:.3}s", secs);
    println!("  throughput: {:.0} submit_sm/s", cps);
    if !sorted.is_empty() {
        println!(
            "  latency   : p50 {:.2}ms  p90 {:.2}ms  p99 {:.2}ms  p999 {:.2}ms  max {:.2}ms",
            pct(50.0) as f64 / 1000.0,
            pct(90.0) as f64 / 1000.0,
            pct(99.0) as f64 / 1000.0,
            pct(99.9) as f64 / 1000.0,
            (*sorted.last().unwrap()) as f64 / 1000.0,
        );
    }
    if errors > 0 {
        std::process::exit(1);
    }
}
