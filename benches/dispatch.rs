//! Per-event cost of running a script handler: an SMPP event handed to its
//! `@smpp.on_pdu` handler and the reply brought back as Rust data.
//!
//! Runs on siphon's real Python executor and asyncio pool, with handlers
//! registered by the real `smpp` decorators (into a stand-in registry, as
//! the engine's own exists only inside a running server). The tokio workers
//! keep a persistent Python thread state, as siphon's do in production, so
//! an attach there costs what it costs in a server.
//!
//!   * `deliver_sm_sync`  — a `def` handler returning `pdu.reply()`
//!   * `submit_sm_async`  — an `async def` handler returning a reply
//!
//! Each iteration is one event, dispatched from inside a tokio task the way
//! an SMPP listener does it.
//!
//! Run: `cargo bench --bench dispatch`

use std::ffi::CString;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use siphon::script::async_pool::AsyncPool;
use siphon::script::py_executor::{ExecutorConfig, PyExecutor};
use siphon_smpp::bench_support::ScriptHandlers;
use siphon_smpp::pyclasses::{Pdu, Session, SourceKind};
use siphon_smpp::SmppConfig;
use smpp34::deliver_sm;

const REGISTRY_SOURCE: &str = r#"
entries = []

def register(kind, filter, fn, is_async, options):
    entries.append((kind, options))
"#;

const SCRIPT: &str = r#"
@smpp.on_pdu("deliver_sm")
def on_deliver(pdu, session):
    return pdu.reply()

@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    return pdu.reply(message_id="m-1")
"#;

fn runtime() -> tokio::runtime::Runtime {
    Python::initialize();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_thread_start(|| {
            // What siphon does for its tokio workers: attach once and keep
            // the thread state, so later attaches are cheap re-attaches.
            // SAFETY: the thread state is never released; these threads
            // live until the bench process exits.
            unsafe {
                let _state = pyo3::ffi::PyGILState_Ensure();
                let _thread = pyo3::ffi::PyEval_SaveThread();
            }
        })
        .build()
        .expect("bench runtime");
    PyExecutor::install(
        runtime.handle().clone(),
        ExecutorConfig {
            core_threads: 4,
            max_threads: 4,
            queue_capacity: 4096,
            stall_abort: None,
        },
    );
    AsyncPool::install(2, runtime.handle().clone(), Duration::from_secs(10));
    runtime
}

fn handlers() -> ScriptHandlers {
    Python::attach(|python| -> PyResult<ScriptHandlers> {
        let registry = PyModule::from_code(
            python,
            &CString::new(REGISTRY_SOURCE)?,
            c"_siphon_registry.py",
            c"_siphon_registry",
        )?;
        python
            .import("sys")?
            .getattr("modules")?
            .set_item("_siphon_registry", &registry)?;
        let config = SmppConfig::from_yaml("{}").expect("empty config");
        let smpp = siphon_smpp::namespace(config)(python)?;
        let globals = PyDict::new(python);
        globals.set_item("smpp", smpp)?;
        python.run(&CString::new(SCRIPT)?, Some(&globals), None)?;
        let entries = registry.getattr("entries")?.cast_into::<PyList>()?;
        ScriptHandlers::from_registrations(entries.as_any())
    })
    .expect("bench script registers")
}

fn pdu() -> Pdu {
    Pdu::from_deliver(&deliver_sm::new(
        1,
        String::new(),
        1,
        1,
        "5550101".to_string(),
        1,
        1,
        "5550199".to_string(),
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
    ))
}

fn session() -> Session {
    Session {
        kind: SourceKind::Bind,
        session_id: "bench-session".to_string(),
        system_id: "bench_bind".to_string(),
        client_addr: "192.0.2.10:2775".to_string(),
    }
}

fn bench_dispatch(c: &mut Criterion) {
    let runtime = runtime();
    let handlers = std::sync::Arc::new(handlers());
    let mut group = c.benchmark_group("dispatch");
    group.throughput(Throughput::Elements(1));

    for (name, command) in [
        ("deliver_sm_sync", "deliver_sm"),
        ("submit_sm_async", "submit_sm"),
    ] {
        group.bench_function(name, |b| {
            b.iter_custom(|iterations| {
                let handlers = std::sync::Arc::clone(&handlers);
                // The bench thread waits for the timed task; it is not attached.
                #[allow(clippy::disallowed_methods)]
                runtime.block_on(async move {
                    tokio::spawn(async move {
                        let started = Instant::now();
                        for _ in 0..iterations {
                            let status = handlers.pdu(command, pdu(), session()).await;
                            assert!(status.is_some(), "handler did not answer");
                        }
                        started.elapsed()
                    })
                    .await
                    .expect("bench task")
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dispatch);
criterion_main!(benches);
