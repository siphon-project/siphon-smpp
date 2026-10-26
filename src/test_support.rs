//! Shared harness for the tests that run handlers and sends on siphon's real
//! Python executor and asyncio pool.
//!
//! The tokio runtime marks its worker threads the way siphon's does (its
//! `on_thread_park` hook only runs on workers, never on the blocking pool), so
//! a test can count interpreter touches that happen on a worker. That count is
//! what the attach-free paths are held to: zero.

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use siphon::script::async_pool::AsyncPool;
use siphon::script::py_executor::{ExecutorConfig, PyExecutor};

/// The asyncio pool's coroutine timeout, kept short for the timeout test.
pub(crate) const COROUTINE_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const EXECUTOR_THREADS: usize = 4;

thread_local! {
    static TOKIO_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// Interpreter touches made on a tokio worker thread.
static ON_TOKIO_WORKER: AtomicUsize = AtomicUsize::new(0);

fn mark_tokio_worker() {
    TOKIO_WORKER.with(|worker| worker.set(true));
}

/// Called wherever this crate touches the interpreter.
pub(crate) fn record_python_use() {
    if TOKIO_WORKER.with(Cell::get) {
        ON_TOKIO_WORKER.fetch_add(1, Ordering::SeqCst);
    }
}

/// How many times this crate touched the interpreter on a tokio worker.
pub(crate) fn python_uses_on_tokio_workers() -> usize {
    ON_TOKIO_WORKER.load(Ordering::SeqCst)
}

/// One runtime and one set of pools for the whole test binary: the pools
/// are process-wide and hold the runtime handle for their lifetime.
pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    static ASYNC_RUNTIME: std::sync::Once = std::sync::Once::new();
    let runtime = RUNTIME.get_or_init(|| {
        Python::initialize();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("smpp-test-tokio")
            .on_thread_park(mark_tokio_worker)
            .enable_all()
            .build()
            .expect("test runtime");
        PyExecutor::install(
            runtime.handle().clone(),
            ExecutorConfig {
                core_threads: EXECUTOR_THREADS,
                max_threads: EXECUTOR_THREADS,
                queue_capacity: 4096,
                stall_abort: None,
            },
        );
        AsyncPool::install(2, runtime.handle().clone(), COROUTINE_TIMEOUT);
        runtime
    });
    // The runtime the send helpers' awaitables run their futures on, as in
    // a server.
    ASYNC_RUNTIME.call_once(|| {
        let _ = pyo3_async_runtimes::tokio::init_with_runtime(runtime);
    });
    runtime
}

/// Run `future` as a tokio task (not as the `block_on` future, which is not
/// a task and does not run on a worker) and wait for it.
pub(crate) fn in_task<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = runtime();
    // The test thread waits here; it is not attached to the interpreter.
    runtime.block_on(async move { tokio::spawn(future).await.expect("task") })
}

/// Held by the tests that measure time or load the interpreter, so they do
/// not run alongside each other.
pub(crate) fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A thread that keeps forcing full collections (each one a stop-the-world
/// on a free-threaded build) until `stop` is set. Returns how many it ran.
pub(crate) fn collect_garbage_until(
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<PyResult<usize>> {
    std::thread::spawn(move || {
        let mut collections = 0;
        while !stop.load(Ordering::SeqCst) {
            Python::attach(|python| -> PyResult<()> {
                let ballast = python.eval(c"[[i] for i in range(20000)]", None, None)?;
                python.import("gc")?.call_method0("collect")?;
                drop(ballast);
                Ok(())
            })?;
            collections += 1;
            // Released between collections: on a GIL build a back-to-back
            // loop would starve every other thread, which is not what the
            // tests using this are about.
            Python::attach(|python| -> PyResult<()> {
                python.import("time")?.call_method1("sleep", (0.002,))?;
                Ok(())
            })?;
        }
        Ok(collections)
    })
}

/// Set (to anything) where the suite must run on a free-threaded
/// interpreter, as CI does: a GIL build then fails the stop-the-world tests
/// instead of skipping them.
const REQUIRE_FREE_THREADED: &str = "SIPHON_SMPP_REQUIRE_FREE_THREADED";

/// Whether the linked interpreter is free-threaded with the GIL off, which
/// is the only place a garbage collection stops the world.
fn free_threaded() -> PyResult<bool> {
    Python::initialize();
    Python::attach(|python| {
        let built_without_gil = python
            .import("sysconfig")?
            .call_method1("get_config_var", ("Py_GIL_DISABLED",))?
            .extract::<Option<i64>>()?
            == Some(1);
        let sys = python.import("sys")?;
        // A free-threaded build can still run with the GIL re-enabled
        // (PYTHON_GIL=1, or an extension that is not marked safe).
        let gil_enabled = if sys.hasattr("_is_gil_enabled")? {
            sys.call_method0("_is_gil_enabled")?.extract::<bool>()?
        } else {
            true
        };
        Ok(built_without_gil && !gil_enabled)
    })
}

/// Gate for a test whose whole point is the free-threaded stop-the-world.
/// Returns `true` when it can run. On a GIL interpreter it says so on the
/// real stderr (past the harness's capture, so it shows in every run) and
/// returns `false`: the test must then return without asserting anything,
/// because passing there would claim a deadlock check that never happened.
/// With `SIPHON_SMPP_REQUIRE_FREE_THREADED` set it fails instead.
pub(crate) fn free_threaded_or_skip(test: &str) -> bool {
    if free_threaded().expect("the interpreter can be inspected") {
        return true;
    }
    assert!(
        std::env::var_os(REQUIRE_FREE_THREADED).is_none(),
        "{test} needs a free-threaded interpreter with the GIL disabled \
         ({REQUIRE_FREE_THREADED} is set)"
    );
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "SKIP (not free-threaded): {test}");
    false
}

const CHILD_MARKER: &str = "SIPHON_SMPP_TEST_CHILD";

/// Run the calling test in a child copy of the test binary, killed once it
/// outlives `budget`: a stop-the-world that never ends hangs instead of
/// failing, and a test that installs process-wide state needs a process of
/// its own. Returns `true` in the child, which then runs the scenario.
pub(crate) fn in_child(test_path: &str, budget: Duration) -> bool {
    if std::env::var_os(CHILD_MARKER).is_some() {
        return true;
    }
    let filter = test_path
        .split_once("::")
        .map_or(test_path, |(_, rest)| rest);
    let executable = std::env::current_exe().expect("the test binary path");
    let mut child = std::process::Command::new(executable)
        .args([filter, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_MARKER, "1")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("the scenario child starts");
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("the child can be polled") {
            let mut output = String::new();
            if let Some(mut stdout) = child.stdout.take() {
                let _ = std::io::Read::read_to_string(&mut stdout, &mut output);
            }
            assert!(
                status.success(),
                "{filter} failed in its child ({status}):\n{output}"
            );
            assert!(
                output.contains(&format!("test {filter} ... ok")),
                "{filter} did not run in its child:\n{output}"
            );
            return false;
        }
        if started.elapsed() > budget {
            // By PID: only this child.
            let _ = child.kill();
            let _ = child.wait();
            panic!("{filter} hung for {budget:?}: the stop-the-world never came back");
        }
        // The test thread polling its child; never attached.
        std::thread::sleep(Duration::from_millis(20));
    }
}
