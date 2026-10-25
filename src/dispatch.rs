//! Running script handlers for SMPP events.
//!
//! Every event (a PDU, a bind request, an alert, a session going up or down)
//! is handled in **one** job on siphon's Python executor: that job looks the
//! handler up, builds the `Pdu` / `Session` objects, calls the handler, and
//! turns what it returned into Rust data. The tokio task that received the
//! PDU only awaits the job's result. It never attaches to Python.
//!
//! An attach on a tokio worker is what this layout avoids. Under
//! free-threaded CPython a garbage collection stops the world, and a thread
//! that tries to attach while it is stopped parks until it ends. A tokio
//! worker parked like that stops every SMPP session it was polling, and if
//! anything attached is in turn waiting on that worker the process hangs.
//! Attaching and detaching around each step of an event also costs time on
//! every PDU.
//!
//! An `async def` handler starts in the same job: its coroutine is wrapped
//! (see [`AWAIT_HANDLER_SOURCE`]) and handed to siphon's asyncio pool. The
//! wrapper bounds it by the pool's coroutine timeout and converts its result
//! on the loop thread, then sends the Rust value over a oneshot. No executor
//! worker is held while the coroutine waits, and the tokio side awaits that
//! oneshot with nothing attached.
//!
//! Handlers come from the script registry on every event, so a hot reload
//! is picked up on the next PDU. The decorators in `python/smpp.py` store a
//! [`RegisteredHandler`] in each registration's options: the callable, its
//! filter key and whether it is a coroutine function, so matching a command
//! is a Rust string compare.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyModule, PyTuple};
use siphon::script::async_pool::AsyncPool;
use siphon::script::{py_executor, ScriptHandle};
use smpp34::SmppError;
use tokio::sync::oneshot;

use crate::pyclasses::{AlertNotification, Bind, BindResult, Pdu, PduReply, Session};

/// Registry kind of `@smpp.on_pdu` handlers.
pub(crate) const PDU_KIND: &str = "smpp.on_pdu";
/// Registry kind of `@smpp.on_bind` handlers.
pub(crate) const BIND_KIND: &str = "smpp.on_bind";
/// Registry kind of `@smpp.on_session` handlers.
pub(crate) const SESSION_KIND: &str = "smpp.on_session";
/// Options key under which the decorators store the [`RegisteredHandler`].
const HANDLER_OPTION: &str = "_handler";

/// Extra time the tokio side gives a coroutine past the pool's coroutine
/// timeout. The wrapper enforces the timeout on the loop itself; this only
/// fires when the loop is not turning at all.
const SETTLE_BACKSTOP: Duration = Duration::from_secs(5);

// ── Handler registration ────────────────────────────────────────────────

/// A handler as the `smpp` decorators registered it.
///
/// Built once, at registration, so an event needs no option-dict lookup to
/// find its handler: `key` is the `@smpp.on_pdu` command or the
/// `@smpp.on_session` event (none for `@smpp.on_bind`).
#[pyclass(module = "siphon.smpp", name = "_Handler", frozen)]
pub struct RegisteredHandler {
    callable: Py<PyAny>,
    is_async: bool,
    key: Option<String>,
}

#[pymethods]
impl RegisteredHandler {
    #[new]
    #[pyo3(signature = (callable, is_async, key = None))]
    fn new(callable: Py<PyAny>, is_async: bool, key: Option<String>) -> Self {
        Self {
            callable,
            is_async,
            key,
        }
    }

    #[getter]
    fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    #[getter]
    fn is_async(&self) -> bool {
        self.is_async
    }

    fn __repr__(&self) -> String {
        format!("_Handler(key={:?}, is_async={})", self.key, self.is_async)
    }
}

/// Where an event's handlers come from. Called on an executor worker, with
/// the interpreter attached.
pub(crate) trait HandlerSource: Send + Sync + 'static {
    /// The handlers registered under `kind`, in registration order.
    fn handlers(&self, python: Python<'_>, kind: &str) -> Vec<Py<RegisteredHandler>>;
}

/// The handler set a runtime dispatches into.
pub(crate) type Handlers = Arc<dyn HandlerSource>;

impl HandlerSource for ScriptHandle {
    fn handlers(&self, python: Python<'_>, kind: &str) -> Vec<Py<RegisteredHandler>> {
        // `handlers_for` attaches to clone each callable; this runs inside
        // the event's executor job, so that is a nested attach there.
        self.handlers_for(kind)
            .iter()
            .filter_map(|handler| registered(kind, handler.options(python)))
            .collect()
    }
}

/// The [`RegisteredHandler`] stored in a registration's options, if the
/// `smpp` decorators made it.
pub(crate) fn registered(
    kind: &str,
    options: Option<Bound<'_, PyDict>>,
) -> Option<Py<RegisteredHandler>> {
    let entry = options.and_then(|options| options.get_item(HANDLER_OPTION).ok().flatten());
    match entry.map(|entry| entry.cast_into::<RegisteredHandler>()) {
        Some(Ok(handler)) => Some(handler.unbind()),
        _ => {
            static WARNED: AtomicBool = AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(target: "siphon_smpp", kind = %kind,
                    "a handler was registered without the smpp decorators and is skipped; \
                     use @smpp.on_pdu / @smpp.on_bind / @smpp.on_session");
            }
            None
        }
    }
}

// ── Errors ──────────────────────────────────────────────────────────────

/// Why a handler produced no result. Already rendered to text, inside the
/// job, so logging it on a tokio worker does not touch the interpreter
/// (formatting a `PyErr` attaches).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DispatchError(String);

impl DispatchError {
    fn from_python(error: PyErr) -> Self {
        Self(error.to_string())
    }

    fn from_job_failure(payload: Box<dyn Any + Send>) -> Self {
        let message = payload
            .downcast_ref::<&'static str>()
            .map(|text| text.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "script handler job panicked".to_string());
        Self(message)
    }

    fn message(text: impl Into<String>) -> Self {
        Self(text.into())
    }
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── The executor job ────────────────────────────────────────────────────

/// Attach to Python. The only attach in this module and in `runtime.rs`;
/// it runs inside an executor job (or on an asyncio loop thread), never in
/// a tokio task.
fn attach<T>(job: impl FnOnce(Python<'_>) -> T) -> T {
    #[cfg(test)]
    crate::test_support::record_python_use();
    Python::attach(job)
}

/// Run `job` attached, as one job on siphon's Python executor, and await its
/// result. Shed or panicked jobs come back as an error.
async fn on_executor<T, F>(job: F) -> Result<T, DispatchError>
where
    F: FnOnce(Python<'_>) -> T + Send + 'static,
    T: Send + 'static,
{
    py_executor::try_run(move || attach(job))
        .await
        .map_err(DispatchError::from_job_failure)
}

/// Turns what a handler returned into Rust data. Runs attached.
type Convert<T> = fn(Python<'_>, &Bound<'_, PyAny>) -> T;

/// A handler that was called, with its result or the means to wait for it.
enum Started<T> {
    Done(Result<T, DispatchError>),
    /// A coroutine on the asyncio pool; its converted result arrives here.
    Pending(oneshot::Receiver<Result<T, DispatchError>>, Duration),
}

impl<T> Started<T> {
    /// The handler's result. Waiting for a coroutine attaches nothing.
    async fn settle(self) -> Result<T, DispatchError> {
        match self {
            Self::Done(result) => result,
            Self::Pending(receiver, timeout) => {
                match tokio::time::timeout(timeout + SETTLE_BACKSTOP, receiver).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err(DispatchError::message(
                        "the asyncio loop dropped the handler before it settled",
                    )),
                    Err(_) => Err(DispatchError::message(format!(
                        "async handler did not settle within {}s; its asyncio loop is not turning",
                        (timeout + SETTLE_BACKSTOP).as_secs()
                    ))),
                }
            }
        }
    }
}

/// Call `handler` with `args`. A sync handler's result is converted here; a
/// coroutine is handed to the asyncio pool.
fn invoke<T: Send + 'static>(
    python: Python<'_>,
    handler: &RegisteredHandler,
    args: Bound<'_, PyTuple>,
    convert: Convert<T>,
) -> Started<T> {
    let returned = match handler.callable.bind(python).call1(args) {
        Ok(returned) => returned,
        Err(error) => return Started::Done(Err(DispatchError::from_python(error))),
    };
    if !handler.is_async {
        return Started::Done(Ok(convert(python, &returned)));
    }
    match submit_coroutine(python, &returned, convert) {
        Ok(started) => started,
        Err(error) => {
            // Never awaited now; close it so it does not warn at collection.
            let _ = returned.call_method0("close");
            Started::Done(Err(DispatchError::from_python(error)))
        }
    }
}

/// Wraps a handler's coroutine for the asyncio pool: bounds it by the pool's
/// coroutine timeout (cancelling it on expiry) and reports exactly once
/// through `completion`, on the loop thread.
const AWAIT_HANDLER_SOURCE: &str = r#"
import asyncio


async def await_handler(coroutine, completion, timeout):
    scope = asyncio.timeout(timeout)
    try:
        async with scope:
            value = await coroutine
    except BaseException as error:
        if scope.expired():
            name = getattr(coroutine, "__qualname__", "handler")
            completion.fail(TimeoutError(
                f"async handler {name} did not settle within {timeout:g}s"))
        else:
            completion.fail(error)
        if not isinstance(error, Exception):
            raise
        return None
    completion.done(value)
"#;

fn await_handler(python: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static HELPER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    HELPER
        .get_or_try_init(python, || {
            let source = std::ffi::CString::new(AWAIT_HANDLER_SOURCE)?;
            let module = PyModule::from_code(
                python,
                source.as_c_str(),
                c"siphon_smpp_await_handler.py",
                c"siphon_smpp_await_handler",
            )?;
            Ok::<_, PyErr>(module.getattr("await_handler")?.unbind())
        })
        .map(|helper| helper.bind(python))
}

fn submit_coroutine<T: Send + 'static>(
    python: Python<'_>,
    coroutine: &Bound<'_, PyAny>,
    convert: Convert<T>,
) -> PyResult<Started<T>> {
    let pool = AsyncPool::global().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "async handler with no asyncio pool installed; siphon installs one at startup",
        )
    })?;
    let timeout = pool.coroutine_timeout();
    let (sender, receiver) = oneshot::channel();
    let completion = Completion::new(Box::new(move |python, outcome| {
        let result = match outcome {
            Ok(value) => Ok(convert(python, &value)),
            Err(error) => Err(DispatchError::from_python(error)),
        };
        // The caller may have given up waiting; nothing left to tell.
        let _ = sender.send(result);
    }));
    let wrapped = await_handler(python)?.call1((coroutine, completion, timeout.as_secs_f64()))?;
    // The pool's own receiver and future are dropped here, attached: the
    // result travels over `receiver` instead, already converted.
    if let Err(error) = pool.submit(python, &wrapped) {
        let _ = wrapped.call_method0("close");
        return Err(error);
    }
    Ok(Started::Pending(receiver, timeout))
}

type Finish = Box<dyn for<'py> FnOnce(Python<'py>, Result<Bound<'py, PyAny>, PyErr>) + Send>;

/// Reports a coroutine's outcome from the asyncio loop thread. Called once.
#[pyclass(module = "siphon.smpp", name = "_Completion", frozen)]
struct Completion {
    // Taken once, by `done` or `fail`, on the loop thread; never contended.
    finish: StdMutex<Option<Finish>>,
}

impl Completion {
    fn new(finish: Finish) -> Self {
        #[cfg(test)]
        probe::completion_created();
        Self {
            finish: StdMutex::new(Some(finish)),
        }
    }

    fn take(&self) -> Option<Finish> {
        match self.finish.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        }
    }
}

#[cfg(test)]
impl Drop for Completion {
    fn drop(&mut self) {
        probe::completion_dropped();
    }
}

#[pymethods]
impl Completion {
    fn done(&self, value: Bound<'_, PyAny>) {
        if let Some(finish) = self.take() {
            finish(value.py(), Ok(value));
        }
    }

    fn fail(&self, error: Bound<'_, PyAny>) {
        if let Some(finish) = self.take() {
            let python = error.py();
            finish(python, Err(PyErr::from_value(error)));
        }
    }
}

fn first_matching(
    python: Python<'_>,
    source: &dyn HandlerSource,
    kind: &str,
    key: Option<&str>,
) -> Option<Py<RegisteredHandler>> {
    source
        .handlers(python, kind)
        .into_iter()
        .find(|handler| key.is_none() || handler.get().key.as_deref() == key)
}

fn arguments<'py, T>(
    python: Python<'py>,
    first: T,
    session: Option<Session>,
) -> PyResult<Bound<'py, PyTuple>>
where
    T: pyo3::PyClass + Into<pyo3::PyClassInitializer<T>>,
{
    let first = Bound::new(python, first)?.into_any();
    match session {
        Some(session) => PyTuple::new(python, [first, Bound::new(python, session)?.into_any()]),
        None => PyTuple::new(python, [first]),
    }
}

// ── Per-event dispatch ──────────────────────────────────────────────────

/// What a `@smpp.on_pdu` handler's return value means as a reply.
fn pdu_reply(_python: Python<'_>, value: &Bound<'_, PyAny>) -> PduReply {
    if value.is_none() {
        return PduReply::default_ok();
    }
    value.extract::<PduReply>().unwrap_or_else(|_| {
        tracing::warn!(target: "siphon_smpp",
            "smpp.on_pdu handler returned a non-PduReply value; defaulting to ESME_ROK");
        PduReply::default_ok()
    })
}

/// Call the `@smpp.on_pdu(command)` handler. `None` when none is registered.
pub(crate) async fn pdu(
    handlers: &Handlers,
    command: &'static str,
    pdu: Pdu,
    session: Session,
) -> Option<Result<PduReply, DispatchError>> {
    let source = Arc::clone(handlers);
    let started = on_executor(move |python| {
        let handler = first_matching(python, &*source, PDU_KIND, Some(command))?;
        Some(match arguments(python, pdu, Some(session)) {
            Ok(args) => invoke(python, handler.get(), args, pdu_reply),
            Err(error) => Started::Done(Err(DispatchError::from_python(error))),
        })
    })
    .await;
    match started {
        Err(error) => Some(Err(error)),
        Ok(None) => None,
        Ok(Some(started)) => Some(started.settle().await),
    }
}

/// Call the `@smpp.on_pdu("alert_notification")` handler; its return value
/// is ignored. `Ok` when none is registered.
pub(crate) async fn alert(
    handlers: &Handlers,
    alert: AlertNotification,
    session: Session,
) -> Result<(), DispatchError> {
    let source = Arc::clone(handlers);
    let started = on_executor(move |python| {
        let handler = first_matching(python, &*source, PDU_KIND, Some("alert_notification"))?;
        Some(match arguments(python, alert, Some(session)) {
            Ok(args) => invoke(python, handler.get(), args, |_, _| ()),
            Err(error) => Started::Done(Err(DispatchError::from_python(error))),
        })
    })
    .await?;
    match started {
        None => Ok(()),
        Some(started) => started.settle().await,
    }
}

/// Outcome of an `@smpp.on_bind` dispatch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BindOutcome {
    pub accept: bool,
    pub status: SmppError,
    pub reason: String,
}

impl BindOutcome {
    fn reject(reason: impl Into<String>) -> Self {
        Self {
            accept: false,
            status: SmppError::ESME_RBINDFAIL,
            reason: reason.into(),
        }
    }
}

/// What an `@smpp.on_bind` handler's return value means.
fn bind_outcome(_python: Python<'_>, value: &Bound<'_, PyAny>) -> BindOutcome {
    if value.is_none() {
        // No explicit return is a rejection; forces explicit accept/reject
        // in scripts, no accidental open binds.
        return BindOutcome::reject("handler returned None");
    }
    if let Ok(result) = value.extract::<BindResult>() {
        return BindOutcome {
            accept: result.accept,
            status: result.status,
            reason: result.reason,
        };
    }
    // Back-compat: a bare truthy/falsy return.
    match value.is_truthy() {
        Ok(true) => BindOutcome {
            accept: true,
            status: SmppError::ESME_ROK,
            reason: String::new(),
        },
        _ => BindOutcome::reject("handler returned a non-truthy value"),
    }
}

/// Ask the `@smpp.on_bind` handler about a bind. No handler, no return, or a
/// raised exception rejects (closed by default; the script is the authority
/// on credentials).
pub(crate) async fn bind(handlers: &Handlers, bind: Bind) -> BindOutcome {
    let source = Arc::clone(handlers);
    let started = on_executor(move |python| {
        let handler = first_matching(python, &*source, BIND_KIND, None)?;
        Some(match arguments(python, bind, None) {
            Ok(args) => invoke(python, handler.get(), args, bind_outcome),
            Err(error) => Started::Done(Err(DispatchError::from_python(error))),
        })
    })
    .await;
    match started {
        Err(error) => BindOutcome::reject(format!("@smpp.on_bind did not run: {error}")),
        Ok(None) => BindOutcome::reject("no @smpp.on_bind handler registered"),
        Ok(Some(started)) => match started.settle().await {
            Ok(outcome) => outcome,
            Err(error) => BindOutcome::reject(format!("@smpp.on_bind raised: {error}")),
        },
    }
}

/// The `@smpp.on_session` handlers still to run for one event.
struct SessionRun {
    event: &'static str,
    session: Session,
    remaining: std::vec::IntoIter<Py<RegisteredHandler>>,
}

/// Where a run of session handlers stopped.
enum SessionStep {
    Finished,
    /// Waiting for an `async def` handler; the rest run after it.
    Awaiting(Started<()>, SessionRun),
}

/// Run session handlers in order until one is a coroutine that has to wait.
fn run_session(python: Python<'_>, mut run: SessionRun) -> SessionStep {
    while let Some(handler) = run.remaining.next() {
        let started = match arguments(python, run.session.clone(), None) {
            Ok(args) => invoke(python, handler.get(), args, |_, _| ()),
            Err(error) => Started::Done(Err(DispatchError::from_python(error))),
        };
        match started {
            Started::Done(Ok(())) => {}
            Started::Done(Err(error)) => log_session_error(run.event, &error),
            pending @ Started::Pending(..) => return SessionStep::Awaiting(pending, run),
        }
    }
    SessionStep::Finished
}

fn log_session_error(event: &str, error: &DispatchError) {
    tracing::error!(target: "siphon_smpp",
        event = %event, error = %error, "@smpp.on_session raised");
}

/// Run every `@smpp.on_session(event)` handler, in registration order, each
/// finishing before the next starts. Best effort: failures are logged, never
/// propagated (lifecycle hooks must not break the runtime).
pub(crate) async fn session(handlers: &Handlers, event: &'static str, session: Session) {
    let source = Arc::clone(handlers);
    let mut step = on_executor(move |python| {
        let remaining: Vec<_> = source
            .handlers(python, SESSION_KIND)
            .into_iter()
            .filter(|handler| handler.get().key.as_deref() == Some(event))
            .collect();
        run_session(
            python,
            SessionRun {
                event,
                session,
                remaining: remaining.into_iter(),
            },
        )
    })
    .await;
    loop {
        match step {
            Err(error) => return log_session_error(event, &error),
            Ok(SessionStep::Finished) => return,
            Ok(SessionStep::Awaiting(pending, run)) => {
                if let Err(error) = pending.settle().await {
                    log_session_error(event, &error);
                }
                step = on_executor(move |python| run_session(python, run)).await;
            }
        }
    }
}

// ── Benchmark access ────────────────────────────────────────────────────

/// Entry points for `benches/dispatch.rs`. Not a supported API.
#[doc(hidden)]
pub mod bench_support {
    use super::*;

    /// A fixed set of handlers taken from a script's `smpp` decorators.
    pub struct ScriptHandlers(pub(crate) Handlers);

    struct Fixed(Vec<(String, Py<RegisteredHandler>)>);

    impl HandlerSource for Fixed {
        fn handlers(&self, python: Python<'_>, kind: &str) -> Vec<Py<RegisteredHandler>> {
            self.0
                .iter()
                .filter(|(registered_kind, _)| registered_kind == kind)
                .map(|(_, handler)| handler.clone_ref(python))
                .collect()
        }
    }

    impl ScriptHandlers {
        /// `registrations` is what the decorators passed to the registry:
        /// `(kind, options)` pairs.
        pub fn from_registrations(registrations: &Bound<'_, PyAny>) -> PyResult<Self> {
            let mut handlers = Vec::new();
            for item in registrations.try_iter()? {
                let (kind, options): (String, Option<Bound<'_, PyDict>>) = item?.extract()?;
                if let Some(handler) = registered(&kind, options) {
                    handlers.push((kind, handler));
                }
            }
            Ok(Self(Arc::new(Fixed(handlers))))
        }

        /// Dispatch a PDU to its handler; the reply's status, or `None` when
        /// no handler matched or it raised.
        pub async fn pdu(
            &self,
            command: &'static str,
            pdu: Pdu,
            session: Session,
        ) -> Option<SmppError> {
            match super::pdu(&self.0, command, pdu, session).await {
                Some(Ok(reply)) => Some(reply.command_status),
                _ => None,
            }
        }
    }
}

// ── Test probes ─────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Live `Completion` objects: back to its starting value once every
    /// coroutine has settled.
    pub(crate) static LIVE_COMPLETIONS: AtomicUsize = AtomicUsize::new(0);

    pub(crate) fn completion_created() {
        LIVE_COMPLETIONS.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn completion_dropped() {
        LIVE_COMPLETIONS.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
pub(crate) mod tests;
