//! Dispatch through siphon's real Python executor and asyncio pool, with
//! handlers registered by the real `smpp` decorators.
//!
//! The registry the decorators write to is a stand-in module (the engine's
//! own is only built inside a running server). Everything after
//! registration (lookup, the executor job, the asyncio hand-over, result
//! conversion) is the production path.

use std::ffi::CString;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pyo3::types::PyList;

use super::bench_support::ScriptHandlers;
use super::*;
use crate::pyclasses::SourceKind;
use crate::test_support::{
    collect_garbage_until, free_threaded_or_skip, in_task, python_uses_on_tokio_workers, runtime,
    serial, COROUTINE_TIMEOUT, EXECUTOR_THREADS,
};
use crate::SmppConfig;

/// Stand-in for the engine's `_siphon_registry`: records what the
/// decorators register, per thread, so tests running in parallel each see
/// their own script's registrations.
const REGISTRY_SOURCE: &str = r#"
import threading
_local = threading.local()

def register(kind, filter, fn, is_async, options):
    _local.entries.append((kind, options))
"#;

/// True when called from inside a tokio task. Handlers call it to prove
/// where they run.
#[pyfunction]
fn in_tokio_task() -> bool {
    tokio::task::try_id().is_some()
}

/// Run `script` with `smpp` in scope and return the handlers its decorators
/// registered.
pub(crate) fn handlers_from(script: &str) -> Handlers {
    runtime();
    Python::attach(|python| {
        // Built once: `from_code` puts the module in `sys.modules` before
        // its body has run, so tests building it at the same time on a
        // free-threaded interpreter would see it without `_local`.
        static REGISTRY: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();
        let registry = REGISTRY
            .get_or_try_init(python, || -> PyResult<Py<PyAny>> {
                let module = PyModule::from_code(
                    python,
                    &CString::new(REGISTRY_SOURCE)?,
                    c"_siphon_registry.py",
                    c"_siphon_registry",
                )?
                .into_any();
                python
                    .import("sys")?
                    .getattr("modules")?
                    .set_item("_siphon_registry", &module)?;
                Ok(module.unbind())
            })?
            .bind(python);
        let entries = PyList::empty(python);
        registry.getattr("_local")?.setattr("entries", &entries)?;

        let config = SmppConfig::from_yaml("{}").expect("empty config");
        let smpp = crate::install::namespace(config)(python)?;
        let globals = PyDict::new(python);
        globals.set_item("smpp", smpp)?;
        globals.set_item("in_tokio_task", wrap_pyfunction!(in_tokio_task, python)?)?;
        python.run(&CString::new(script)?, Some(&globals), None)?;
        Ok::<_, PyErr>(ScriptHandlers::from_registrations(&entries)?.0)
    })
    .expect("script registers its handlers")
}

pub(crate) fn session(id: &str) -> Session {
    Session {
        kind: SourceKind::Bind,
        session_id: id.to_string(),
        system_id: "test_bind".to_string(),
        client_addr: "192.0.2.10:2775".to_string(),
    }
}

pub(crate) fn deliver(body: &str) -> Pdu {
    Pdu::from_deliver(&smpp34::deliver_sm::new(
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
        body.as_bytes().to_vec(),
    ))
}

fn bind_request(system_id: &str, password: &str) -> Bind {
    Bind {
        system_id: system_id.to_string(),
        password: password.to_string(),
        client_addr: "192.0.2.20:40000".to_string(),
    }
}

/// Wait (bounded) for every coroutine's `Completion` to be released.
///
/// On a free-threaded interpreter an object whose last reference goes on
/// another thread is queued for the thread that made it, here an executor
/// worker that may now be idle, and freed when that thread next runs or a
/// collection merges the queues. So each check is preceded by a collection.
fn completions_released() -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        Python::attach(|python| {
            python
                .import("gc")
                .and_then(|gc| gc.call_method0("collect"))
                .map(|_| ())
        })
        .expect("collect");
        if probe::LIVE_COMPLETIONS.load(Ordering::SeqCst) == 0 {
            return true;
        }
        in_task(async { tokio::time::sleep(Duration::from_millis(20)).await });
    }
    false
}

// ── Results ─────────────────────────────────────────────────────────────

#[test]
fn a_sync_handler_sees_the_pdu_and_its_reply_is_the_answer() {
    let handlers = handlers_from(
        r#"
@smpp.on_pdu("deliver_sm")
def on_deliver(pdu, session):
    assert pdu.source_addr == "5550101", pdu.source_addr
    assert session.session_id == "s-1"
    return pdu.reply(command_status="ESME_RX_T_APPN")
"#,
    );
    let reply =
        in_task(async move { pdu(&handlers, "deliver_sm", deliver("hi"), session("s-1")).await });
    let reply = reply.expect("handler registered").expect("handler ran");
    assert_eq!(reply.command_status, SmppError::ESME_RX_T_APPN);
}

#[test]
fn an_async_handler_reply_comes_back_converted() {
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    await asyncio.sleep(0.01)
    return pdu.reply(message_id="m-" + pdu.body.decode())
"#,
    );
    let reply =
        in_task(async move { pdu(&handlers, "submit_sm", deliver("42"), session("s-2")).await });
    let reply = reply.expect("handler registered").expect("handler ran");
    assert_eq!(reply.command_status, SmppError::ESME_ROK);
    assert_eq!(reply.message_id.as_deref(), Some("m-42"));
}

#[test]
fn the_handler_for_the_command_is_chosen_and_none_means_unhandled() {
    let handlers = handlers_from(
        r#"
@smpp.on_pdu("data_sm")
def on_data(pdu, session):
    return pdu.reply(command_status="ESME_RSYSERR")

@smpp.on_pdu("deliver_sm")
def on_deliver(pdu, session):
    return None
"#,
    );
    let (deliver_reply, cancel_reply) = in_task(async move {
        (
            pdu(&handlers, "deliver_sm", deliver("x"), session("s-3")).await,
            pdu(&handlers, "cancel_sm", deliver("x"), session("s-3")).await,
        )
    });
    let deliver_reply = deliver_reply.expect("registered").expect("ran");
    // A handler returning None accepts.
    assert_eq!(deliver_reply.command_status, SmppError::ESME_ROK);
    assert!(cancel_reply.is_none(), "no cancel_sm handler is registered");
}

#[test]
fn a_raising_handler_reports_its_exception_sync_or_async() {
    let handlers = handlers_from(
        r#"
@smpp.on_pdu("deliver_sm")
def on_deliver(pdu, session):
    raise ValueError("sync boom")

@smpp.on_pdu("data_sm")
async def on_data(pdu, session):
    raise KeyError("async boom")
"#,
    );
    let (sync_error, async_error) = in_task(async move {
        (
            pdu(&handlers, "deliver_sm", deliver("x"), session("s-4")).await,
            pdu(&handlers, "data_sm", deliver("x"), session("s-4")).await,
        )
    });
    let sync_error = sync_error.expect("registered").expect_err("raised");
    assert!(sync_error.to_string().contains("sync boom"), "{sync_error}");
    let async_error = async_error.expect("registered").expect_err("raised");
    assert!(
        async_error.to_string().contains("async boom"),
        "{async_error}"
    );
}

#[test]
fn a_coroutine_past_the_timeout_is_cancelled_and_fails_the_event() {
    let _serial = serial();
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    await asyncio.sleep(60)
"#,
    );
    let started = Instant::now();
    let result =
        in_task(async move { pdu(&handlers, "deliver_sm", deliver("x"), session("s-5")).await });
    let error = result.expect("registered").expect_err("timed out");
    assert!(error.to_string().contains("did not settle"), "{error}");
    // Ended by the loop-side timeout, not the backstop.
    assert!(started.elapsed() < COROUTINE_TIMEOUT + SETTLE_BACKSTOP);
}

// ── Bind ────────────────────────────────────────────────────────────────

#[test]
fn on_bind_accepts_rejects_and_closes_by_default() {
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_bind
async def authorise(bind):
    await asyncio.sleep(0)
    if bind.system_id == "good":
        return bind.accept()
    if bind.system_id == "truthy":
        return 1
    if bind.system_id == "silent":
        return None
    return bind.reject("ESME_RINVPASWD", "bad password")
"#,
    );
    let none = handlers_from("");
    let outcomes = in_task(async move {
        let mut outcomes = Vec::new();
        for system_id in ["good", "truthy", "silent", "other"] {
            outcomes.push(bind(&handlers, bind_request(system_id, "pw")).await);
        }
        outcomes.push(bind(&none, bind_request("good", "pw")).await);
        outcomes
    });
    assert!(outcomes[0].accept);
    assert!(outcomes[1].accept, "a bare truthy return accepts");
    assert!(!outcomes[2].accept);
    assert_eq!(outcomes[2].reason, "handler returned None");
    assert!(!outcomes[3].accept);
    assert_eq!(outcomes[3].status, SmppError::ESME_RINVPASWD);
    assert_eq!(outcomes[3].reason, "bad password");
    assert!(!outcomes[4].accept);
    assert_eq!(outcomes[4].reason, "no @smpp.on_bind handler registered");
}

// ── Session and alert ───────────────────────────────────────────────────

#[test]
fn session_handlers_run_in_order_each_finishing_first() {
    let handlers = handlers_from(
        r#"
import asyncio
seen = []

@smpp.on_session("bound")
def first(session):
    seen.append("first")

@smpp.on_session("bound")
async def second(session):
    await asyncio.sleep(0.05)
    seen.append("second")

@smpp.on_session("unbound")
def elsewhere(session):
    seen.append("unbound")

@smpp.on_session("bound")
def broken(session):
    raise RuntimeError("logged, not fatal")

@smpp.on_session("bound")
def third(session):
    seen.append("third:" + session.session_id)

@smpp.on_pdu("alert_notification")
def on_alert(alert, session):
    seen.append("alert:" + alert.source_addr)

@smpp.on_pdu("deliver_sm")
def report(pdu, session):
    return pdu.reply(message_id=",".join(seen))
"#,
    );
    let reply = in_task(async move {
        super::session(&handlers, "bound", session("s-6")).await;
        let notification = AlertNotification {
            source_addr: "5550123".to_string(),
            ..Default::default()
        };
        alert(&handlers, notification, session("s-6"))
            .await
            .expect("alert handler ran");
        pdu(&handlers, "deliver_sm", deliver("x"), session("s-6")).await
    });
    let reply = reply.expect("registered").expect("ran");
    assert_eq!(
        reply.message_id.as_deref(),
        Some("first,second,third:s-6,alert:5550123")
    );
}

// ── Threads ─────────────────────────────────────────────────────────────

#[test]
fn no_event_attaches_to_python_inside_a_tokio_task() {
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_pdu("deliver_sm")
def on_deliver(pdu, session):
    assert not in_tokio_task()
    return pdu.reply()

@smpp.on_pdu("submit_sm")
async def on_submit(pdu, session):
    assert not in_tokio_task()
    await asyncio.sleep(0)
    return pdu.reply(message_id="ok")

@smpp.on_bind
def authorise(bind):
    assert not in_tokio_task()
    return bind.accept()

@smpp.on_session("bound")
async def bound(session):
    assert not in_tokio_task()
"#,
    );
    let before = python_uses_on_tokio_workers();
    let (deliver_reply, submit_reply, outcome) = in_task(async move {
        let deliver_reply = pdu(&handlers, "deliver_sm", deliver("x"), session("s-7")).await;
        let submit_reply = pdu(&handlers, "submit_sm", deliver("x"), session("s-7")).await;
        let outcome = bind(&handlers, bind_request("esme", "pw")).await;
        super::session(&handlers, "bound", session("s-7")).await;
        (deliver_reply, submit_reply, outcome)
    });
    assert!(deliver_reply.expect("registered").is_ok());
    assert!(submit_reply.expect("registered").is_ok());
    assert!(outcome.accept, "{outcome:?}");
    let after = python_uses_on_tokio_workers();
    assert_eq!(
        after - before,
        0,
        "an event attached to Python inside a tokio task"
    );
}

#[test]
fn more_waiting_coroutines_than_executor_threads_all_complete() {
    let _serial = serial();
    // Each coroutine waits 200 ms. If waiting held an executor worker, 64 of
    // them on 4 workers would take 3.2 s; held by nothing, about 0.2 s.
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    await asyncio.sleep(0.2)
    return pdu.reply()
"#,
    );
    let started = Instant::now();
    let completed = in_task(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..64 {
            let handlers = Arc::clone(&handlers);
            tasks.spawn(async move {
                pdu(
                    &handlers,
                    "deliver_sm",
                    deliver("x"),
                    session(&format!("s-{index}")),
                )
                .await
            });
        }
        let mut completed = 0;
        let mut failures = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Some(Ok(_))) => completed += 1,
                other => failures.push(format!("{other:?}")),
            }
        }
        (completed, failures)
    });
    let (completed, failures) = completed;
    assert_eq!(completed, 64, "{:?}", failures.first());
    assert!(
        started.elapsed() < Duration::from_millis(200) * 64 / EXECUTOR_THREADS as u32,
        "waiting coroutines held executor workers: {:?}",
        started.elapsed()
    );
}

#[test]
fn events_on_several_binds_complete_while_gc_stops_the_world() {
    if !free_threaded_or_skip("events_on_several_binds_complete_while_gc_stops_the_world") {
        return;
    }
    let _serial = serial();
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_pdu("deliver_sm")
def on_deliver(pdu, session):
    return pdu.reply(message_id=session.session_id)

@smpp.on_pdu("data_sm")
async def on_data(pdu, session):
    await asyncio.sleep(0)
    return pdu.reply(message_id=session.session_id)
"#,
    );
    // A collector thread forcing full collections, each a stop-the-world
    // on a free-threaded build.
    let stop = Arc::new(AtomicBool::new(false));
    let collector = collect_garbage_until(Arc::clone(&stop));

    const BINDS: usize = 4;
    const EVENTS_PER_BIND: usize = 50;
    let outcome = in_task(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for bind_index in 0..BINDS {
            let handlers = Arc::clone(&handlers);
            tasks.spawn(async move {
                let mut answered = 0;
                for event in 0..EVENTS_PER_BIND {
                    let command = if event % 2 == 0 {
                        "deliver_sm"
                    } else {
                        "data_sm"
                    };
                    let id = format!("bind-{bind_index}");
                    if let Some(Ok(reply)) =
                        pdu(&handlers, command, deliver("x"), session(&id)).await
                    {
                        if reply.message_id.as_deref() == Some(id.as_str()) {
                            answered += 1;
                        }
                    }
                }
                answered
            });
        }
        tokio::time::timeout(Duration::from_secs(60), async move {
            let mut answered = 0;
            while let Some(result) = tasks.join_next().await {
                answered += result.unwrap_or(0);
            }
            answered
        })
        .await
    });
    stop.store(true, Ordering::SeqCst);
    let collections = collector
        .join()
        .expect("collector thread")
        .expect("collections ran");
    assert!(collections > 0, "the collector never ran");
    assert_eq!(
        outcome.expect("events did not finish within the deadline"),
        BINDS * EVENTS_PER_BIND
    );
}

#[test]
fn every_coroutine_completion_is_released() {
    let handlers = handlers_from(
        r#"
import asyncio

@smpp.on_pdu("deliver_sm")
async def on_deliver(pdu, session):
    await asyncio.sleep(0)
    if pdu.body == b"raise":
        raise ValueError("expected")
    return pdu.reply()
"#,
    );
    let results = in_task(async move {
        let mut results = Vec::new();
        for index in 0..200 {
            let body = if index % 4 == 0 { "raise" } else { "ok" };
            results.push(pdu(&handlers, "deliver_sm", deliver(body), session("s-8")).await);
        }
        results
    });
    assert_eq!(results.len(), 200);
    assert!(
        completions_released(),
        "{} completions still live",
        probe::LIVE_COMPLETIONS.load(Ordering::SeqCst)
    );
}

// ── Source gate ─────────────────────────────────────────────────────────

/// The part of a source file before `marker` (where its tests start).
fn production_part<'a>(source: &'a str, marker: &str) -> &'a str {
    source.split(marker).next().unwrap_or(source)
}

#[test]
fn the_runtime_only_attaches_through_the_dispatch_job() {
    let runtime_source = production_part(include_str!("runtime.rs"), "#[cfg(test)]\nmod tests");
    assert_eq!(runtime_source.matches("Python::attach").count(), 0);
    assert!(!runtime_source.contains("call_handler"));
    assert!(!runtime_source.contains("handlers_for"));
    let dispatch_source = production_part(include_str!("dispatch.rs"), "// ── Test probes");
    // The one attach, inside `attach()`, which only runs in executor jobs.
    assert_eq!(dispatch_source.matches("Python::attach(").count(), 1);
    for blocking in [
        "block_on",
        "block_in_place",
        "blocking_recv",
        "thread::sleep",
    ] {
        assert!(!runtime_source.contains(blocking), "runtime.rs: {blocking}");
        assert!(
            !dispatch_source.contains(blocking),
            "dispatch.rs: {blocking}"
        );
    }
}
