//! `siphon-smpp` — SMPP 3.4 addon for the siphon scripting platform.
//!
//! This crate is a **siphon addon**, not a standalone server. It plugs an
//! `smpp` Python namespace into a siphon binary so user scripts can write:
//!
//! ```python
//! from siphon import smpp, log, cache
//!
//! @smpp.on_pdu("submit_sm")
//! async def handle_submit(pdu, session):
//!     log.info(f"submit_sm from {session.system_id}: {pdu.short_message!r}")
//!     dest = pdu.destination_addr
//!     if not await cache.exists(f"reg:{dest}"):
//!         return pdu.reply(command_status="ESME_RINVDSTADR")
//!     # ...route the SMS...
//!     return pdu.reply(message_id="abc123")
//!
//! @smpp.on_bind
//! async def authorize(bind, session):
//!     password_ok = await check_credentials(bind.system_id, bind.password)
//!     return bind.accept() if password_ok else bind.reject("ESME_RBINDFAIL")
//! ```
//!
//! ## Install contract
//!
//! Composing binaries register the namespace + task during startup.
//! Together they:
//!
//! 1. build a Python module with the `smpp` decorators + helper types and
//!    register it under the name `"smpp"`;
//! 2. spawn a tokio-side SMPP server (per [`SmppConfig`]) wired to the
//!    script registry — when a PDU arrives, the matching `@smpp.on_pdu`
//!    handler is invoked on siphon's script asyncio loop.
//!
//! ## Config
//!
//! `SmppConfig` is loaded from a separate YAML/TOML file referenced by the
//! main siphon config (e.g. `addons.smpp = "/etc/siphon/smpp.yaml"`).
//! Keeping addon config out of siphon's main config schema means siphon
//! doesn't need to know about every addon's options at compile time.
//! See [`config`] for the schema.
//!
//! ## What stays in Rust vs. moves to Python
//!
//! Rust handles: TCP framing, bind/enquire_link/inactivity timers,
//! sequence-number windowing, PDU codec, retransmission of unacked
//! deliver_sm. Python handles: routing decisions, ESME credential
//! verification, message persistence/queueing, throttling policy.

pub mod config;
pub mod install;
pub mod pyclasses;
pub mod runtime;
pub mod submit;

pub use config::SmppConfig;
pub use install::{namespace, task};

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("python error: {0}")]
    Python(String),
    #[error("siphon namespace registration: {0}")]
    Siphon(String),
    #[error("config: {0}")]
    Config(#[from] config::ConfigError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
