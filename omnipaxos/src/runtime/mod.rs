//! Async runtime wrapper for OmniPaxos.
//!
//! The [`AsyncRuntime`] trait abstracts over the async executor so this module can be used
//! on top of any runtime. A ready-to-use [`TokioRuntime`] is provided behind the
//! `tokio_runtime` feature.
//!
//! The main entry point is [`spawn_actor`], which takes an [`OmniPaxos`](crate::OmniPaxos)
//! instance and returns an [`OmniPaxosHandle`]. The handle is cloneable and provides
//! `async` methods to append entries and observe state transitions.
//!
//! The runtime does NOT own message transport: use [`OmniPaxosHandle::outgoing_messages`]
//! to drain outgoing messages and [`OmniPaxosHandle::handle_incoming`] to deliver
//! incoming ones. This keeps the crate free of I/O dependencies and lets users plug in
//! any transport (TCP, QUIC, in-process channels, etc.).

mod actor;
mod event;
mod handle;
mod traits;

#[cfg(feature = "tokio_runtime")]
mod tokio_rt;

pub use event::{AppendError, OmniPaxosEvent};
pub use handle::{spawn_actor, OmniPaxosHandle, RuntimeConfig};
pub use traits::{ActorEntry, AsyncRuntime};

#[cfg(feature = "tokio_runtime")]
pub use tokio_rt::TokioRuntime;
