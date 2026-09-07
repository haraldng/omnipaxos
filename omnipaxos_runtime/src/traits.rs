use core::future::Future;
use core::time::Duration;

use omnipaxos::storage::Entry;

/// A blanket trait that fixes all the `Send` bounds an [`Entry`] must satisfy to cross
/// the actor boundary. Users normally do not implement this manually — a blanket impl
/// covers any `Entry` whose associated types are `Send`.
#[cfg(not(feature = "unicache"))]
pub trait ActorEntry: Entry<Snapshot: Send> + Send + 'static {}

#[cfg(not(feature = "unicache"))]
impl<T> ActorEntry for T where T: Entry<Snapshot: Send> + Send + 'static {}

/// A blanket trait that fixes all the `Send` bounds an [`Entry`] must satisfy to cross
/// the actor boundary. Users normally do not implement this manually — a blanket impl
/// covers any `Entry` whose associated types are `Send`.
#[cfg(feature = "unicache")]
pub trait ActorEntry:
    Entry<Snapshot: Send, UniCache: Send, EncodeResult: Send> + Send + 'static
{
}

#[cfg(feature = "unicache")]
impl<T> ActorEntry for T where
    T: Entry<Snapshot: Send, UniCache: Send, EncodeResult: Send> + Send + 'static
{
}

/// Abstraction over an async executor.
///
/// Implement this trait to run OmniPaxos on runtimes other than tokio. Two primitives
/// are required: task spawning and a timer. The actor loop builds intervals on top of
/// [`AsyncRuntime::sleep`], so no separate `interval` primitive is needed.
pub trait AsyncRuntime: 'static {
    /// The join handle returned by [`AsyncRuntime::spawn`]. The runtime is free to
    /// return `()` if it does not need cancellation or joining.
    type JoinHandle: Send + 'static;

    /// Spawn a future onto the executor.
    fn spawn<F>(future: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;

    /// Return a future that resolves after `duration` elapses.
    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send + 'static;
}
