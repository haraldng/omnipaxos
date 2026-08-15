# OmniPaxos Async Runtime Architecture Plan

This document outlines the proposed architecture for introducing an Async API to OmniPaxos. The primary goal is to resolve the manual polling, boilerplate event loops, and heavy synchronization currently required of users (e.g., in the `kv_store` example).

## 1. Current Pain Points

- **Manual Event Loops:** Users must build their own `select!` loops to interleave `tick()`, fetching outgoing messages, and processing incoming messages.
- **Polling for Decisions:** After appending an entry, users must currently sleep and poll `read_decided_suffix()` to see if the entry was committed.
- **Polling for Leader Election:** Users must sleep and repeatedly poll `get_current_leader()`.
- **Heavy Synchronization:** `OmniPaxos` is often wrapped in `Arc<Mutex<OmniPaxosKV>>` to be shared safely between the background loop and main application code.

## 2. Proposed API Enhancements

### Future-Based `append_notify`
Instead of a fire-and-forget `append`, we introduce `append_notify` returning a `Future` (via `futures-channel::oneshot`). When the entry is decided, the sender is triggered.
```rust
// Awaits efficiently without sleeping or polling
let committed_index = paxos.append_notify(kv1).await.expect("append failed");
```

### Event Streams for State Changes
Expose an asynchronous `Stream` of events (e.g., via `async-channel`) for state transitions.
```rust
let mut events = paxos.subscribe_events();
while let Some(event) = events.next().await {
    if let OmniPaxosEvent::LeaderElected(node_id) = event {
        println!("New leader: {}", node_id);
    }
}
```

## 3. Runtime Agnosticism Strategy

To ensure OmniPaxos is not tightly coupled to `tokio`, but remains completely frictionless for `tokio` users, we will use a **Trait with an Out-of-the-Box Implementation**.

### The `AsyncRuntime` Trait
Defined in the core library, this trait abstracts away runtime-specific functions.
```rust
pub trait AsyncRuntime {
    fn spawn<F>(future: F) where F: Future<Output = ()> + Send + 'static;
    fn sleep(duration: std::time::Duration) -> impl Future<Output = ()> + Send;
}
```

### The Out-of-the-Box `TokioRuntime`
Behind a feature flag (`[features] tokio_runtime = ["tokio"]`), we provide the concrete implementation.
```rust
#[cfg(feature = "tokio_runtime")]
pub struct TokioRuntime;

#[cfg(feature = "tokio_runtime")]
impl AsyncRuntime for TokioRuntime {
    fn spawn<F>(future: F) where F: Future<Output = ()> + Send + 'static {
        tokio::spawn(future);
    }
    async fn sleep(duration: std::time::Duration) {
        tokio::time::sleep(duration).await;
    }
}
```
**Benefits:**
- **95% of Users:** Enable the `tokio_runtime` feature and pass in `TokioRuntime`. Zero boilerplate.
- **Other Runtimes:** Do not enable the feature flag; implement `AsyncRuntime` manually for `async-std` or embedded executors.

## 4. Open Architectural Decision: State Encapsulation

We have not yet decided how the consensus state should be shared. There are two primary options under consideration:

### Option A: The Actor Model
OmniPaxos runs inside a single spawned background task. The user interacts purely through a clonable `OmniPaxosHandle` that uses channels to send commands.
- **Pros:** Completely eliminates `Arc<Mutex<T>>`. No user boilerplate for event loops. Clean, encapsulated state management.
- **Cons:** Channel overhead. Users must ask the actor for state (cannot synchronously read local state instantly).

### Option B: Mutable References (Current Pattern + `oneshot`)
Maintain the current `Arc<Mutex>` pattern, but enhance it so `append` returns a `oneshot::Receiver`.
- **Pros:** Highly familiar to existing users. Users retain maximum control over the event loop and can synchronously lock and read state instantly.
- **Cons:** Users still have to write complex `select!` event loops. High risk of lock contention blocking the main thread.
