use core::time::Duration;
use std::marker::PhantomData;

use futures::channel::oneshot;

use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Storage};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxos};

use super::actor::{run, ActorConfig, ActorState};
use super::event::{AppendError, Command, OmniPaxosEvent, RuntimeProposeErr};
use super::traits::{ActorEntry, AsyncRuntime};

/// Configuration for the async runtime actor.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// How often the actor calls `tick()` on the underlying OmniPaxos.
    /// Should match the tick timeout budget used when building `ServerConfig`.
    pub tick_period: Duration,
    /// How often outgoing messages are drained and forwarded on the `outgoing_messages`
    /// channel.
    pub egress_period: Duration,
    /// Bounded capacity for the actor's command channel.
    pub cmd_channel_capacity: usize,
    /// Bounded capacity for the outgoing message channel.
    pub outgoing_capacity: usize,
    /// Bounded capacity for the incoming message channel.
    pub incoming_capacity: usize,
    /// Bounded capacity for the event channel.
    pub event_capacity: usize,
    /// How long an `append_notify` call waits for its entry to be decided before
    /// resolving with [`AppendError::Timeout`](super::AppendError::Timeout).
    pub append_notify_timeout: Duration,
    /// Bounded capacity for each per-subscriber decided-log stream returned by
    /// [`OmniPaxosHandle::subscribe_decided`].
    pub decided_channel_capacity: usize,
    /// Maximum number of concurrently outstanding `append_notify` calls (i.e.
    /// calls that have not yet resolved). Once reached, further `append_notify`
    /// calls are rejected immediately with [`AppendError::TooManyOutstanding`]
    /// rather than queued, so a stalled cluster (e.g. no leader) can't grow this
    /// backlog without bound.
    pub max_pending_appends: usize,
    /// Maximum number of concurrent [`OmniPaxosHandle::subscribe_decided`]
    /// subscribers. Once reached, a new subscription is rejected: the returned
    /// receiver's channel is closed immediately without delivering any entries.
    pub max_decided_subscribers: usize,
    /// Fallback cap on how many outgoing messages the actor buffers internally
    /// waiting for room on the (bounded) `outgoing_messages` channel, used only
    /// if that channel's own capacity can't be read. In practice the channel's
    /// capacity is always known (`outgoing_capacity` above), so this is a
    /// last-resort bound, not the primary one. Once over the effective cap, the
    /// *oldest* buffered messages are dropped to make room for new ones, rather
    /// than growing without bound -- safe because OmniPaxos already tolerates
    /// lost messages via its own resend/retry mechanisms, the same way it would
    /// tolerate them being dropped by a flaky network. A sufficiently slow or
    /// stalled `outgoing_messages` consumer will lose outgoing messages under
    /// this cap rather than cause unbounded memory growth.
    pub max_outgoing_buffered: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            tick_period: Duration::from_millis(10),
            egress_period: Duration::from_millis(1),
            cmd_channel_capacity: 1024,
            outgoing_capacity: 10_000,
            incoming_capacity: 10_000,
            event_capacity: 1024,
            append_notify_timeout: Duration::from_secs(5),
            decided_channel_capacity: 1024,
            max_pending_appends: 1024,
            max_decided_subscribers: 128,
            max_outgoing_buffered: 10_000,
        }
    }
}

/// A cloneable, `Send + Sync` handle to an OmniPaxos actor running in the background.
///
/// All state-mutating and read methods are `async`. Users must drive message transport
/// externally via [`OmniPaxosHandle::outgoing_messages`] and
/// [`OmniPaxosHandle::handle_incoming`]. Dropping every clone shuts the actor down.
pub struct OmniPaxosHandle<T>
where
    T: Entry,
{
    cmd_tx: async_channel::Sender<Command<T>>,
    events_rx: async_channel::Receiver<OmniPaxosEvent>,
    outgoing_rx: async_channel::Receiver<Message<T>>,
    incoming_tx: async_channel::Sender<Message<T>>,
    decided_channel_capacity: usize,
}

impl<T> Clone for OmniPaxosHandle<T>
where
    T: Entry,
{
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            events_rx: self.events_rx.clone(),
            outgoing_rx: self.outgoing_rx.clone(),
            incoming_tx: self.incoming_tx.clone(),
            decided_channel_capacity: self.decided_channel_capacity,
        }
    }
}

impl<T> OmniPaxosHandle<T>
where
    T: Entry + Send + 'static,
{
    /// Append an entry and await its decided log index.
    ///
    /// Works from any node: the actor tags the entry with a unique id, routes it to
    /// the current leader, and resolves the future when the assigned log index is
    /// decided. If no leader is known yet, the call waits rather than failing fast --
    /// it resolves once a leader is elected, or with `Timeout` if none is elected in
    /// time. Fails with:
    /// - [`AppendError::Timeout`] if the entry is not decided within
    ///   [`RuntimeConfig::append_notify_timeout`] (including when no leader was ever
    ///   elected in that time);
    /// - [`AppendError::Propose`] if the underlying propose call itself failed (e.g. a
    ///   reconfiguration is already pending) -- only when this node owns the pending
    ///   call, otherwise it still resolves via `Timeout`;
    /// - [`AppendError::Superseded`] if a leader change occurred before decision --
    ///   a heuristic, not a certainty; see its doc comment for why, and check the
    ///   returned index yourself if you need to know for sure;
    /// - [`AppendError::TooManyOutstanding`] if [`RuntimeConfig::max_pending_appends`]
    ///   outstanding calls are already queued.
    ///
    /// Uses [`RuntimeConfig::append_notify_timeout`] as the deadline; use
    /// [`Self::append_notify_with_timeout`] to override it for a single call.
    pub async fn append_notify(&self, entry: T) -> Result<usize, AppendError<T>> {
        self.append_notify_inner(entry, None).await
    }

    /// Same as [`append_notify`](Self::append_notify), but `timeout` overrides
    /// [`RuntimeConfig::append_notify_timeout`] for this call only -- e.g. to give a
    /// latency-sensitive caller a shorter deadline, or a bulk/background caller a
    /// longer one, without changing the default for every other call.
    pub async fn append_notify_with_timeout(
        &self,
        entry: T,
        timeout: Duration,
    ) -> Result<usize, AppendError<T>> {
        self.append_notify_inner(entry, Some(timeout)).await
    }

    async fn append_notify_inner(
        &self,
        entry: T,
        timeout: Option<Duration>,
    ) -> Result<usize, AppendError<T>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AppendNotify {
                entry,
                timeout,
                reply: tx,
            })
            .await
            .map_err(|_| AppendError::Shutdown)?;
        rx.await.unwrap_or(Err(AppendError::Shutdown))
    }

    /// Append an entry without waiting for decision. Mirrors the sync API.
    pub async fn append(&self, entry: T) -> Result<(), RuntimeProposeErr<T>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Append { entry, reply: tx })
            .await
            .map_err(|_| RuntimeProposeErr::Shutdown)?;
        match rx.await {
            Ok(res) => res.map_err(RuntimeProposeErr::Propose),
            Err(_) => Err(RuntimeProposeErr::Shutdown),
        }
    }

    /// Ask the actor for the current leader.
    pub async fn current_leader(&self) -> Option<(NodeId, bool)> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::CurrentLeader { reply: tx })
            .await
            .is_err()
        {
            return None;
        }
        rx.await.unwrap_or(None)
    }

    /// Ask the actor for the current decided index.
    pub async fn decided_idx(&self) -> usize {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::DecidedIdx { reply: tx })
            .await
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    /// Ask the actor for a slice of the decided log starting at `from`.
    pub async fn read_decided_suffix(&self, from: usize) -> Option<Vec<LogEntry<T>>> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::ReadDecidedSuffix { from, reply: tx })
            .await
            .is_err()
        {
            return None;
        }
        rx.await.unwrap_or(None)
    }

    /// Propose a cluster reconfiguration.
    pub async fn reconfigure(
        &self,
        new_configuration: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), RuntimeProposeErr<T>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Reconfigure {
                new_configuration,
                metadata,
                reply: tx,
            })
            .await
            .map_err(|_| RuntimeProposeErr::Shutdown)?;
        match rx.await {
            Ok(res) => res.map_err(RuntimeProposeErr::Propose),
            Err(_) => Err(RuntimeProposeErr::Shutdown),
        }
    }

    /// Attempt to become the leader by incrementing the local ballot.
    pub async fn try_become_leader(&self) {
        let _ = self.cmd_tx.send(Command::TryBecomeLeader).await;
    }

    /// Notify the actor that a previously disconnected peer is reachable again.
    pub async fn reconnected(&self, pid: NodeId) {
        let _ = self.cmd_tx.send(Command::Reconnected { pid }).await;
    }

    /// Subscribe to state-transition events. Each event is delivered to exactly one
    /// receiver: if you clone the returned receiver, the clones share the queue.
    /// For fan-out to multiple independent subscribers, wrap in a broadcasting task.
    pub fn subscribe_events(&self) -> async_channel::Receiver<OmniPaxosEvent> {
        self.events_rx.clone()
    }

    /// Subscribe to decided log entries, starting at `from`. Returns a receiver
    /// that emits [`LogEntry`]s in log order — both the backlog already decided
    /// at subscription time and entries decided later.
    ///
    /// Each call creates an independent subscription with its own cursor;
    /// multiple subscribers can coexist, up to [`RuntimeConfig::max_decided_subscribers`]
    /// — beyond that, the returned receiver's channel is closed immediately
    /// without delivering any entries. Dropping the receiver ends the
    /// subscription; slow subscribers apply backpressure by causing the actor
    /// to defer pushes rather than blocking the actor loop.
    pub async fn subscribe_decided(&self, from: usize) -> async_channel::Receiver<LogEntry<T>> {
        let (tx, rx) = async_channel::bounded(self.decided_channel_capacity);
        // If the actor is gone, `rx` stays open but never receives — the
        // caller's stream drops naturally when the sender is dropped here.
        let _ = self
            .cmd_tx
            .send(Command::SubscribeDecided { from, tx })
            .await;
        rx
    }

    /// Returns a receiver from which outgoing messages should be drained and forwarded
    /// by the application's transport.
    pub fn outgoing_messages(&self) -> async_channel::Receiver<Message<T>> {
        self.outgoing_rx.clone()
    }

    /// Deliver an incoming message from the network to the actor.
    pub async fn handle_incoming(&self, m: Message<T>) {
        let _ = self.incoming_tx.send(m).await;
    }
}

/// Spawn the actor and return a handle to it.
pub fn spawn_actor<T, B, R>(op: OmniPaxos<T, B>, cfg: RuntimeConfig) -> OmniPaxosHandle<T>
where
    T: ActorEntry,
    B: Storage<T> + Send + 'static,
    R: AsyncRuntime,
{
    let (cmd_tx, cmd_rx) = async_channel::bounded(cfg.cmd_channel_capacity);
    let (outgoing_tx, outgoing_rx) = async_channel::bounded(cfg.outgoing_capacity);
    let (incoming_tx, incoming_rx) = async_channel::bounded(cfg.incoming_capacity);
    let (event_tx, events_rx) = async_channel::bounded(cfg.event_capacity);

    let state = ActorState::new(
        op,
        cmd_rx,
        incoming_rx,
        outgoing_tx,
        event_tx,
        ActorConfig::from(&cfg),
    );

    R::spawn(run::<T, B, R>(state, PhantomData::<fn() -> R>));

    OmniPaxosHandle {
        cmd_tx,
        events_rx,
        outgoing_rx,
        incoming_tx,
        decided_channel_capacity: cfg.decided_channel_capacity,
    }
}
