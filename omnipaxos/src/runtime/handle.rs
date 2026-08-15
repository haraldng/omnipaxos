use core::time::Duration;
use std::marker::PhantomData;

use futures::channel::oneshot;

use crate::messages::Message;
use crate::storage::{Entry, Storage};
use crate::util::{LogEntry, NodeId};
use crate::{ClusterConfig, OmniPaxos, ProposeErr};

use super::actor::{run, ActorState};
use super::event::{AppendError, Command, OmniPaxosEvent};
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
        }
    }
}

impl<T> OmniPaxosHandle<T>
where
    T: Entry + Send + 'static,
{
    /// Append an entry and await its decided log index. If a leader change causes
    /// the entry to be overwritten before decision, returns [`AppendError::Superseded`]
    /// and the user should re-append.
    pub async fn append_notify(&self, entry: T) -> Result<usize, AppendError<T>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AppendNotify { entry, reply: tx })
            .await
            .map_err(|_| AppendError::Shutdown)?;
        rx.await.unwrap_or(Err(AppendError::Shutdown))
    }

    /// Append an entry without waiting for decision. Mirrors the sync API.
    pub async fn append(&self, entry: T) -> Result<(), ProposeErr<T>> {
        let (tx, rx) = oneshot::channel();
        // Sending failure means the actor has shut down; treat as if pending reconfig.
        // We can't fabricate a ProposeErr without T though, so we panic — the correct
        // sentinel would be an AppendError, but `append` mirrors the sync API. Instead:
        // if the send fails, return the entry via PendingReconfigEntry as a best-effort.
        if let Err(err) = self.cmd_tx.send(Command::Append { entry, reply: tx }).await {
            // Recover the entry from the returned Command so we can hand it back.
            if let Command::Append { entry, .. } = err.into_inner() {
                return Err(ProposeErr::PendingReconfigEntry(entry));
            }
            unreachable!("Command::Append round-trips its variant");
        }
        rx.await.unwrap_or_else(|_| {
            // Actor dropped without responding; nothing we can return that carries T.
            // Users can subscribe to events to observe shutdown.
            Ok(())
        })
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
    ) -> Result<(), ProposeErr<T>> {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self
            .cmd_tx
            .send(Command::Reconfigure {
                new_configuration,
                metadata,
                reply: tx,
            })
            .await
        {
            if let Command::Reconfigure {
                new_configuration,
                metadata,
                ..
            } = err.into_inner()
            {
                return Err(ProposeErr::PendingReconfigConfig(
                    new_configuration,
                    metadata,
                ));
            }
            unreachable!("Command::Reconfigure round-trips its variant");
        }
        rx.await.unwrap_or(Ok(()))
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
        cfg.tick_period,
        cfg.egress_period,
    );

    R::spawn(run::<T, B, R>(state, PhantomData::<fn() -> R>));

    OmniPaxosHandle {
        cmd_tx,
        events_rx,
        outgoing_rx,
        incoming_tx,
    }
}
