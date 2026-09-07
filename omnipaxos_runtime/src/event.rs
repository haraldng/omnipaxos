use futures::channel::oneshot;

use omnipaxos::storage::Entry;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, ProposeErr};

/// A state transition observed by the actor and broadcast on the event stream.
#[derive(Debug, Clone)]
pub enum OmniPaxosEvent {
    /// A new leader was observed on this node.
    LeaderElected {
        /// The pid of the elected leader.
        pid: NodeId,
        /// Whether this node's phase is `Phase::Accept`.
        accepted: bool,
    },
    /// The decided index advanced. The value is the new decided index.
    Decided {
        /// The new decided index.
        new_decided_idx: usize,
    },
    /// A reconfiguration StopSign was decided.
    Reconfigured {
        /// The log index at which the StopSign was decided.
        stop_sign_idx: usize,
    },
}

/// Error returned by [`OmniPaxosHandle::append_notify`](super::OmniPaxosHandle::append_notify).
#[derive(Debug)]
pub enum AppendError<T>
where
    T: Entry,
{
    /// The underlying [`OmniPaxos::append`](crate::OmniPaxos::append) call failed.
    Propose(ProposeErr<T>),
    /// No leader is currently known so the entry cannot be routed. Retry after election.
    NotLeader {
        /// The current leader as observed by this node, if known.
        current_leader: Option<NodeId>,
    },
    /// A leader change occurred after the entry was accepted at its assigned index but
    /// before it was decided; the entry at that index may have been overwritten under a
    /// higher ballot. Users should re-append if this error is returned.
    Superseded,
    /// The `append_notify_timeout` elapsed before the entry was decided. Retry safe.
    Timeout,
    /// The actor has shut down before the entry could be decided.
    Shutdown,
    /// The actor already has [`RuntimeConfig::max_pending_appends`](super::RuntimeConfig::max_pending_appends)
    /// outstanding `append_notify` calls; this one was rejected immediately rather
    /// than queued. Nothing was proposed, so it's safe to retry (ideally after a
    /// backoff, since the backlog needs time to drain).
    TooManyOutstanding,
}

/// Commands sent from an [`OmniPaxosHandle`](super::OmniPaxosHandle) to the actor task.
///
/// This type is `pub(crate)` because it is an implementation detail of the actor <-> handle
/// protocol.
pub(crate) enum Command<T>
where
    T: Entry,
{
    Append {
        entry: T,
        reply: oneshot::Sender<Result<(), ProposeErr<T>>>,
    },
    AppendNotify {
        entry: T,
        reply: oneshot::Sender<Result<usize, AppendError<T>>>,
    },
    CurrentLeader {
        reply: oneshot::Sender<Option<(NodeId, bool)>>,
    },
    DecidedIdx {
        reply: oneshot::Sender<usize>,
    },
    ReadDecidedSuffix {
        from: usize,
        reply: oneshot::Sender<Option<Vec<LogEntry<T>>>>,
    },
    SubscribeDecided {
        from: usize,
        tx: async_channel::Sender<LogEntry<T>>,
    },
    Reconfigure {
        new_configuration: ClusterConfig,
        metadata: Option<Vec<u8>>,
        reply: oneshot::Sender<Result<(), ProposeErr<T>>>,
    },
    TryBecomeLeader,
    Reconnected {
        pid: NodeId,
    },
}
