use core::time::Duration;

use futures::channel::oneshot;

use omnipaxos::{
    storage::Entry,
    util::{LogEntry, NodeId},
    ClusterConfig, ProposeErr,
};

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
    /// The underlying [`OmniPaxos::append`](omnipaxos::OmniPaxos::append) call failed while
    /// this node was accepting the entry as leader (e.g. a reconfiguration is already
    /// pending). Unlike the other variants, this is resolved immediately rather than
    /// waiting for `append_notify_timeout` -- but only when this node itself owns the
    /// pending call (i.e. it originated the `append_notify` or was routed to it as
    /// leader). If the failure happens on a different node than the origin, the origin
    /// still just sees a `Timeout`, since surfacing the specific error cross-node would
    /// require a new wire message.
    Propose(ProposeErr<T>),
    /// A leader change occurred after the entry was accepted at the given index but
    /// before it was decided. This is a *heuristic*, not a certainty: a new leader is
    /// required to adopt any value a majority-overlapping quorum may have already
    /// accepted, so the entry can still end up decided at `log_entry_idx` under the
    /// new ballot -- we just stop waiting rather than confirm it. Check
    /// [`OmniPaxosHandle::read_decided_suffix`](super::OmniPaxosHandle::read_decided_suffix)
    /// at that index if you need to know for sure before re-appending.
    Superseded {
        /// The log index the entry was assigned before the leader change.
        log_entry_idx: usize,
    },
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

/// Error returned by [`OmniPaxosHandle::append`](super::OmniPaxosHandle::append) and
/// [`OmniPaxosHandle::reconfigure`](super::OmniPaxosHandle::reconfigure). Distinct from
/// [`AppendError`]: these calls don't track decision, so they only ever fail
/// synchronously -- either the propose call itself was rejected, or the actor is gone.
#[derive(Debug)]
pub enum RuntimeProposeErr<T>
where
    T: Entry,
{
    /// The underlying propose call failed (e.g. a reconfiguration is already pending,
    /// or an invalid cluster config was proposed).
    Propose(ProposeErr<T>),
    /// The actor has shut down.
    Shutdown,
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
        /// Overrides [`RuntimeConfig::append_notify_timeout`](super::RuntimeConfig::append_notify_timeout)
        /// for this call only, if `Some`.
        timeout: Option<Duration>,
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
