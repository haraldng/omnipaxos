use crate::{
    leader_election::ballot_leader_election::Ballot,
    storage::{Snapshot, SnapshotType, StopSign},
};
use std::marker::PhantomData;

/// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
#[derive(Copy, Clone, Debug)]
pub struct Prepare {
    /// The current round.
    pub n: Ballot,
    /// The decided index of this leader.
    pub ld: u64,
    /// The latest round in which an entry was accepted.
    pub n_accepted: Ballot,
    /// The log length of this leader.
    pub la: u64,
}

impl Prepare {
    /// Creates a [`Prepare`] message.
    pub fn with(n: Ballot, ld: u64, n_accepted: Ballot, la: u64) -> Self {
        Prepare {
            n,
            ld,
            n_accepted,
            la,
        }
    }
}

/// Promise message sent by a follower in response to a [`Prepare`] sent by the leader.
#[derive(Clone, Debug)]
pub struct Promise<T>
where
    T: Clone,
{
    /// The current round.
    pub n: Ballot,
    /// The latest round in which an entry was accepted.
    pub n_accepted: Ballot,
    /// The suffix of missing entries at the leader.
    pub sfx: Vec<T>,
    /// The decided index of this follower.
    pub ld: u64,
    /// The log length of this follower.
    pub la: u64,
    pub stop_sign: Option<StopSign>,
}

impl<T> Promise<T>
where
    T: Clone,
{
    /// Creates a [`Promise`] message.
    pub fn with(
        n: Ballot,
        n_accepted: Ballot,
        sfx: Vec<T>,
        ld: u64,
        la: u64,
        stop_sign: Option<StopSign>,
    ) -> Self {
        Self {
            n,
            n_accepted,
            sfx,
            ld,
            la,
            stop_sign,
        }
    }
}

/// Promise message using Snapshot sent by a follower in response to a [`Prepare`] sent by the leader.
#[derive(Clone, Debug)]
pub struct SnapshotPromise<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// The current round.
    pub n: Ballot,
    /// The latest round in which an entry was accepted.
    pub n_accepted: Ballot,
    /// The decided index of this follower.
    pub ld: u64,
    /// The log length of this follower.
    pub trimmed_idx: u64,
    pub snapshot: Option<SnapshotType<T, S>>,
    pub stop_sign: Option<StopSign>,
    phantom: PhantomData<T>,
}

impl<T, S> SnapshotPromise<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// Creates a [`Promise`] message.
    pub fn with(
        n: Ballot,
        n_accepted: Ballot,
        snapshot: Option<SnapshotType<T, S>>,
        ld: u64,
        trimmed_idx: u64,
        stop_sign: Option<StopSign>,
    ) -> Self {
        Self {
            n,
            n_accepted,
            snapshot,
            ld,
            trimmed_idx,
            stop_sign,
            phantom: PhantomData,
        }
    }
}

/// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
#[derive(Clone, Debug)]
pub struct AcceptSync<T>
where
    T: Clone,
{
    /// The current round.
    pub n: Ballot,
    /// Entries that the receiving replica is missing in its log.
    pub entries: Vec<T>,
    /// The index of the log where `entries` should be applied at.
    pub sync_idx: u64,
    pub decide_idx: Option<u64>,
}

impl<T> AcceptSync<T>
where
    T: Clone,
{
    /// Creates an [`AcceptSync`] message.
    pub fn with(n: Ballot, sfx: Vec<T>, sync_idx: u64, decide_idx: Option<u64>) -> Self {
        AcceptSync {
            n,
            entries: sfx,
            sync_idx,
            decide_idx,
        }
    }
}

/// AcceptSync message with snapshot sent by the leader to synchronize the logs of all replicas in the prepare phase.
#[derive(Clone, Debug)]
pub struct SnapshotAcceptSync<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// The current round.
    pub n: Ballot,
    /// Snapshot that the receiving replica should sync with.
    pub snapshot: SnapshotType<T, S>,
    /// The index of the log where `entries` should be applied at.
    pub trimmed_idx: u64,
    phantom: PhantomData<T>,
}

impl<T, S> SnapshotAcceptSync<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// Creates an [`AcceptSync`] message.
    pub fn with(n: Ballot, snapshot: SnapshotType<T, S>, trimmed_idx: u64) -> Self {
        Self {
            n,
            snapshot,
            trimmed_idx,
            phantom: PhantomData,
        }
    }
}

/// The first accept message sent. Only used by a pre-elected leader after reconfiguration.
#[derive(Clone, Debug)]
pub struct FirstAccept<T>
where
    T: Clone,
{
    /// The current round.
    pub n: Ballot,
    /// Entries to be replicated.
    pub entries: Vec<T>,
}

impl<T> FirstAccept<T>
where
    T: Clone,
{
    /// Creates a [`FirstAccept`] message.
    pub fn with(n: Ballot, entries: Vec<T>) -> Self {
        FirstAccept { n, entries }
    }
}

/// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
#[derive(Clone, Debug)]
pub struct AcceptDecide<T>
where
    T: Clone,
{
    /// The current round.
    pub n: Ballot,
    /// The decided index.
    pub ld: u64,
    /// Entries to be replicated.
    pub entries: Vec<T>,
}

impl<T> AcceptDecide<T>
where
    T: Clone,
{
    /// Creates an [`AcceptDecide`] message.
    pub fn with(n: Ballot, ld: u64, entries: Vec<T>) -> Self {
        AcceptDecide { n, ld, entries }
    }
}

/// Message sent by follower to leader when entries has been accepted.
#[derive(Copy, Clone, Debug)]
pub struct Accepted {
    /// The current round.
    pub n: Ballot,
    /// The accepted index.
    pub la: u64,
}

impl Accepted {
    /// Creates an [`Accepted`] message.
    pub fn with(n: Ballot, la: u64) -> Self {
        Accepted { n, la }
    }
}

/// Message sent by leader to followers to decide up to a certain index in the log.
#[derive(Copy, Clone, Debug)]
pub struct Decide {
    /// The current round.
    pub n: Ballot,
    /// The decided index.
    pub ld: u64,
}

impl Decide {
    /// Creates a [`Decide`] message.
    pub fn with(n: Ballot, ld: u64) -> Self {
        Decide { n, ld }
    }
}

/// An enum for all the different message types.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub enum PaxosMsg<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
    PrepareReq,
    #[allow(missing_docs)]
    Prepare(Prepare),
    Promise(Promise<T>),
    SnapshotPromise(SnapshotPromise<T, S>),
    AcceptSync(AcceptSync<T>),
    SnapshotAcceptSync(SnapshotAcceptSync<T, S>),
    FirstAccept(FirstAccept<T>),
    AcceptDecide(AcceptDecide<T>),
    Accepted(Accepted),
    Decide(Decide),
    /// Forward client proposals to the leader.
    ProposalForward(Vec<T>),
    GarbageCollect(u64),
    ForwardGarbageCollect(Option<u64>),

}

/// A struct for a Paxos message that also includes sender and receiver.
#[derive(Clone, Debug)]
pub struct Message<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// Sender of `msg`.
    pub from: u64,
    /// Balloteceiver of `msg`.
    pub to: u64,
    /// The message content.
    pub msg: PaxosMsg<T, S>,
}

impl<T, S> Message<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// Creates a message.
    pub fn with(from: u64, to: u64, msg: PaxosMsg<T, S>) -> Self {
        Message { from, to, msg }
    }
}
