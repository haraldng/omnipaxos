use super::{
    ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, StopSign},
    util::SyncItem,
};
use std::fmt::Debug;
use serde::{Deserialize, Serialize};

/// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Promise<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// The current round.
    pub n: Ballot,
    /// The latest round in which an entry was accepted.
    pub n_accepted: Ballot,
    /// The suffix of missing entries at the leader.
    pub sync_item: Option<SyncItem<T, S>>,
    /// The decided index of this follower.
    pub ld: u64,
    /// The log length of this follower.
    pub la: u64,
    /// The StopSign accepted by this follower
    pub stopsign: Option<StopSign>,
}

impl<T, S> Promise<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Creates a [`Promise`] message.
    pub fn with(
        n: Ballot,
        n_accepted: Ballot,
        sync_item: Option<SyncItem<T, S>>,
        ld: u64,
        la: u64,
        stopsign: Option<StopSign>,
    ) -> Self {
        Self {
            n,
            n_accepted,
            sync_item,
            ld,
            la,
            stopsign,
        }
    }
}

/// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptSync<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// The current round.
    pub n: Ballot,
    /// Entries that the receiving replica is missing in its log.
    pub sync_item: SyncItem<T, S>,
    /// The index of the log where the entries from `sync_item` should be applied at or the compacted idx
    pub sync_idx: u64,
    /// The decided index
    pub decide_idx: Option<u64>,
    /// StopSign to be accepted
    pub stopsign: Option<StopSign>,
}

impl<T, S> AcceptSync<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Creates an [`AcceptSync`] message.
    pub fn with(
        n: Ballot,
        sync_item: SyncItem<T, S>,
        sync_idx: u64,
        decide_idx: Option<u64>,
        stopsign: Option<StopSign>,
    ) -> Self {
        AcceptSync {
            n,
            sync_item,
            sync_idx,
            decide_idx,
            stopsign,
        }
    }
}

/// The first accept message sent. Only used by a pre-elected leader after reconfiguration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FirstAccept<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// Entries to be replicated.
    pub entries: Vec<T>,
}

impl<T> FirstAccept<T>
where
    T: Entry,
{
    /// Creates a [`FirstAccept`] message.
    pub fn with(n: Ballot, entries: Vec<T>) -> Self {
        FirstAccept { n, entries }
    }
}

/// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptDecide<T>
where
    T: Entry,
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
    T: Entry,
{
    /// Creates an [`AcceptDecide`] message.
    pub fn with(n: Ballot, ld: u64, entries: Vec<T>) -> Self {
        AcceptDecide { n, ld, entries }
    }
}

/// Message sent by follower to leader when entries has been accepted.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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

/// Message sent by leader to followers to accept a StopSign
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptStopSign {
    /// The current round.
    pub n: Ballot,
    /// The decided index.
    pub ss: StopSign,
}

impl AcceptStopSign {
    /// Creates a [`AcceptStopSign`] message.
    pub fn with(n: Ballot, ss: StopSign) -> Self {
        AcceptStopSign { n, ss }
    }
}

/// Message sent by followers to leader when accepted StopSign
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct AcceptedStopSign {
    /// The current round.
    pub n: Ballot,
}

impl AcceptedStopSign {
    /// Creates a [`AcceptedStopSign`] message.
    pub fn with(n: Ballot) -> Self {
        AcceptedStopSign { n }
    }
}

/// Message sent by leader to decide a StopSign
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct DecideStopSign {
    /// The current round.
    pub n: Ballot,
}

impl DecideStopSign {
    /// Creates a [`DecideStopSign`] message.
    pub fn with(n: Ballot) -> Self {
        DecideStopSign { n }
    }
}

/// Compaction Request
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Compaction {
    Trim(Option<u64>),
    Snapshot(u64),
}

/// An enum for all the different message types.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PaxosMsg<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
    PrepareReq,
    #[allow(missing_docs)]
    Prepare(Prepare),
    Promise(Promise<T, S>),
    AcceptSync(AcceptSync<T, S>),
    FirstAccept(FirstAccept<T>),
    AcceptDecide(AcceptDecide<T>),
    Accepted(Accepted),
    Decide(Decide),
    /// Forward client proposals to the leader.
    ProposalForward(Vec<T>),
    Compaction(Compaction),
    ForwardCompaction(Compaction),
    AcceptStopSign(AcceptStopSign),
    AcceptedStopSign(AcceptedStopSign),
    DecideStopSign(DecideStopSign),
}

/// A struct for a Paxos message that also includes sender and receiver.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Sender of `msg`.
    pub from: u64,
    /// Receiver of `msg`.
    pub to: u64,
    /// The message content.
    pub msg: PaxosMsg<T, S>,
}

impl<T, S> Message<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Creates a message.
    pub fn with(from: u64, to: u64, msg: PaxosMsg<T, S>) -> Self {
        Message { from, to, msg }
    }
}
