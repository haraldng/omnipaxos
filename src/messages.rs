use crate::{leader_election::Round, storage::Entry};

/// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
#[derive(Clone, Debug)]
pub struct Prepare<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// The decided index of this leader.
    pub ld: u64,
    /// The latest round in which an entry was accepted.
    pub n_accepted: R,
    /// The log length of this leader.
    pub la: u64,
}

impl<R> Prepare<R>
where
    R: Round,
{
    /// Creates a [`Prepare`] message.
    pub fn with(n: R, ld: u64, n_accepted: R, la: u64) -> Self {
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
pub struct Promise<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// The latest round in which an entry was accepted.
    pub n_accepted: R,
    /// The suffix of missing entries at the leader.
    pub sfx: Vec<Entry<R>>,
    /// The decided index of this follower.
    pub ld: u64,
    /// The log length of this follower.
    pub la: u64,
}

impl<R> Promise<R>
where
    R: Round,
{
    /// Creates a [`Promise`] message.
    pub fn with(n: R, n_accepted: R, sfx: Vec<Entry<R>>, ld: u64, la: u64) -> Self {
        Promise {
            n,
            n_accepted,
            sfx,
            ld,
            la,
        }
    }
}

/// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
#[derive(Clone, Debug)]
pub struct AcceptSync<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// Entries that the receiving replica is missing in its log.
    pub entries: Vec<Entry<R>>,
    /// The index of the log where `entries` should be applied at.
    pub sync_idx: u64,
}

impl<R> AcceptSync<R>
where
    R: Round,
{
    /// Creates an [`AcceptSync`] message.
    pub fn with(n: R, sfx: Vec<Entry<R>>, sync_idx: u64) -> Self {
        AcceptSync {
            n,
            entries: sfx,
            sync_idx,
        }
    }
}

/// The first accept message sent. Only used by a pre-elected leader after reconfiguration.
#[derive(Clone, Debug)]
pub struct FirstAccept<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// Entries to be replicated.
    pub entries: Vec<Entry<R>>,
}

impl<R> FirstAccept<R>
where
    R: Round,
{
    /// Creates a [`FirstAccept`] message.
    pub fn with(n: R, entries: Vec<Entry<R>>) -> Self {
        FirstAccept { n, entries }
    }
}

/// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
#[derive(Clone, Debug)]
pub struct AcceptDecide<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// The decided index.
    pub ld: u64,
    /// Entries to be replicated.
    pub entries: Vec<Entry<R>>,
}

impl<R> AcceptDecide<R>
where
    R: Round,
{
    /// Creates an [`AcceptDecide`] message.
    pub fn with(n: R, ld: u64, entries: Vec<Entry<R>>) -> Self {
        AcceptDecide { n, ld, entries }
    }
}

/// Message sent by follower to leader when entries has been accepted.
#[derive(Clone, Debug)]
pub struct Accepted<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// The accepted index.
    pub la: u64,
}

impl<R> Accepted<R>
where
    R: Round,
{
    /// Creates an [`Accepted`] message.
    pub fn with(n: R, la: u64) -> Self {
        Accepted { n, la }
    }
}

/// Message sent by leader to followers to decide up to a certain index in the log.
#[derive(Clone, Debug)]
pub struct Decide<R>
where
    R: Round,
{
    /// The current round.
    pub n: R,
    /// The decided index.
    pub ld: u64,
}

impl<R> Decide<R>
where
    R: Round,
{
    /// Creates a [`Decide`] message.
    pub fn with(n: R, ld: u64) -> Self {
        Decide { n, ld }
    }
}

/// An enum for all the different message types.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub enum PaxosMsg<R>
where
    R: Round,
{
    /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
    PrepareReq,
    #[allow(missing_docs)]
    Prepare(Prepare<R>),
    Promise(Promise<R>),
    AcceptSync(AcceptSync<R>),
    FirstAccept(FirstAccept<R>),
    AcceptDecide(AcceptDecide<R>),
    Accepted(Accepted<R>),
    Decide(Decide<R>),
    /// Forward client proposals to the leader.
    ProposalForward(Vec<Entry<R>>),
    GarbageCollect(u64),
    ForwardGarbageCollect(Option<u64>),
}

/// A struct for a Paxos message that also includes sender and receiver.
#[derive(Clone, Debug)]
pub struct Message<R>
where
    R: Round,
{
    /// Sender of `msg`.
    pub from: u64,
    /// Receiver of `msg`.
    pub to: u64,
    /// The message content.
    pub msg: PaxosMsg<R>,
}

impl<R> Message<R>
where
    R: Round,
{
    /// Creates a message.
    pub fn with(from: u64, to: u64, msg: PaxosMsg<R>) -> Self {
        Message { from, to, msg }
    }
}
