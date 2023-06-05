use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::Promise,
    storage::{Entry, SnapshotType, StopSign},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData};

#[derive(Debug, Clone)]
pub(crate) struct AcceptedMetaData<T: Entry> {
    pub accepted_idx: u64,
    pub flushed_entries: Vec<T>,
}

#[derive(Debug, Clone, Default)]
/// Promise without the suffix
pub(crate) struct PromiseMetaData {
    pub n: Ballot,
    pub accepted_idx: u64,
    pub pid: NodeId,
    pub stopsign: Option<StopSign>,
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n == other.n
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
        {
            Ordering::Equal
        } else if self.n > other.n || (self.n == other.n && self.accepted_idx > other.accepted_idx)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n && self.accepted_idx == other.accepted_idx && self.pid == other.pid
    }
}

#[derive(Debug, Clone, Default)]
/// Actual data of a promise i.e., the decided snapshot and/or the suffix.
pub(crate) struct PromiseData<T: Entry> {
    pub decided_snapshot: Option<SnapshotType<T>>,
    pub suffix: Vec<T>,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub n_leader: Ballot,
    pub promises_meta: Vec<Option<PromiseMetaData>>,
    // the sequence number of accepts for each follower where AcceptSync has sequence number = 1
    pub follower_seq_nums: Vec<SequenceNumber>,
    pub accepted_indexes: Vec<u64>,
    pub decided_indexes: Vec<Option<u64>>,
    pub max_promise_meta: PromiseMetaData,
    pub max_promise: Option<PromiseData<T>>,
    #[cfg(feature = "batch_accept")]
    pub batch_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub accepted_stopsign: Vec<bool>,
    pub max_pid: usize,
    // The number of promises needed in the prepare phase to become synced and
    // the number of accepteds needed in the accept phase to decide an entry.
    pub quorum: Quorum,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub fn with(
        n_leader: Ballot,
        decided_indexes: Option<Vec<Option<u64>>>,
        max_pid: usize,
        quorum: Quorum,
    ) -> Self {
        Self {
            n_leader,
            promises_meta: vec![None; max_pid],
            follower_seq_nums: vec![SequenceNumber::default(); max_pid],
            accepted_indexes: vec![0; max_pid],
            decided_indexes: decided_indexes.unwrap_or_else(|| vec![None; max_pid]),
            max_promise_meta: PromiseMetaData::default(),
            max_promise: None,
            #[cfg(feature = "batch_accept")]
            batch_accept_meta: vec![None; max_pid],
            accepted_stopsign: vec![false; max_pid],
            max_pid,
            quorum,
        }
    }

    fn pid_to_idx(pid: NodeId) -> usize {
        (pid - 1) as usize
    }

    // Resets `pid`'s accept sequence to indicate they are in the next session of accepts
    pub fn increment_seq_num_session(&mut self, pid: NodeId) {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].session += 1;
        self.follower_seq_nums[idx].counter = 0;
    }

    pub fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].counter += 1;
        self.follower_seq_nums[idx]
    }

    pub fn get_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        self.follower_seq_nums[Self::pid_to_idx(pid)]
    }

    pub fn set_decided_idx(&mut self, pid: NodeId, idx: Option<u64>) {
        self.decided_indexes[Self::pid_to_idx(pid)] = idx;
    }

    pub fn set_promise(&mut self, prom: Promise<T>, from: u64, check_max_prom: bool) -> bool {
        let promise_meta = PromiseMetaData {
            n: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            pid: from,
            stopsign: prom.stopsign,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise = Some(PromiseData {
                decided_snapshot: prom.decided_snapshot,
                suffix: prom.suffix,
            })
        }
        self.decided_indexes[Self::pid_to_idx(from)] = Some(prom.decided_idx);
        self.promises_meta[Self::pid_to_idx(from)] = Some(promise_meta);
        let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
        self.quorum.is_prepare_quorum(num_promised)
    }

    pub fn take_max_promise(&mut self) -> Option<PromiseData<T>> {
        std::mem::take(&mut self.max_promise)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn set_accepted_stopsign(&mut self, from: NodeId) {
        self.accepted_stopsign[Self::pid_to_idx(from)] = true;
    }

    pub fn follower_has_accepted_stopsign(&mut self, from: NodeId) -> bool {
        self.accepted_stopsign[Self::pid_to_idx(from)]
    }

    pub fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        self.promises_meta[Self::pid_to_idx(pid)]
            .as_ref()
            .expect("No Metadata found for promised follower")
    }

    pub fn get_min_all_accepted_idx(&self) -> &u64 {
        self.accepted_indexes
            .iter()
            .min()
            .expect("Should be all initialised to 0!")
    }

    #[cfg(feature = "batch_accept")]
    pub fn reset_batch_accept_meta(&mut self) {
        self.batch_accept_meta = vec![None; self.max_pid];
    }

    pub fn get_promised_followers(&self) -> Vec<NodeId> {
        self.decided_indexes
            .iter()
            .enumerate()
            .filter(|(pid, x)| x.is_some() && *pid != Self::pid_to_idx(self.n_leader.pid))
            .map(|(idx, _)| (idx + 1) as NodeId)
            .collect()
    }

    pub fn get_unpromised_peers(&self) -> Vec<NodeId> {
        self.decided_indexes
            .iter()
            .enumerate()
            .filter(|(pid, x)| x.is_none() && *pid != Self::pid_to_idx(self.n_leader.pid))
            .map(|(idx, _)| (idx + 1) as NodeId)
            .collect()
    }

    #[cfg(feature = "batch_accept")]
    pub fn set_batch_accept_meta(&mut self, pid: NodeId, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.batch_accept_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub fn set_accepted_idx(&mut self, pid: NodeId, idx: u64) {
        self.accepted_indexes[Self::pid_to_idx(pid)] = idx;
    }

    #[cfg(feature = "batch_accept")]
    pub fn get_batch_accept_meta(&self, pid: NodeId) -> Option<(Ballot, usize)> {
        self.batch_accept_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }

    pub fn get_decided_idx(&self, pid: NodeId) -> &Option<u64> {
        self.decided_indexes.get(Self::pid_to_idx(pid)).unwrap()
    }

    pub fn is_stopsign_chosen(&self) -> bool {
        let num_accepted = self.accepted_stopsign.iter().filter(|x| **x).count();
        self.quorum.is_accept_quorum(num_accepted)
    }

    pub fn is_chosen(&self, idx: u64) -> bool {
        let num_accepted = self
            .accepted_indexes
            .iter()
            .filter(|la| **la >= idx)
            .count();
        self.quorum.is_accept_quorum(num_accepted)
    }

    pub fn take_max_promise_stopsign(&mut self) -> Option<StopSign> {
        self.max_promise_meta.stopsign.take()
    }
}

/// The entry read in the log.
#[derive(Debug, Clone)]
pub enum LogEntry<T>
where
    T: Entry,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedIndex),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T>),
    /// This Sequence Paxos instance has been stopped for reconfiguration.
    StopSign(StopSign),
}

/// Convenience struct for checking if a certain index exists, is compacted or is a StopSign.
#[derive(Debug, Clone)]
pub(crate) enum IndexEntry {
    Entry,
    Compacted,
    StopSign(StopSign),
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct SnapshottedEntry<T>
where
    T: Entry,
{
    pub trimmed_idx: TrimmedIndex,
    pub snapshot: T::Snapshot,
    _p: PhantomData<T>,
}

impl<T> SnapshottedEntry<T>
where
    T: Entry,
{
    pub(crate) fn with(trimmed_idx: u64, snapshot: T::Snapshot) -> Self {
        Self {
            trimmed_idx,
            snapshot,
            _p: PhantomData,
        }
    }
}

pub(crate) mod defaults {
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 10;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
}

#[allow(missing_docs)]
pub type TrimmedIndex = u64;

/// ID for an OmniPaxos node
pub type NodeId = u64;
/// ID for an OmniPaxos configuration (i.e., the set of servers in an OmniPaxos cluster)
pub type ConfigurationId = u32;

/// Used for checking the ordering of message sequences in the accept phase
#[derive(PartialEq, Eq)]
pub(crate) enum MessageStatus {
    /// Expected message sequence progression
    Expected,
    /// Identified a message sequence break
    DroppedPreceding,
    /// An already identified message sequence break
    Outdated,
}

/// Keeps track of the ordering of messages in the accept phase
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SequenceNumber {
    /// Meant to refer to a TCP session
    pub session: u64,
    /// The sequence number with respect to a session
    pub counter: u64,
}

impl SequenceNumber {
    /// Compares this sequence number with the sequence number of an incoming message.
    pub(crate) fn check_msg_status(&self, msg_seq_num: SequenceNumber) -> MessageStatus {
        if msg_seq_num.session == self.session && msg_seq_num.counter == self.counter + 1 {
            MessageStatus::Expected
        } else if msg_seq_num <= *self {
            MessageStatus::Outdated
        } else {
            MessageStatus::DroppedPreceding
        }
    }
}

pub(crate) struct LogicalClock {
    time: u64,
    timeout: u64,
}

impl LogicalClock {
    pub fn with(timeout: u64) -> Self {
        Self { time: 0, timeout }
    }

    pub fn tick_and_check_timeout(&mut self) -> bool {
        self.time += 1;
        if self.time == self.timeout {
            self.time = 0;
            true
        } else {
            false
        }
    }
}

/// Flexible quorums can be used to increase/decrease the read and write quorum sizes,
/// for different latency vs fault tolerance tradeoffs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlexibleQuorum {
    /// The number of nodes a leader needs to consult to get an up-to-date view of the log.
    pub read_quorum_size: usize,
    /// The number of acknowledgments a leader needs to commit an entry to the log
    pub write_quorum_size: usize,
}

/// The type of quorum used by the OmniPaxos cluster.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Quorum {
    /// Both the read quorum and the write quorums are a majority of nodes
    Majority(usize),
    /// The read and write quorum sizes are defined by a `FlexibleQuorum`
    Flexible(FlexibleQuorum),
}

impl Quorum {
    pub(crate) fn with(flexible_quorum_config: Option<FlexibleQuorum>, num_nodes: usize) -> Self {
        match flexible_quorum_config {
            Some(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }) => Quorum::Flexible(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }),
            None => Quorum::Majority(num_nodes / 2 + 1),
        }
    }

    pub(crate) fn is_prepare_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.read_quorum_size,
        }
    }

    pub(crate) fn is_accept_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.write_quorum_size,
        }
    }
}
