use super::storage::{Entry, SnapshotType, StopSign};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData};

/// Struct used to help another server synchronize their log with the current state of our own log.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogSync<T>
where
    T: Entry,
{
    /// The decided snapshot.
    pub decided_snapshot: Option<SnapshotType<T>>,
    /// The log suffix.
    pub suffix: Vec<T>,
    /// The index of the log where the entries from `suffix` should be applied at (also the compacted idx of `decided_snapshot` if it exists).
    pub sync_idx: usize,
    /// The accepted StopSign.
    pub stopsign: Option<StopSign>,
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
    /// This Sequence Paxos instance has been stopped for reconfiguration. The accompanying bool
    /// indicates whether the reconfiguration has been decided or not. If it is `true`, then the OmniPaxos instance for the new configuration can be started.
    StopSign(StopSign, bool),
}

impl<T: PartialEq + Entry> PartialEq for LogEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntry::Decided(v1), LogEntry::Decided(v2)) => v1 == v2,
            (LogEntry::Undecided(v1), LogEntry::Undecided(v2)) => v1 == v2,
            (LogEntry::Trimmed(idx1), LogEntry::Trimmed(idx2)) => idx1 == idx2,
            (LogEntry::Snapshotted(s1), LogEntry::Snapshotted(s2)) => s1 == s2,
            (LogEntry::StopSign(ss1, b1), LogEntry::StopSign(ss2, b2)) => ss1 == ss2 && b1 == b2,
            _ => false,
        }
    }
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
    pub(crate) fn with(trimmed_idx: usize, snapshot: T::Snapshot) -> Self {
        Self {
            trimmed_idx,
            snapshot,
            _p: PhantomData,
        }
    }
}

impl<T: Entry> PartialEq for SnapshottedEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.trimmed_idx == other.trimmed_idx && self.snapshot == other.snapshot
    }
}

pub(crate) mod defaults {
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 10;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
}

#[allow(missing_docs)]
pub type TrimmedIndex = usize;

/// ID for an OmniPaxos node
pub type NodeId = u64;
/// ID for an OmniPaxos configuration (i.e., the set of servers in an OmniPaxos cluster)
pub type ConfigurationId = u32;

/// Error message to display when there was an error reading to the storage implementation.
pub const READ_ERROR_MSG: &str = "Error reading from storage.";
/// Error message to display when there was an error writing to the storage implementation.
pub const WRITE_ERROR_MSG: &str = "Error writing to storage.";

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

    pub fn check_timeout(&self) -> bool {
        self.timeout == 0
    }

    pub fn reset(&mut self) {
        self.time = 0;
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

/// The entries flushed due to an append operation
pub(crate) struct AcceptedMetaData<T: Entry> {
    pub accepted_idx: usize,
    #[cfg(not(feature = "unicache"))]
    pub entries: Vec<T>,
    #[cfg(feature = "unicache")]
    pub entries: Vec<T::EncodeResult>,
}
