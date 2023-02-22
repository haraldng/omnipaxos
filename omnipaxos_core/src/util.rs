use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::Promise,
    storage::{Entry, Snapshot, SnapshotType, StopSign},
};
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData};

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
pub(crate) struct LeaderState<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    pub n_leader: Ballot,
    pub promises_meta: Vec<Option<PromiseMetaData>>,
    pub accepted_indexes: Vec<u64>,
    pub decided_indexes: Vec<Option<u64>>,
    pub chosen_idx: u64, // length of longest chosen seq
    pub max_promise_meta: PromiseMetaData,
    pub max_promise: Option<(Option<SnapshotType<T, S>>, Vec<T>)>, // (decided_snapshot, suffix)
    #[cfg(feature = "batch_accept")]
    pub batch_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub accepted_stopsign: Vec<bool>,
    pub max_pid: usize,
    pub majority: usize,
}

impl<T, S> LeaderState<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    pub fn with(
        n_leader: Ballot,
        decided_indexes: Option<Vec<Option<u64>>>,
        max_pid: usize,
        majority: usize,
    ) -> Self {
        Self {
            n_leader,
            promises_meta: vec![None; max_pid],
            accepted_indexes: vec![0; max_pid],
            decided_indexes: decided_indexes.unwrap_or_else(|| vec![None; max_pid]),
            chosen_idx: 0,
            max_promise_meta: PromiseMetaData::default(),
            max_promise: None,
            #[cfg(feature = "batch_accept")]
            batch_accept_meta: vec![None; max_pid],
            accepted_stopsign: vec![false; max_pid],
            max_pid,
            majority,
        }
    }

    fn pid_to_idx(pid: NodeId) -> usize {
        (pid - 1) as usize
    }

    pub fn set_decided_idx(&mut self, pid: NodeId, idx: Option<u64>) {
        self.decided_indexes[Self::pid_to_idx(pid)] = idx;
    }

    pub fn set_promise(&mut self, prom: Promise<T, S>, from: u64, check_max_prom: bool) -> bool {
        let promise_meta = PromiseMetaData {
            n: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            pid: from,
            stopsign: prom.stopsign,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise = Some((prom.decided_snapshot, prom.suffix))
        }
        self.decided_indexes[Self::pid_to_idx(from)] = Some(prom.decided_idx);
        self.promises_meta[Self::pid_to_idx(from)] = Some(promise_meta);
        let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
        num_promised >= self.majority
    }

    pub fn take_max_promise(&mut self) -> Option<(Option<SnapshotType<T, S>>, Vec<T>)> {
        std::mem::take(&mut self.max_promise)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn set_accepted_stopsign(&mut self, from: u64) {
        self.accepted_stopsign[Self::pid_to_idx(from)] = true;
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

    pub fn set_chosen_idx(&mut self, idx: u64) {
        self.chosen_idx = idx;
    }

    pub fn get_chosen_idx(&self) -> u64 {
        self.chosen_idx
    }

    pub fn get_promised_followers(&self) -> Vec<u64> {
        self.decided_indexes
            .iter()
            .enumerate()
            .filter(|(pid, x)| x.is_some() && *pid != Self::pid_to_idx(self.n_leader.pid))
            .map(|(idx, _)| (idx + 1) as u64)
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
        num_accepted >= self.majority
    }

    pub fn is_chosen(&self, idx: u64) -> bool {
        self.accepted_indexes
            .iter()
            .filter(|la| **la >= idx)
            .count()
            >= self.majority
    }

    pub fn take_max_promise_stopsign(&mut self) -> Option<StopSign> {
        self.max_promise_meta.stopsign.take()
    }
}

/*
/// Item used for log synchronization in the Prepare phase.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum SyncItem<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    Entries(Vec<T>),
    Snapshot(SnapshotType<T, S>),
    None,
}

impl<T, S> Default for SyncItem<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    fn default() -> Self {
        SyncItem::None
    }
}
*/

/// The entry read in the log.
#[derive(Debug, Clone)]
pub enum LogEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedIndex),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T, S>),
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
pub struct SnapshottedEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    pub trimmed_idx: TrimmedIndex,
    pub snapshot: S,
    _p: PhantomData<T>,
}

impl<T, S> SnapshottedEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    pub(crate) fn with(trimmed_idx: u64, snapshot: S) -> Self {
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
}

#[allow(missing_docs)]
pub type TrimmedIndex = u64;

/// ID for an OmniPaxos node
pub type NodeId = u64;
/// ID for an OmniPaxos configuration (i.e., the set of servers in an OmniPaxos cluster)
pub type ConfigurationId = u32;
