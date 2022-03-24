use super::{
    ballot_leader_election::Ballot,
    messages::Promise,
    storage::{Entry, Snapshot, SnapshotType, StopSign},
};
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
/// Promise without the suffix
pub(crate) struct PromiseMetaData {
    pub n: Ballot,
    pub la: u64,
    pub pid: u64,
    pub stopsign: Option<StopSign>,
}

impl PromiseMetaData {
    pub fn with(n: Ballot, la: u64, pid: u64, stopsign: Option<StopSign>) -> Self {
        Self {
            n,
            la,
            pid,
            stopsign,
        }
    }
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n == other.n && self.la == other.la && self.pid == other.pid {
            Ordering::Equal
        } else if self.n > other.n || (self.n == other.n && self.la > other.la) {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n && self.la == other.la && self.pid == other.pid
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
    pub las: Vec<u64>,// length of longest accepted sequence per acceptor
    pub lds: Vec<Option<u64>>,// length of longest known decided sequence per acceptor
    pub lc: u64, // length of longest chosen seq
    pub max_promise_meta: PromiseMetaData,
    pub max_promise: SyncItem<T, S>,
    pub batch_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub latest_decide_meta: Vec<Option<(Ballot, usize)>>,
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
        lds: Option<Vec<Option<u64>>>,
        max_pid: usize,
        majority: usize,
    ) -> Self {
        Self {
            n_leader,
            promises_meta: vec![None; max_pid],
            las: vec![0; max_pid],
            lds: lds.unwrap_or_else(|| vec![None; max_pid]),
            lc: 0,
            max_promise_meta: PromiseMetaData::default(),
            max_promise: SyncItem::None,
            batch_accept_meta: vec![None; max_pid],
            latest_decide_meta: vec![None; max_pid],
            accepted_stopsign: vec![false; max_pid],
            max_pid,
            majority,
        }
    }

    fn pid_to_idx(pid: u64) -> usize {
        (pid - 1) as usize
    }

    pub fn set_decided_idx(&mut self, pid: u64, idx: Option<u64>) {
        self.lds[Self::pid_to_idx(pid)] = idx;
    }

    pub fn set_promise(&mut self, prom: Promise<T, S>, from: u64) -> bool {
        let promise_meta = PromiseMetaData::with(prom.n_accepted, prom.la, from, prom.stopsign);
        if promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise = prom.sync_item.unwrap_or(SyncItem::None); // TODO: this should be fine?
        }
        self.lds[Self::pid_to_idx(from)] = Some(prom.ld);
        self.promises_meta[Self::pid_to_idx(from)] = Some(promise_meta);
        let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
        num_promised >= self.majority
    }

    pub fn take_max_promise(&mut self) -> SyncItem<T, S> {
        std::mem::take(&mut self.max_promise)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn set_accepted_stopsign(&mut self, from: u64) {
        self.accepted_stopsign[Self::pid_to_idx(from)] = true;
    }

    pub fn get_promise_meta(&self, pid: u64) -> &PromiseMetaData {
        self.promises_meta[Self::pid_to_idx(pid)]
            .as_ref()
            .expect("No Metadata found for promised follower")
    }

    pub fn get_min_all_accepted_idx(&self) -> &u64 {
        self.las
            .iter()
            .min()
            .expect("Should be all initialised to 0!")
    }

    pub fn reset_batch_accept_meta(&mut self) {
        self.batch_accept_meta = vec![None; self.max_pid];
    }

    pub fn reset_latest_decided_meta(&mut self) {
        self.latest_decide_meta = vec![None; self.max_pid];
    }

    pub fn set_chosen_idx(&mut self, idx: u64) {
        self.lc = idx;
    }

    pub fn get_chosen_idx(&self) -> u64 {
        self.lc
    }

    pub fn get_promised_followers(&self) -> Vec<u64> {
        self.lds
            .iter()
            .enumerate()
            .filter(|(pid, x)| x.is_some() && *pid != Self::pid_to_idx(self.n_leader.pid))
            .map(|(idx, _)| (idx + 1) as u64)
            .collect()
    }

    pub fn set_batch_accept_meta(&mut self, pid: u64, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.batch_accept_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub fn set_latest_decide_meta(&mut self, pid: u64, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.latest_decide_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub fn set_accepted_idx(&mut self, pid: u64, idx: u64) {
        self.las[Self::pid_to_idx(pid)] = idx;
    }

    pub fn get_batch_accept_meta(&self, pid: u64) -> Option<(Ballot, usize)> {
        self.batch_accept_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }
    pub fn get_latest_decide_meta(&self, pid: u64) -> Option<(Ballot, usize)> {
        self.latest_decide_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }

    pub fn get_decided_idx(&self, pid: u64) -> &Option<u64> {
        self.lds.get(Self::pid_to_idx(pid)).unwrap()
    }

    pub fn is_stopsign_chosen(&self) -> bool {
        let num_accepted = self.accepted_stopsign.iter().filter(|x| **x).count();
        num_accepted >= self.majority
    }

    pub fn is_chosen(&self, idx: u64) -> bool {
        self.las.iter().filter(|la| **la >= idx).count() >= self.majority
    }

    pub fn take_max_promise_stopsign(&mut self) -> Option<StopSign> {
        self.max_promise_meta.stopsign.take()
    }
}

/// Item used for log synchronization in the Prepare phase.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// The entry read in the log.
#[derive(Debug, Clone)]
pub enum LogEntry<'a, T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// The entry is decided.
    Decided(&'a T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(&'a T),
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
    pub(crate) const HB_TIMEOUT: u64 = 500;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
}

#[allow(missing_docs)]
pub type TrimmedIndex = u64;
