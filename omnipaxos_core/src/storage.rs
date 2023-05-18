use super::ballot_leader_election::Ballot;
use crate::{
    omni_paxos::CompactionErr,
    util::{ConfigurationId, IndexEntry, LogEntry, NodeId, SnapshottedEntry},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

/// Type of the entries stored in the log.
pub trait Entry: Clone + Debug {}
impl<T> Entry for T where T: Clone + Debug {}

/// A StopSign entry that marks the end of a configuration. Used for reconfiguration.
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct StopSignEntry {
    pub stopsign: StopSign,
    pub decided: bool,
}

impl StopSignEntry {
    /// Creates a [`StopSign`].
    pub fn with(stopsign: StopSign, decided: bool) -> Self {
        StopSignEntry { stopsign, decided }
    }
}

/// A StopSign entry that marks the end of a configuration. Used for reconfiguration.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StopSign {
    /// The identifier for the new configuration.
    pub config_id: ConfigurationId,
    /// The process ids of the new configuration.
    pub nodes: Vec<NodeId>,
    /// Metadata for the reconfiguration. Can be used for pre-electing leader for the new configuration and skip prepare phase when starting the new configuration with the given leader.
    pub metadata: Option<Vec<u8>>,
}

impl StopSign {
    /// Creates a [`StopSign`].
    pub fn with(config_id: ConfigurationId, nodes: Vec<NodeId>, metadata: Option<Vec<u8>>) -> Self {
        StopSign {
            config_id,
            nodes,
            metadata,
        }
    }
}

impl PartialEq for StopSign {
    fn eq(&self, other: &Self) -> bool {
        self.config_id == other.config_id && self.nodes == other.nodes
    }
}

/// Snapshot type. A `Complete` snapshot contains all snapshotted data while `Delta` has snapshotted changes since an earlier snapshot.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SnapshotType<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    Complete(S),
    Delta(S),
    _Phantom(PhantomData<T>),
}

/// Functions required by Sequence Paxos to implement snapshot operations for `T`. If snapshot is not desired to be used, use the unit type `()` as the Snapshot parameter in `SequencePaxos`.
pub trait Snapshot<T>: Clone
where
    T: Entry,
{
    /// Create a snapshot from the log `entries`.
    fn create(entries: &[T]) -> Self;

    /// Merge another snapshot `delta` into self.
    fn merge(&mut self, delta: Self);

    /// Whether `T` is snapshottable. If not, simply return `false` and leave the other functions `unimplemented!()`.
    fn use_snapshots() -> bool;

    //fn size_hint() -> u64;  // TODO: To let the system know trade-off of using entries vs snapshot?
}

/// Trait for implementing the storage backend of Sequence Paxos.
pub trait Storage<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Appends an entry to the end of the log and returns the log length.
    fn append_entry(&mut self, entry: T) -> u64;

    /// Appends the entries of `entries` to the end of the log and returns the log length.
    fn append_entries(&mut self, entries: Vec<T>) -> u64;

    /// Appends the entries of `entries` to the prefix from index `from_index` in the log and returns the log length.
    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, n_prom: Ballot);

    /// Sets the decided index in the log.
    fn set_decided_idx(&mut self, ld: u64);

    /// Returns the decided index in the log.
    fn get_decided_idx(&self) -> u64;

    /// Sets the latest accepted round.
    fn set_accepted_round(&mut self, na: Ballot);

    /// Returns the latest round in which entries have been accepted, returns `None` if no
    /// entries have been accepted.
    fn get_accepted_round(&self) -> Option<Ballot>;

    /// Returns the entries in the log in the index interval of [from, to).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    fn get_entries(&self, from: u64, to: u64) -> Vec<T>;

    /// Returns the current length of the log.
    fn get_log_len(&self) -> u64;

    /// Returns the suffix of entries in the log from index `from`.
    fn get_suffix(&self, from: u64) -> Vec<T>;

    /// Returns the round that has been promised, returns `None` if no promise has been made.
    fn get_promise(&self) -> Option<Ballot>;

    /// Sets the StopSign used for reconfiguration.
    fn set_stopsign(&mut self, s: StopSignEntry);

    /// Returns the stored StopSign, returns `None` if no StopSign has been stored.
    fn get_stopsign(&self) -> Option<StopSignEntry>;

    /// Removes elements up to the given [`idx`] from storage.
    fn trim(&mut self, idx: u64);

    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    fn set_compacted_idx(&mut self, idx: u64);

    /// Returns the garbage collector index from storage.
    fn get_compacted_idx(&self) -> u64;

    /// Sets the snapshot.
    fn set_snapshot(&mut self, snapshot: S);

    /// Returns the stored snapshot.
    fn get_snapshot(&self) -> Option<S>;
}

#[allow(missing_docs)]

impl<T: Entry> Snapshot<T> for () {
    fn create(_: &[T]) -> Self {
        unimplemented!()
    }

    fn merge(&mut self, _: Self) {
        unimplemented!()
    }

    fn use_snapshots() -> bool {
        false
    }
}

/// A simple in-memory storage for simple state values of OmniPaxos.
#[derive(Clone)]
struct StateCache<T>
    where
        T: Entry,
{
    /// The maximum number of entries to batch.
    batch_size: usize,
    /// Vector which contains all the logged entries in-memory.
    batched_entries: Vec<T>,
    /// Last promised round.
    n_prom: Ballot,
    /// Last accepted round.
    acc_round: Ballot,
    /// Length of the decided log.
    ld: u64,
    /// Garbage collected index.
    compacted_idx: u64,
    /// Real length of logs in the storage.
    real_log_len: u64,
}


impl<T> StateCache<T>
    where
        T: Entry
 {
    pub fn new(batch_size: usize) -> Self {
        StateCache {
            batch_size,
            batched_entries: Vec::with_capacity(batch_size),
            n_prom: Ballot::default(),
            acc_round: Ballot::default(),
            ld: 0,
            compacted_idx: 0,
            real_log_len: 0,
        }
    }

    // Returns the index of the last accepted entry.
    fn get_accepted_idx(&self) -> u64 {
        self.compacted_idx + self.real_log_len
    }

    // Appends an entry to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    fn append_entry(&mut self, entry: T) -> Option<Vec<T>> {
        self.batched_entries.push(entry);
        self.take_entries_if_batch_full()
    }

    // Appends entries to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    fn append_entries(&mut self, entries: Vec<T>) -> Option<Vec<T>> {
        self.batched_entries.extend(entries);
        self.take_entries_if_batch_full()
    }

    // Return batched entries if the batch is full that need to be flushed in to storage.
    fn take_entries_if_batch_full(&mut self) -> Option<Vec<T>> {
        if self.batched_entries.len() >= self.batch_size {
            Some(self.take_batched_entries())
        } else {
            None
        }
    }

    // Clears the batched entries and returns the cleared entries. If the batch is empty,
    // return an empty vector.
    fn take_batched_entries(&mut self) -> Vec<T> {
        std::mem::take(&mut self.batched_entries)
    }
}

/// Internal representation of storage. Hides all complexities with the compacted index
/// such that Sequence Paxos accesses the log with the uncompacted index.
pub(crate) struct InternalStorage<I, T, S>
where
    I: Storage<T, S>,
    T: Entry,
    S: Snapshot<T>,
{
    storage: I,
    state_cache: StateCache<T>,
    _t: PhantomData<T>,
    _i: PhantomData<S>,
}

impl<I, T, S> InternalStorage<I, T, S>
where
    I: Storage<T, S>,
    T: Entry,
    S: Snapshot<T>,
{
    pub(crate) fn with(storage: I, batch_size: usize) -> Self {
        let mut internal_store = InternalStorage {
            storage,
            state_cache: StateCache::new(batch_size),
            _t: Default::default(),
            _i: Default::default(),
        };
        internal_store.load_cache();
        internal_store
    }

    fn get_entry_type(
        &self,
        idx: u64,
        compacted_idx: u64,
        virtual_log_len: u64,
    ) -> Option<IndexEntry> {
        if idx < compacted_idx {
            Some(IndexEntry::Compacted)
        } else if idx < virtual_log_len {
            Some(IndexEntry::Entry)
        } else if idx == virtual_log_len {
            match self.get_stopsign() {
                Some(ss) if ss.decided => Some(IndexEntry::StopSign(ss.stopsign)),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub(crate) fn read<R>(&self, r: R) -> Option<Vec<LogEntry<T, S>>>
    where
        R: RangeBounds<u64>,
    {
        let from_idx = match r.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(e) => *e + 1,
            Bound::Unbounded => 0,
        };
        let to_idx = match r.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => {
                let idx = self.get_accepted_idx();
                match self.get_stopsign() {
                    Some(ss) if ss.decided => idx + 1,
                    _ => idx,
                }
            }
        };
        let compacted_idx = self.get_compacted_idx();
        let virtual_log_len = self.get_accepted_idx();
        let to_type = match self.get_entry_type(to_idx - 1, compacted_idx, virtual_log_len) {
            // use to_idx-1 when getting the entry type as to_idx is exclusive
            Some(IndexEntry::Compacted) => {
                return Some(vec![self.create_compacted_entry(compacted_idx)])
            }
            Some(from_type) => from_type,
            _ => return None,
        };
        let from_type = match self.get_entry_type(from_idx, compacted_idx, virtual_log_len) {
            Some(from_type) => from_type,
            _ => return None,
        };
        let decided_idx = self.get_decided_idx();
        match (from_type, to_type) {
            (IndexEntry::Entry, IndexEntry::Entry) => {
                let from_suffix_idx = from_idx - compacted_idx;
                let to_suffix_idx = to_idx - compacted_idx;
                Some(self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                ))
            }
            (IndexEntry::Entry, IndexEntry::StopSign(ss)) => {
                let from_suffix_idx = from_idx - compacted_idx;
                let to_suffix_idx = to_idx - compacted_idx - 1;
                let mut entries = self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                );
                entries.push(LogEntry::StopSign(ss));
                Some(entries)
            }
            (IndexEntry::Compacted, IndexEntry::Entry) => {
                let from_suffix_idx = 0;
                let to_suffix_idx = to_idx - compacted_idx;
                let mut entries = Vec::with_capacity((to_suffix_idx + 1) as usize);
                let compacted = self.create_compacted_entry(compacted_idx);
                entries.push(compacted);
                let mut e = self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                );
                entries.append(&mut e);
                Some(entries)
            }
            (IndexEntry::Compacted, IndexEntry::StopSign(ss)) => {
                let from_suffix_idx = 0;
                let to_suffix_idx = to_idx - compacted_idx - 1;
                let mut entries = Vec::with_capacity((to_suffix_idx + 1) as usize);
                let compacted = self.create_compacted_entry(compacted_idx);
                entries.push(compacted);
                let mut e = self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                );
                entries.append(&mut e);
                entries.push(LogEntry::StopSign(ss));
                Some(entries)
            }
            (IndexEntry::StopSign(ss), IndexEntry::StopSign(_)) => {
                Some(vec![LogEntry::StopSign(ss)])
            }
            e => {
                unimplemented!("{}", format!("Unexpected read combination: {:?}", e))
            }
        }
    }

    fn create_read_log_entries_with_real_idx(
        &self,
        from_sfx_idx: u64,
        to_sfx_idx: u64,
        compacted_idx: u64,
        decided_idx: u64,
    ) -> Vec<LogEntry<T, S>> {
        self.get_entries_with_real_idx(from_sfx_idx, to_sfx_idx)
            .into_iter()
            .enumerate()
            .map(|(idx, e)| {
                let log_idx = idx as u64 + compacted_idx;
                if log_idx > decided_idx {
                    LogEntry::Undecided(e)
                } else {
                    LogEntry::Decided(e)
                }
            })
            .collect()
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub(crate) fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<LogEntry<T, S>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx)
        } else {
            None
        }
    }

    fn create_compacted_entry(&self, compacted_idx: u64) -> LogEntry<T, S> {
        match self.storage.get_snapshot() {
            Some(s) => LogEntry::Snapshotted(SnapshottedEntry::with(compacted_idx, s)),
            None => LogEntry::Trimmed(compacted_idx),
        }
    }

    fn load_cache(&mut self) {
        // try to load from storage
        if let Some(promise) = self.storage.get_promise() {
            self.state_cache.n_prom = promise;
            self.state_cache.ld = self.storage.get_decided_idx();
            self.state_cache.acc_round = self.storage.get_accepted_round().unwrap_or_default();
            self.state_cache.compacted_idx = self.storage.get_compacted_idx();
            self.state_cache.real_log_len = self.storage.get_log_len();
        }
    }

    /*** Writing ***/
    // Append entry, if the batch size is reached, flush the batch and return the actual
    // accepted index (not including the batched entries)
    pub(crate) fn append_entry(&mut self, entry: T) -> Option<(u64, Vec<T>)> {
        let append_res = self.state_cache.append_entry(entry);
        append_res.map(|flushed_entries| (self.append_entries_without_batching(flushed_entries.clone()), flushed_entries))
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index. If the batch size is not reached, return None.
    pub(crate) fn append_entries_with_batching(&mut self, entries: Vec<T>) -> Option<u64> {
        let append_res = self.state_cache.append_entries(entries);
        append_res.map(|flushed_entries| self.append_entries_without_batching(flushed_entries))
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index and the flushed entries. If the batch size is not reached, return None.
    pub(crate) fn append_entries_and_get_flushed(&mut self, entries: Vec<T>) -> Option<(u64, Vec<T>)> {
        let append_res = self.state_cache.append_entries(entries);
        append_res.map(|flushed_entries| (self.append_entries_without_batching(flushed_entries.clone()), flushed_entries))
    }

    pub(crate) fn flush_batch(&mut self) {
        let flushed_entries = self.state_cache.take_batched_entries();
        self.append_entries_without_batching(flushed_entries);
    }

    // Append entries without batching, return the accepted index
    pub(crate) fn append_entries_without_batching(&mut self, entries: Vec<T>) -> u64 {
        self.state_cache.real_log_len = self.storage.append_entries(entries);
        self.get_accepted_idx()
    }

    pub(crate) fn append_on_decided_prefix(&mut self, entries: Vec<T>) -> u64 {
        let decided_idx = self.storage.get_decided_idx();
        let compacted_idx = self.get_compacted_idx();
        self.state_cache.real_log_len = self.storage.append_on_prefix(decided_idx - compacted_idx, entries);
        self.get_accepted_idx()
    }

    pub(crate) fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
        let compacted_idx = self.get_compacted_idx();
        self.state_cache.real_log_len = self.storage.append_on_prefix(from_idx - compacted_idx, entries);
        self.get_accepted_idx()
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) {
        self.state_cache.n_prom = n_prom;
        self.storage.set_promise(n_prom)
    }

    pub(crate) fn set_decided_idx(&mut self, ld: u64) {
        self.state_cache.ld = ld;
        self.storage.set_decided_idx(ld)
    }

    pub(crate) fn get_decided_idx(&self) -> u64 {
        self.state_cache.ld
    }

    pub(crate) fn set_accepted_round(&mut self, na: Ballot) {
        self.state_cache.acc_round = na;
        self.storage.set_accepted_round(na)
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.state_cache.acc_round
    }

    pub(crate) fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
        let compacted_idx = self.get_compacted_idx();
        self.get_entries_with_real_idx(from - compacted_idx.min(from), to - compacted_idx.min(to))
    }

    /// Get entries with real physical log indexes i.e. the index with the compacted offset.
    fn get_entries_with_real_idx(&self, from_sfx_idx: u64, to_sfx_idx: u64) -> Vec<T> {
        self.storage.get_entries(from_sfx_idx, to_sfx_idx)
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_accepted_idx(&self) -> u64 {
        self.state_cache.get_accepted_idx()
    }

    pub(crate) fn get_suffix(&self, from: u64) -> Vec<T> {
        self.storage
            .get_suffix(from - self.get_compacted_idx().min(from))
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.state_cache.n_prom
    }

    pub(crate) fn set_stopsign(&mut self, s: StopSignEntry) {
        self.storage.set_stopsign(s)
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSignEntry> {
        self.storage.get_stopsign()
    }

    pub(crate) fn create_snapshot(&mut self, compact_idx: u64) -> S {
        let entries = self
            .storage
            .get_entries(0, compact_idx - self.get_compacted_idx());
        let delta = S::create(entries.as_slice());
        match self.storage.get_snapshot() {
            Some(mut s) => {
                s.merge(delta);
                s
            }
            None => delta,
        }
    }

    pub(crate) fn create_diff_snapshot(
        &mut self,
        from_idx: u64,
        to_idx: u64,
    ) -> SnapshotType<T, S> {
        if self.get_compacted_idx() >= from_idx {
            SnapshotType::Complete(self.create_snapshot(to_idx))
        } else {
            let diff_entries = self.get_entries(from_idx, to_idx);
            SnapshotType::Delta(S::create(diff_entries.as_slice()))
        }
    }

    pub(crate) fn set_snapshot(&mut self, idx: u64, snapshot: S) {
        let compacted_idx = self.get_compacted_idx();
        if idx > compacted_idx {
            self.storage.trim(idx - compacted_idx);
            self.state_cache.real_log_len = self.storage.get_log_len();
            self.storage.set_snapshot(snapshot);
            self.set_compacted_idx(idx);
        }
    }

    pub(crate) fn merge_snapshot(&mut self, idx: u64, delta: S) {
        let mut snapshot = self
            .storage
            .get_snapshot()
            .unwrap_or_else(|| self.create_snapshot(self.storage.get_log_len()));
        snapshot.merge(delta);
        self.set_snapshot(idx, snapshot);
    }

    pub(crate) fn try_trim(&mut self, idx: u64) -> Result<(), CompactionErr> {
        let compacted_idx = self.get_compacted_idx();
        if idx <= compacted_idx {
            Ok(()) // already trimmed or snapshotted this index.
        } else {
            let decided_idx = self.storage.get_decided_idx();
            if idx <= decided_idx {
                self.storage.trim(idx - compacted_idx);
                self.state_cache.real_log_len = self.storage.get_log_len();
                self.set_compacted_idx(idx);
                Ok(())
            } else {
                Err(CompactionErr::UndecidedIndex(decided_idx))
            }
        }
    }

    pub(crate) fn set_compacted_idx(&mut self, idx: u64) {
        self.state_cache.compacted_idx = idx;
        self.storage.set_compacted_idx(idx)
    }

    pub(crate) fn get_compacted_idx(&self) -> u64 {
        self.state_cache.compacted_idx
    }

    pub(crate) fn try_snapshot(&mut self, snapshot_idx: Option<u64>) -> Result<(), CompactionErr> {
        let decided_idx = self.get_decided_idx();
        let idx = match snapshot_idx {
            Some(i) => {
                if i <= decided_idx {
                    i
                } else {
                    return Err(CompactionErr::UndecidedIndex(decided_idx));
                }
            }
            None => decided_idx,
        };
        if idx > self.get_compacted_idx() {
            let snapshot = self.create_snapshot(idx);
            self.set_snapshot(idx, snapshot);
        }
        Ok(())
    }
}
