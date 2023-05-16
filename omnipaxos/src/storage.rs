use super::ballot_leader_election::Ballot;
use crate::{
    util::{ConfigurationId, IndexEntry, LogEntry, NodeId, SnapshottedEntry},
    CompactionErr, ClusterConfig,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

/// Type of the entries stored in the log.
pub trait Entry: Clone + Debug {
    #[cfg(not(feature = "serde"))]
    /// The snapshot type for this entry type.
    type Snapshot: Snapshot<Self>;

    #[cfg(feature = "serde")]
    /// The snapshot type for this entry type.
    type Snapshot: Snapshot<Self> + Serialize + for<'a> Deserialize<'a>;
}

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
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StopSign {
    /// The new `Omnipaxos` cluster configuration
    pub next_config: ClusterConfig,
}

impl StopSign {
    /// Creates a [`StopSign`].
    pub fn with(next_config: ClusterConfig) -> Self {
        StopSign {
            next_config
        }
    }
}

/// Snapshot type. A `Complete` snapshot contains all snapshotted data while `Delta` has snapshotted changes since an earlier snapshot.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SnapshotType<T>
where
    T: Entry,
{
    Complete(T::Snapshot),
    Delta(T::Snapshot),
}

/// Trait for implementing snapshot operations for log entries of type `T` in OmniPaxos.
pub trait Snapshot<T>: Clone + Debug
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
pub trait Storage<T>
where
    T: Entry,
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

    /// Returns the latest round in which entries have been accepted.
    fn get_accepted_round(&self) -> Ballot;

    /// Returns the entries in the log in the index interval of [from, to).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    fn get_entries(&self, from: u64, to: u64) -> Vec<T>;

    /// Returns the current length of the log.
    fn get_log_len(&self) -> u64;

    /// Returns the suffix of entries in the log from index `from`.
    fn get_suffix(&self, from: u64) -> Vec<T>;

    /// Returns the round that has been promised.
    fn get_promise(&self) -> Ballot;

    /// Sets the StopSign used for reconfiguration.
    fn set_stopsign(&mut self, s: StopSignEntry);

    /// Returns the stored StopSign.
    fn get_stopsign(&self) -> Option<StopSignEntry>;

    /// Removes elements up to the given [`idx`] from storage.
    fn trim(&mut self, idx: u64);

    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    fn set_compacted_idx(&mut self, idx: u64);

    /// Returns the garbage collector index from storage.
    fn get_compacted_idx(&self) -> u64;

    /// Sets the snapshot.
    fn set_snapshot(&mut self, snapshot: T::Snapshot);

    /// Returns the stored snapshot.
    fn get_snapshot(&self) -> Option<T::Snapshot>;
}

/// A place holder type for when not using snapshots. You should not use this type, it is only internally when deriving the Entry implementation.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NoSnapshot;

impl<T: Entry> Snapshot<T> for NoSnapshot {
    fn create(_entries: &[T]) -> Self {
        panic!("NoSnapshot should not be created");
    }

    fn merge(&mut self, _delta: Self) {
        panic!("NoSnapshot should not be merged");
    }

    fn use_snapshots() -> bool {
        false
    }
}

/// Internal representation of storage. Hides all complexities with the compacted index
/// such that Sequence Paxos accesses the log with the uncompacted index.
pub(crate) struct InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    storage: I,
    _t: PhantomData<T>,
}

impl<I, T> InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    pub(crate) fn with(storage: I) -> Self {
        InternalStorage {
            storage,
            _t: Default::default(),
        }
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
    pub(crate) fn read<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
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
                let idx = self.get_log_len();
                match self.get_stopsign() {
                    Some(ss) if ss.decided => idx + 1,
                    _ => idx,
                }
            }
        };
        let compacted_idx = self.get_compacted_idx();
        let virtual_log_len = self.get_log_len();
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
    ) -> Vec<LogEntry<T>> {
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
    pub(crate) fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<LogEntry<T>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx)
        } else {
            None
        }
    }

    fn create_compacted_entry(&self, compacted_idx: u64) -> LogEntry<T> {
        match self.storage.get_snapshot() {
            Some(s) => LogEntry::Snapshotted(SnapshottedEntry::with(compacted_idx, s)),
            None => LogEntry::Trimmed(compacted_idx),
        }
    }

    /*** Writing ***/
    pub(crate) fn append_entry(&mut self, entry: T) -> u64 {
        self.storage.append_entry(entry) + self.storage.get_compacted_idx()
    }

    pub(crate) fn append_entries(&mut self, entries: Vec<T>) -> u64 {
        self.storage.append_entries(entries) + self.storage.get_compacted_idx()
    }

    pub(crate) fn append_on_decided_prefix(&mut self, entries: Vec<T>) -> u64 {
        let decided_idx = self.storage.get_decided_idx();
        let compacted_idx = self.storage.get_compacted_idx();
        self.storage
            .append_on_prefix(decided_idx - compacted_idx, entries)
            + compacted_idx
    }

    pub(crate) fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
        let compacted_idx = self.storage.get_compacted_idx();
        self.storage
            .append_on_prefix(from_idx - compacted_idx, entries)
            + compacted_idx
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) {
        self.storage.set_promise(n_prom)
    }

    pub(crate) fn set_decided_idx(&mut self, ld: u64) {
        self.storage.set_decided_idx(ld)
    }

    pub(crate) fn get_decided_idx(&self) -> u64 {
        self.storage.get_decided_idx()
    }

    pub(crate) fn set_accepted_round(&mut self, na: Ballot) {
        self.storage.set_accepted_round(na)
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.storage.get_accepted_round()
    }

    pub(crate) fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
        let compacted_idx = self.storage.get_compacted_idx();
        self.get_entries_with_real_idx(from - compacted_idx.min(from), to - compacted_idx.min(to))
    }

    /// Get entries with real physical log indexes i.e. the index with the compacted offset.
    fn get_entries_with_real_idx(&self, from_sfx_idx: u64, to_sfx_idx: u64) -> Vec<T> {
        self.storage.get_entries(from_sfx_idx, to_sfx_idx)
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_log_len(&self) -> u64 {
        self.get_real_log_len() + self.storage.get_compacted_idx()
    }

    /// The length of the physical log, which can get smaller with compaction
    fn get_real_log_len(&self) -> u64 {
        self.storage.get_log_len()
    }

    pub(crate) fn get_suffix(&self, from: u64) -> Vec<T> {
        self.storage
            .get_suffix(from - self.storage.get_compacted_idx().min(from))
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.storage.get_promise()
    }

    pub(crate) fn set_stopsign(&mut self, s: StopSignEntry) {
        self.storage.set_stopsign(s)
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSignEntry> {
        self.storage.get_stopsign()
    }

    pub(crate) fn create_snapshot(&mut self, compact_idx: u64) -> T::Snapshot {
        let entries = self
            .storage
            .get_entries(0, compact_idx - self.storage.get_compacted_idx());
        let delta = T::Snapshot::create(entries.as_slice());
        match self.storage.get_snapshot() {
            Some(mut s) => {
                s.merge(delta);
                s
            }
            None => delta,
        }
    }

    pub(crate) fn create_diff_snapshot(&mut self, from_idx: u64, to_idx: u64) -> SnapshotType<T> {
        if self.get_compacted_idx() >= from_idx {
            SnapshotType::Complete(self.create_snapshot(to_idx))
        } else {
            let diff_entries = self.get_entries(from_idx, to_idx);
            SnapshotType::Delta(T::Snapshot::create(diff_entries.as_slice()))
        }
    }

    pub(crate) fn set_snapshot(&mut self, idx: u64, snapshot: T::Snapshot) {
        let compacted_idx = self.storage.get_compacted_idx();
        if idx > compacted_idx {
            self.storage.trim(idx - compacted_idx);
            self.storage.set_snapshot(snapshot);
            self.storage.set_compacted_idx(idx);
        }
    }

    pub(crate) fn merge_snapshot(&mut self, idx: u64, delta: T::Snapshot) {
        let mut snapshot = self
            .storage
            .get_snapshot()
            .unwrap_or_else(|| self.create_snapshot(self.storage.get_log_len()));
        snapshot.merge(delta);
        self.set_snapshot(idx, snapshot);
    }

    pub(crate) fn try_trim(&mut self, idx: u64) -> Result<(), CompactionErr> {
        let compacted_idx = self.storage.get_compacted_idx();
        if idx <= compacted_idx {
            Ok(()) // already trimmed or snapshotted this index.
        } else {
            let decided_idx = self.storage.get_decided_idx();
            if idx <= decided_idx {
                self.storage.trim(idx - compacted_idx);
                self.storage.set_compacted_idx(idx);
                Ok(())
            } else {
                Err(CompactionErr::UndecidedIndex(decided_idx))
            }
        }
    }

    pub(crate) fn get_compacted_idx(&self) -> u64 {
        self.storage.get_compacted_idx()
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
