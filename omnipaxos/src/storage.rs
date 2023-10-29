use super::ballot_leader_election::Ballot;
#[cfg(feature = "unicache")]
use crate::{unicache::*, util::NodeId};
use crate::{
    util::{AcceptedMetaData, IndexEntry, LogEntry, SnapshottedEntry},
    ClusterConfig, CompactionErr,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    error::Error,
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

    #[cfg(feature = "unicache")]
    /// The encoded type of some data. If there is a cache hit in UniCache, the data will be replaced and get sent over the network as this type instead. E.g., if `u8` then the cached `Entry` (or field of it) will be sent as `u8` instead.
    type Encoded: Encoded;
    #[cfg(feature = "unicache")]
    /// The type representing the encodable parts of an `Entry`. It can be set to `Self` if the whole `Entry` is cachable. See docs of `pre_process()` for an example of deriving `Encodable` from an `Entry`.
    type Encodable: Encodable;
    #[cfg(feature = "unicache")]
    /// The type representing the **NOT** encodable parts of an `Entry`. Any `NotEncodable` data will be transmitted in its original form, without encoding. It can be set to `()` if the whole `Entry` is cachable. See docs of `pre_process()` for an example.
    type NotEncodable: NotEncodable;

    #[cfg(all(feature = "unicache", not(feature = "serde")))]
    /// The type that represents if there was a cache hit or miss in UniCache.
    type EncodeResult: Clone + Debug;

    #[cfg(all(feature = "unicache", feature = "serde"))]
    /// The type that represents the results of trying to encode i.e., if there was a cache hit or miss in UniCache.
    type EncodeResult: Clone + Debug + Serialize + for<'a> Deserialize<'a>;

    #[cfg(all(feature = "unicache", not(feature = "serde")))]
    /// The type that represents the results of trying to encode i.e., if there was a cache hit or miss in UniCache.
    type UniCache: UniCache<T = Self>;
    #[cfg(all(feature = "unicache", feature = "serde"))]
    /// The unicache type for caching popular/re-occurring fields of an entry.
    type UniCache: UniCache<T = Self> + Serialize + for<'a> Deserialize<'a>;
}

/// A StopSign entry that marks the end of a configuration. Used for reconfiguration.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StopSign {
    /// The new `Omnipaxos` cluster configuration
    pub next_config: ClusterConfig,
    /// Metadata for the reconfiguration.
    pub metadata: Option<Vec<u8>>,
}

impl StopSign {
    /// Creates a [`StopSign`].
    pub fn with(next_config: ClusterConfig, metadata: Option<Vec<u8>>) -> Self {
        StopSign {
            next_config,
            metadata,
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

    //fn size_hint() -> usize;  // TODO: To let the system know trade-off of using entries vs snapshot?
}

/// The Result type returned by the storage API.
pub type StorageResult<T> = Result<T, Box<dyn Error>>;

/// TODO: docs
#[derive(Debug)]
pub enum StorageOp<T: Entry> {
    ///
    AppendEntry(T),
    ///
    AppendEntries(Vec<T>),
    ///
    AppendOnPrefix(usize, Vec<T>),
    ///
    SetPromise(Ballot),
    ///
    SetDecidedIndex(usize),
    ///
    SetAcceptedRound(Ballot),
    ///
    SetCompactedIdx(usize),
    ///
    Trim(usize),
    ///
    SetStopsign(Option<StopSign>),
    ///
    SetSnapshot(Option<T::Snapshot>),
}

/// Trait for implementing the storage backend of Sequence Paxos.
pub trait Storage<T>
where
    T: Entry,
{
    /// Atomically perform and persist all write operations in order.
    /// Returns an Error if the transaction was not committed (so it was rolled back).
    fn write_batch(&mut self, batch: Vec<StorageOp<T>>) -> StorageResult<()>;

    /// Appends an entry to the end of the log and returns the log length.
    fn append_entry(&mut self, entry: T) -> StorageResult<()>;

    /// Appends the entries of `entries` to the end of the log and returns the log length.
    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()>;

    /// Appends the entries of `entries` to the prefix from index `from_index` in the log and returns the log length.
    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()>;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()>;

    /// Sets the decided index in the log.
    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()>;

    /// Returns the decided index in the log.
    fn get_decided_idx(&self) -> StorageResult<usize>;

    /// Sets the latest accepted round.
    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()>;

    /// Returns the latest round in which entries have been accepted, returns `None` if no
    /// entries have been accepted.
    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>>;

    /// Returns the entries in the log in the index interval of [from, to).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>>;

    /// Returns the current length of the log.
    fn get_log_len(&self) -> StorageResult<usize>;

    /// Returns the suffix of entries in the log from index `from`.
    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>>;

    /// Returns the round that has been promised.
    fn get_promise(&self) -> StorageResult<Option<Ballot>>;

    /// Sets the StopSign used for reconfiguration.
    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()>;

    /// Returns the stored StopSign, returns `None` if no StopSign has been stored.
    fn get_stopsign(&self) -> StorageResult<Option<StopSign>>;

    /// Removes elements up to the given [`idx`] from storage.
    fn trim(&mut self, idx: usize) -> StorageResult<()>;

    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()>;

    /// Returns the garbage collector index from storage.
    fn get_compacted_idx(&self) -> StorageResult<usize>;

    /// Sets the snapshot.
    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()>;

    /// Returns the stored snapshot.
    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>>;
}

/// A place holder type for when not using snapshots. You should not use this type, it is only internally when deriving the Entry implementation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

/// Used to perform convenient rollbacks of storage operations on internal storage.
/// Represents only values that can and will actually be rolled back from outside internal storage.
pub(crate) enum RollbackValue<T: Entry> {
    DecidedIdx(usize),
    AcceptedRound(Ballot),
    Log(Vec<T>),
    /// compacted index and snapshot
    Snapshot(usize, Option<T::Snapshot>),
}

/// A simple in-memory storage for simple state values of OmniPaxos.
struct StateCache<T>
where
    T: Entry,
{
    #[cfg(feature = "unicache")]
    /// Id of this node
    pid: NodeId,
    /// The maximum number of entries to batch.
    batch_size: usize,
    /// Vector which contains all the logged entries in-memory.
    batched_entries: Vec<T>,
    /// TODO: docs
    num_batched_entries: usize,
    /// Last promised round.
    promise: Ballot,
    /// Last accepted round.
    accepted_round: Ballot,
    /// Length of the decided log.
    decided_idx: usize,
    /// Length of the accepted log.
    accepted_idx: usize,
    /// Garbage collected index.
    compacted_idx: usize,
    /// Stopsign entry.
    stopsign: Option<StopSign>,
    #[cfg(feature = "unicache")]
    /// Batch of entries that are processed (i.e., maybe encoded). Only used by the leader.
    batched_processed_by_leader: Vec<T::EncodeResult>,
    #[cfg(feature = "unicache")]
    unicache: T::UniCache,
}

impl<T> StateCache<T>
where
    T: Entry,
{
    pub fn new(config: InternalStorageConfig, #[cfg(feature = "unicache")] pid: NodeId) -> Self {
        StateCache {
            #[cfg(feature = "unicache")]
            pid,
            batch_size: config.batch_size,
            batched_entries: Vec::with_capacity(config.batch_size),
            num_batched_entries: 0,
            promise: Ballot::default(),
            accepted_round: Ballot::default(),
            decided_idx: 0,
            accepted_idx: 0,
            compacted_idx: 0,
            stopsign: None,
            #[cfg(feature = "unicache")]
            batched_processed_by_leader: Vec::with_capacity(config.batch_size),
            #[cfg(feature = "unicache")]
            unicache: T::UniCache::new(),
        }
    }

    // Returns whether a stopsign is decided
    fn stopsign_is_decided(&self) -> bool {
        self.stopsign.is_some() && self.decided_idx == self.accepted_idx
    }
}

pub(crate) struct InternalStorageConfig {
    pub(crate) batch_size: usize,
}

/// Internal representation of storage. Hides all complexities with the compacted index
/// such that Sequence Paxos accesses the log with the uncompacted index.
pub(crate) struct InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    storage: I,
    // TODO: make private
    pub write_batch: Vec<StorageOp<T>>,
    state_cache: StateCache<T>,
    _t: PhantomData<T>,
}

impl<I, T> InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    pub(crate) fn with(
        storage: I,
        config: InternalStorageConfig,
        #[cfg(feature = "unicache")] pid: NodeId,
    ) -> Self {
        let mut internal_store = InternalStorage {
            storage,
            write_batch: vec![],
            state_cache: StateCache::new(
                config,
                #[cfg(feature = "unicache")]
                pid,
            ),
            _t: Default::default(),
        };
        internal_store.load_cache();
        internal_store
    }

    pub(crate) fn commit_write_batch(&mut self) -> StorageResult<Option<usize>> {
        // TODO: do we need to roll back state cache if batch fails?
        // Update state cache
        let old_accepted_idx = self.state_cache.accepted_idx;
        for op in self.write_batch.iter() {
            match op {
                StorageOp::AppendEntry(_) => self.state_cache.accepted_idx += 1,
                StorageOp::AppendEntries(e) => self.state_cache.accepted_idx += e.len(),
                StorageOp::AppendOnPrefix(from_idx, entries) => {
                    self.state_cache.stopsign = None;
                    self.state_cache.accepted_idx = from_idx + entries.len();
                }
                StorageOp::SetPromise(bal) => self.state_cache.promise = *bal,
                StorageOp::SetDecidedIndex(idx) => self.state_cache.decided_idx = *idx,
                StorageOp::SetAcceptedRound(bal) => self.state_cache.accepted_round = *bal,
                StorageOp::SetCompactedIdx(idx) => self.state_cache.compacted_idx = *idx,
                StorageOp::Trim(_) => (),
                StorageOp::SetStopsign(ss) => {
                    if ss.is_some() && self.state_cache.stopsign.is_none() {
                        self.state_cache.accepted_idx += 1;
                    } else if ss.is_none() && self.state_cache.stopsign.is_some() {
                        self.state_cache.accepted_idx -= 1;
                    }
                    self.state_cache.stopsign = ss.clone();
                }
                StorageOp::SetSnapshot(_) => (),
            }
        }
        // Commit writes to storage
        self.storage
            .write_batch(std::mem::take(&mut self.write_batch))?;
        self.state_cache.num_batched_entries = 0;
        if old_accepted_idx == self.state_cache.accepted_idx {
            Ok(None)
        } else {
            Ok(Some(self.state_cache.accepted_idx))
        }
    }

    pub(crate) fn get_cached_entries(&self) -> Vec<T> {
        self.write_batch.iter().fold(Vec::new(), |mut acc, op| {
            match op {
                StorageOp::AppendEntry(e) => acc.push(e.clone()),
                StorageOp::AppendEntries(e) => acc.extend_from_slice(e),
                StorageOp::AppendOnPrefix(_, e) => acc.extend_from_slice(e),
                _ => (),
            }
            acc
        })

        // self.write_batch.iter().flat_map(|op| {
        //     match op {
        //         StorageOp::AppendEntry(e) => core::slice::from_ref(e),
        //         StorageOp::AppendEntries(e) => {
        //             e
        //         },
        //         _ => &[],
        //     }
        // }).collect()
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn get_cached_entries_encoded(&mut self) -> Vec<T::EncodeResult> {
        let mut encoded = vec![];
        for op in self.write_batch.iter() {
            match op {
                StorageOp::AppendEntry(e) => encoded.push(self.state_cache.unicache.try_encode(e)),
                StorageOp::AppendEntries(entries) => encoded.extend(
                    entries
                        .iter()
                        .map(|e| self.state_cache.unicache.try_encode(e)),
                ),
                StorageOp::AppendOnPrefix(_, entries) => encoded.extend(
                    entries
                        .iter()
                        .map(|e| self.state_cache.unicache.try_encode(e)),
                ),
                _ => (),
            }
        }
        encoded
    }

    fn get_entry_type(
        &self,
        idx: usize,
        compacted_idx: usize,
        accepted_idx: usize,
    ) -> StorageResult<Option<IndexEntry>> {
        if idx < compacted_idx {
            Ok(Some(IndexEntry::Compacted))
        } else if idx < accepted_idx - 1 {
            Ok(Some(IndexEntry::Entry))
        } else if idx == accepted_idx - 1 {
            match self.get_stopsign() {
                Some(ss) => Ok(Some(IndexEntry::StopSign(ss))),
                _ => Ok(Some(IndexEntry::Entry)),
            }
        } else {
            Ok(None)
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub(crate) fn read<R>(&self, r: R) -> StorageResult<Option<Vec<LogEntry<T>>>>
    where
        R: RangeBounds<usize>,
    {
        let from_idx = match r.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(e) => *e + 1,
            Bound::Unbounded => 0,
        };
        let to_idx = match r.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => self.get_accepted_idx(),
        };
        if to_idx == 0 {
            return Ok(None);
        }
        let compacted_idx = self.get_compacted_idx();
        let accepted_idx = self.get_accepted_idx();
        // use to_idx-1 when getting the entry type as to_idx is exclusive
        let to_type = match self.get_entry_type(to_idx - 1, compacted_idx, accepted_idx)? {
            Some(IndexEntry::Compacted) => {
                return Ok(Some(vec![self.create_compacted_entry(compacted_idx)?]))
            }
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        let from_type = match self.get_entry_type(from_idx, compacted_idx, accepted_idx)? {
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        match (from_type, to_type) {
            (IndexEntry::Entry, IndexEntry::Entry) => {
                Ok(Some(self.create_read_log_entries(from_idx, to_idx)?))
            }
            (IndexEntry::Entry, IndexEntry::StopSign(ss)) => {
                let mut entries = self.create_read_log_entries(from_idx, to_idx - 1)?;
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::Entry) => {
                let mut entries = Vec::with_capacity(to_idx - compacted_idx + 1);
                let compacted = self.create_compacted_entry(compacted_idx)?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries(compacted_idx, to_idx)?;
                entries.append(&mut e);
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::StopSign(ss)) => {
                let mut entries = Vec::with_capacity(to_idx - compacted_idx + 1);
                let compacted = self.create_compacted_entry(compacted_idx)?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries(compacted_idx, to_idx - 1)?;
                entries.append(&mut e);
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::StopSign(ss), IndexEntry::StopSign(_)) => {
                Ok(Some(vec![LogEntry::StopSign(
                    ss,
                    self.stopsign_is_decided(),
                )]))
            }
            e => {
                unimplemented!("{}", format!("Unexpected read combination: {:?}", e))
            }
        }
    }

    fn create_read_log_entries(&self, from: usize, to: usize) -> StorageResult<Vec<LogEntry<T>>> {
        let decided_idx = self.get_decided_idx();
        let entries = self
            .get_entries(from, to)?
            .into_iter()
            .enumerate()
            .map(|(idx, e)| {
                let log_idx = idx + from;
                if log_idx < decided_idx {
                    LogEntry::Decided(e)
                } else {
                    LogEntry::Undecided(e)
                }
            })
            .collect();
        Ok(entries)
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub(crate) fn read_decided_suffix(
        &self,
        from_idx: usize,
    ) -> StorageResult<Option<Vec<LogEntry<T>>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx)
        } else {
            Ok(None)
        }
    }

    fn create_compacted_entry(&self, compacted_idx: usize) -> StorageResult<LogEntry<T>> {
        self.storage.get_snapshot().map(|snap| match snap {
            Some(s) => LogEntry::Snapshotted(SnapshottedEntry::with(compacted_idx, s)),
            None => LogEntry::Trimmed(compacted_idx),
        })
    }

    fn load_cache(&mut self) {
        self.state_cache.promise = self
            .storage
            .get_promise()
            .expect("Failed to load cache from storage.")
            .unwrap_or_default();
        self.state_cache.decided_idx = self.storage.get_decided_idx().unwrap();
        self.state_cache.accepted_round = self
            .storage
            .get_accepted_round()
            .unwrap()
            .unwrap_or_default();
        self.state_cache.compacted_idx = self.storage.get_compacted_idx().unwrap();
        self.state_cache.stopsign = self.storage.get_stopsign().unwrap();
        self.state_cache.accepted_idx =
            self.storage.get_log_len().unwrap() + self.state_cache.compacted_idx;
        if self.state_cache.stopsign.is_some() {
            self.state_cache.accepted_idx += 1;
        }
    }

    /*** Writing ***/
    #[cfg(feature = "unicache")]
    pub(crate) fn get_unicache(&self) -> T::UniCache {
        self.state_cache.unicache.clone()
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn set_unicache(&mut self, unicache: T::UniCache) {
        self.state_cache.unicache = unicache;
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn decode_entries(
        &mut self,
        encoded_entries: Vec<<T as Entry>::EncodeResult>,
    ) -> Vec<T> {
        encoded_entries
            .into_iter()
            .map(|x| self.state_cache.unicache.decode(x))
            .collect()
    }

    pub(crate) fn batch_append_entry(&mut self, entry: T) -> bool {
        self.state_cache.num_batched_entries += 1;
        self.write_batch.push(StorageOp::AppendEntry(entry));
        self.state_cache.num_batched_entries >= self.state_cache.batch_size
    }

    pub(crate) fn batch_append_entries(&mut self, entries: Vec<T>) -> bool {
        self.state_cache.num_batched_entries += entries.len();
        self.write_batch.push(StorageOp::AppendEntries(entries));
        self.state_cache.num_batched_entries >= self.state_cache.batch_size
    }

    pub(crate) fn batch_append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> bool {
        self.state_cache.num_batched_entries += entries.len();
        self.write_batch
            .push(StorageOp::AppendOnPrefix(from_idx, entries));
        self.state_cache.num_batched_entries >= self.state_cache.batch_size
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.state_cache.promise = n_prom;
        self.storage.set_promise(n_prom)
    }

    pub(crate) fn batch_set_promise(&mut self, promise: Ballot) {
        self.write_batch.push(StorageOp::SetPromise(promise));
    }

    pub(crate) fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.state_cache.decided_idx = ld;
        self.storage.set_decided_idx(ld)
    }

    pub(crate) fn batch_set_decided_idx(&mut self, idx: usize) {
        self.write_batch.push(StorageOp::SetDecidedIndex(idx));
    }

    pub(crate) fn get_decided_idx(&self) -> usize {
        self.state_cache.decided_idx
    }

    fn get_decided_idx_without_stopsign(&self) -> usize {
        match self.stopsign_is_decided() {
            true => self.get_decided_idx() - 1,
            false => self.get_decided_idx(),
        }
    }

    pub(crate) fn batch_set_accepted_round(&mut self, bal: Ballot) {
        self.write_batch.push(StorageOp::SetAcceptedRound(bal));
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.state_cache.accepted_round
    }

    pub(crate) fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        self.storage.get_entries(from, to)
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_accepted_idx(&self) -> usize {
        self.state_cache.accepted_idx
    }

    pub(crate) fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.storage.get_suffix(from)
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.state_cache.promise
    }

    pub(crate) fn batch_set_stopsign(&mut self, ss: Option<StopSign>) {
        self.write_batch.push(StorageOp::SetStopsign(ss));
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSign> {
        self.state_cache.stopsign.clone()
    }

    // Returns whether a stopsign is decided
    pub(crate) fn stopsign_is_decided(&self) -> bool {
        self.state_cache.stopsign_is_decided()
    }

    pub(crate) fn create_snapshot(&mut self, compact_idx: usize) -> StorageResult<T::Snapshot> {
        let current_compacted_idx = self.get_compacted_idx();
        if compact_idx < current_compacted_idx {
            Err(CompactionErr::TrimmedIndex(current_compacted_idx))?
        }
        let entries = self
            .storage
            .get_entries(current_compacted_idx, compact_idx)?;
        let delta = T::Snapshot::create(entries.as_slice());
        match self.storage.get_snapshot()? {
            Some(mut s) => {
                s.merge(delta);
                Ok(s)
            }
            None => Ok(delta),
        }
    }

    fn create_decided_snapshot(&mut self) -> StorageResult<T::Snapshot> {
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        self.create_snapshot(log_decided_idx)
    }

    pub(crate) fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        self.storage.get_snapshot()
    }

    // Creates a Delta snapshot of entries from `from_idx` to the end of the decided log and also
    // returns the compacted idx of the created snapshot. If the range of entries contains entries
    // which have already been compacted a valid delta cannot be created, so creates a Complete
    // snapshot of the entire decided log instead.
    pub(crate) fn create_diff_snapshot(
        &mut self,
        from_idx: usize,
    ) -> StorageResult<(Option<SnapshotType<T>>, usize)> {
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let compacted_idx = self.get_compacted_idx();
        let snapshot = if from_idx <= compacted_idx {
            // Some entries in range are compacted, snapshot entire decided log
            if compacted_idx < log_decided_idx {
                Some(SnapshotType::Complete(
                    self.create_snapshot(log_decided_idx)?,
                ))
            } else {
                // Entire decided log already snapshotted
                self.get_snapshot()?.map(|s| SnapshotType::Complete(s))
            }
        } else {
            let diff_entries = self.get_entries(from_idx, log_decided_idx)?;
            Some(SnapshotType::Delta(T::Snapshot::create(
                diff_entries.as_slice(),
            )))
        };
        Ok((snapshot, log_decided_idx))
    }

    /// This operation is atomic, but non-reversible after completion
    pub(crate) fn set_snapshot(&mut self, idx: usize, snapshot: T::Snapshot) -> StorageResult<()> {
        let old_compacted_idx = self.get_compacted_idx();
        let old_snapshot = self.storage.get_snapshot()?;
        if idx > old_compacted_idx {
            self.set_compacted_idx(idx)?;
            if let Err(e) = self.storage.set_snapshot(Some(snapshot)) {
                self.set_compacted_idx(old_compacted_idx)?;
                return Err(e);
            }
            if let Err(e) = self.storage.trim(idx) {
                self.set_compacted_idx(old_compacted_idx)?;
                self.storage.set_snapshot(old_snapshot)?;
                return Err(e);
            }
        }
        Ok(())
    }

    pub(crate) fn batch_set_snapshot(&mut self, idx: usize, snapshot: T::Snapshot) {
        let old_compacted_idx = self.get_compacted_idx();
        if idx > old_compacted_idx {
            self.write_batch.push(StorageOp::Trim(idx));
            self.write_batch.push(StorageOp::SetCompactedIdx(idx));
            self.write_batch
                .push(StorageOp::SetSnapshot(Some(snapshot)));
        }
    }

    pub(crate) fn batch_merge_snapshot(
        &mut self,
        idx: usize,
        delta: T::Snapshot,
    ) -> StorageResult<()> {
        let mut snapshot = if let Some(snap) = self.storage.get_snapshot()? {
            snap
        } else {
            self.create_decided_snapshot()?
        };
        snapshot.merge(delta);
        self.batch_set_snapshot(idx, snapshot);
        Ok(())
    }

    pub(crate) fn try_trim(&mut self, idx: usize) -> StorageResult<()> {
        let compacted_idx = self.get_compacted_idx();
        if idx <= compacted_idx {
            Ok(()) // already trimmed or snapshotted this index.
        } else {
            let decided_idx = self.get_decided_idx();
            if idx <= decided_idx {
                // TODO: Make batch version? Also we don't trim away decided SS correctly
                self.set_compacted_idx(idx)?;
                if let Err(e) = self.storage.trim(idx) {
                    self.set_compacted_idx(compacted_idx)?;
                    Err(e)
                } else {
                    Ok(())
                }
            } else {
                Err(CompactionErr::UndecidedIndex(decided_idx))?
            }
        }
    }

    pub(crate) fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.state_cache.compacted_idx = idx;
        self.storage.set_compacted_idx(idx)
    }

    pub(crate) fn batch_set_compacted_idx(&mut self, idx: usize) {
        self.write_batch.push(StorageOp::SetCompactedIdx(idx));
    }

    pub(crate) fn get_compacted_idx(&self) -> usize {
        self.state_cache.compacted_idx
    }

    pub(crate) fn try_snapshot(&mut self, snapshot_idx: Option<usize>) -> StorageResult<()> {
        let decided_idx = self.get_decided_idx();
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let idx = match snapshot_idx {
            Some(i) => match i.cmp(&decided_idx) {
                Ordering::Less => i,
                Ordering::Equal => log_decided_idx,
                Ordering::Greater => Err(CompactionErr::UndecidedIndex(decided_idx))?,
            },
            None => log_decided_idx,
        };
        if idx > self.get_compacted_idx() {
            let snapshot = self.create_snapshot(idx)?;
            self.set_snapshot(idx, snapshot)?;
        }
        Ok(())
    }
}
