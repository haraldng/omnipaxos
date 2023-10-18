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

    //fn size_hint() -> u64;  // TODO: To let the system know trade-off of using entries vs snapshot?
}

/// The Result type returned by the storage API.
pub type StorageResult<T> = Result<T, Box<dyn Error>>;

/// Trait for implementing the storage backend of Sequence Paxos.
pub trait Storage<T>
where
    T: Entry,
{
    /// Appends an entry to the end of the log and returns the log length.
    fn append_entry(&mut self, entry: T) -> StorageResult<u64>;

    /// Appends the entries of `entries` to the end of the log and returns the log length.
    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64>;

    /// Appends the entries of `entries` to the prefix from index `from_index` in the log and returns the log length.
    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64>;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()>;

    /// Sets the decided index in the log.
    fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()>;

    /// Returns the decided index in the log.
    fn get_decided_idx(&self) -> StorageResult<u64>;

    /// Sets the latest accepted round.
    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()>;

    /// Returns the latest round in which entries have been accepted, returns `None` if no
    /// entries have been accepted.
    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>>;

    /// Returns the entries in the log in the index interval of [from, to).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>>;

    /// Returns the current length of the log.
    fn get_log_len(&self) -> StorageResult<u64>;

    /// Returns the suffix of entries in the log from index `from`.
    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>>;

    /// Returns the round that has been promised.
    fn get_promise(&self) -> StorageResult<Option<Ballot>>;

    /// Sets the StopSign used for reconfiguration.
    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()>;

    /// Returns the stored StopSign, returns `None` if no StopSign has been stored.
    fn get_stopsign(&self) -> StorageResult<Option<StopSign>>;

    /// Removes elements up to the given [`idx`] from storage.
    fn trim(&mut self, idx: u64) -> StorageResult<()>;

    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    fn set_compacted_idx(&mut self, idx: u64) -> StorageResult<()>;

    /// Returns the garbage collector index from storage.
    fn get_compacted_idx(&self) -> StorageResult<u64>;

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
    DecidedIdx(u64),
    AcceptedRound(Ballot),
    Log(Vec<T>),
    /// compacted index and snapshot
    Snapshot(u64, Option<T::Snapshot>),
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
    /// Last promised round.
    promise: Ballot,
    /// Last accepted round.
    accepted_round: Ballot,
    /// Length of the decided log.
    decided_idx: u64,
    /// Garbage collected index.
    compacted_idx: u64,
    /// Real length of logs in the storage.
    real_log_len: u64,
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
            promise: Ballot::default(),
            accepted_round: Ballot::default(),
            decided_idx: 0,
            compacted_idx: 0,
            real_log_len: 0,
            stopsign: None,
            #[cfg(feature = "unicache")]
            batched_processed_by_leader: Vec::with_capacity(config.batch_size),
            #[cfg(feature = "unicache")]
            unicache: T::UniCache::new(),
        }
    }

    // Returns the index of the last accepted entry.
    fn get_accepted_idx(&self) -> u64 {
        let log_len = self.compacted_idx + self.real_log_len;
        if self.stopsign.is_some() {
            log_len + 1
        } else {
            log_len
        }
    }

    // Returns whether a stopsign is decided
    fn stopsign_is_decided(&self) -> bool {
        self.stopsign.is_some() && self.decided_idx == self.get_accepted_idx()
    }

    // Appends an entry to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    fn append_entry(&mut self, entry: T) -> Option<Vec<T>> {
        #[cfg(feature = "unicache")]
        {
            let processed = self.unicache.try_encode(&entry);
            self.batched_processed_by_leader.push(processed);
        }
        self.batched_entries.push(entry);
        self.take_entries_if_batch_is_full()
    }

    // Appends entries to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    fn append_entries(&mut self, entries: Vec<T>) -> Option<Vec<T>> {
        #[cfg(feature = "unicache")]
        {
            if self.promise.pid == self.pid {
                // only try encoding if we're the leader
                for entry in &entries {
                    let processed = self.unicache.try_encode(entry);
                    self.batched_processed_by_leader.push(processed);
                }
            }
        }
        self.batched_entries.extend(entries);
        self.take_entries_if_batch_is_full()
    }

    // Return batched entries if the batch is full that need to be flushed in to storage.
    fn take_entries_if_batch_is_full(&mut self) -> Option<Vec<T>> {
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

    #[cfg(feature = "unicache")]
    fn take_batched_processed(&mut self) -> Vec<T::EncodeResult> {
        std::mem::take(&mut self.batched_processed_by_leader)
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

    /// Writes the value.
    pub(crate) fn single_rollback(&mut self, value: RollbackValue<T>) {
        match value {
            RollbackValue::DecidedIdx(idx) => self
                .set_decided_idx(idx)
                .expect("storage error while trying to write decided_idx"),
            RollbackValue::AcceptedRound(b) => self
                .set_accepted_round(b)
                .expect("storage error while trying to write accepted_round"),
            RollbackValue::Log(entries) => {
                self.rollback_log(entries);
            }
            RollbackValue::Snapshot(compacted_idx, snapshot) => {
                self.rollback_snapshot(compacted_idx, snapshot);
            }
        }
    }

    /// Writes the values.
    pub(crate) fn rollback(&mut self, values: Vec<RollbackValue<T>>) {
        for value in values {
            self.single_rollback(value);
        }
    }

    pub(crate) fn rollback_and_panic(&mut self, values: Vec<RollbackValue<T>>, msg: &str) {
        for value in values {
            self.single_rollback(value);
        }
        panic!("{}", msg);
    }

    /// This function is useful to handle `StorageResult::Error`.
    /// If `result` is an error, this function tries to write the `values` and then panics with `msg`.
    /// Otherwise it returns.
    pub(crate) fn rollback_and_panic_if_err<R>(
        &mut self,
        result: &StorageResult<R>,
        values: Vec<RollbackValue<T>>,
        msg: &str,
    ) where
        R: Debug,
    {
        if result.is_err() {
            self.rollback(values);
            panic!("{}: {}", msg, result.as_ref().unwrap_err());
        }
    }

    /// Rollback the log in the storage using given log entries.
    fn rollback_log(&mut self, entries: Vec<T>) {
        self.try_trim(self.get_accepted_idx())
            .expect("storage error while trying to trim log entries before rolling back");
        self.append_entries_without_batching(entries)
            .expect("storage error while trying to rollback log entries");
    }

    /// Rollback the snapshot in the storage using given compacted_idx and snapshot.
    fn rollback_snapshot(&mut self, compacted_idx: u64, snapshot: Option<T::Snapshot>) {
        if let Some(old_snapshot) = snapshot {
            self.set_snapshot(compacted_idx, old_snapshot)
                .expect("storage error while trying to rollback snapshot");
        } else {
            self.set_compacted_idx(compacted_idx)
                .expect("storage error while trying to rollback compacted index");
            self.reset_snapshot()
                .expect("storage error while trying to reset snapshot");
        }
    }

    fn get_entry_type(
        &self,
        idx: u64,
        compacted_idx: u64,
        virtual_log_len: u64,
    ) -> StorageResult<Option<IndexEntry>> {
        if idx < compacted_idx {
            Ok(Some(IndexEntry::Compacted))
        } else if idx < virtual_log_len {
            Ok(Some(IndexEntry::Entry))
        } else if idx == virtual_log_len {
            match self.get_stopsign() {
                Some(ss) => Ok(Some(IndexEntry::StopSign(ss))),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub(crate) fn read<R>(&self, r: R) -> StorageResult<Option<Vec<LogEntry<T>>>>
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
            Bound::Unbounded => self.get_accepted_idx(),
        };
        if to_idx == 0 {
            return Ok(None);
        }
        let compacted_idx = self.get_compacted_idx();
        let log_len = self.state_cache.real_log_len + self.state_cache.compacted_idx;
        let to_type = match self.get_entry_type(to_idx - 1, compacted_idx, log_len)? {
            // use to_idx-1 when getting the entry type as to_idx is exclusive
            Some(IndexEntry::Compacted) => {
                return Ok(Some(vec![self.create_compacted_entry(compacted_idx)?]))
            }
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        let from_type = match self.get_entry_type(from_idx, compacted_idx, log_len)? {
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        let decided_idx = self.get_decided_idx();
        match (from_type, to_type) {
            (IndexEntry::Entry, IndexEntry::Entry) => {
                let from_suffix_idx = from_idx - compacted_idx;
                let to_suffix_idx = to_idx - compacted_idx;
                Ok(Some(self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                )?))
            }
            (IndexEntry::Entry, IndexEntry::StopSign(ss)) => {
                let from_suffix_idx = from_idx - compacted_idx;
                let to_suffix_idx = to_idx - compacted_idx - 1;
                let mut entries = self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                )?;
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::Entry) => {
                let from_suffix_idx = 0;
                let to_suffix_idx = to_idx - compacted_idx;
                let mut entries = Vec::with_capacity((to_suffix_idx + 1) as usize);
                let compacted = self.create_compacted_entry(compacted_idx)?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                )?;
                entries.append(&mut e);
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::StopSign(ss)) => {
                let from_suffix_idx = 0;
                let to_suffix_idx = to_idx - compacted_idx - 1;
                let mut entries = Vec::with_capacity((to_suffix_idx + 1) as usize);
                let compacted = self.create_compacted_entry(compacted_idx)?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries_with_real_idx(
                    from_suffix_idx,
                    to_suffix_idx,
                    compacted_idx,
                    decided_idx,
                )?;
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

    fn create_read_log_entries_with_real_idx(
        &self,
        from_sfx_idx: u64,
        to_sfx_idx: u64,
        compacted_idx: u64,
        decided_idx: u64,
    ) -> StorageResult<Vec<LogEntry<T>>> {
        let entries = self
            .get_entries_with_real_idx(from_sfx_idx, to_sfx_idx)?
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
            .collect();
        Ok(entries)
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub(crate) fn read_decided_suffix(
        &self,
        from_idx: u64,
    ) -> StorageResult<Option<Vec<LogEntry<T>>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx)
        } else {
            Ok(None)
        }
    }

    fn create_compacted_entry(&self, compacted_idx: u64) -> StorageResult<LogEntry<T>> {
        self.storage.get_snapshot().map(|snap| match snap {
            Some(s) => LogEntry::Snapshotted(SnapshottedEntry::with(compacted_idx, s)),
            None => LogEntry::Trimmed(compacted_idx),
        })
    }

    fn load_cache(&mut self) {
        // try to load from storage
        if let Some(promise) = self
            .storage
            .get_promise()
            .expect("failed to load cache from storage")
        {
            self.state_cache.promise = promise;
            self.state_cache.decided_idx = self.storage.get_decided_idx().unwrap();
            self.state_cache.accepted_round = self
                .storage
                .get_accepted_round()
                .unwrap()
                .unwrap_or_default();
            self.state_cache.compacted_idx = self.storage.get_compacted_idx().unwrap();
            self.state_cache.real_log_len = self.storage.get_log_len().unwrap();
            self.state_cache.stopsign = self.storage.get_stopsign().unwrap();
        }
    }

    /*** Writing ***/
    // Append entry, if the batch size is reached, flush the batch and return the actual
    // accepted index (not including the batched entries)
    pub(crate) fn append_entry_with_batching(
        &mut self,
        entry: T,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entry(entry);
        self.flush_if_full_batch(append_res)
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index and the flushed entries. If the batch size is not reached, return None.
    pub(crate) fn append_entries_with_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entries(entries);
        self.flush_if_full_batch(append_res)
    }

    fn flush_if_full_batch(
        &mut self,
        append_res: Option<Vec<T>>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        if let Some(flushed_entries) = append_res {
            let accepted_idx = self.append_entries_without_batching(flushed_entries.clone())?;
            Ok(Some(AcceptedMetaData {
                accepted_idx,
                #[cfg(not(feature = "unicache"))]
                flushed_entries,
                #[cfg(feature = "unicache")]
                flushed_processed: self.state_cache.take_batched_processed(),
            }))
        } else {
            Ok(None)
        }
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index. If the batch size is not reached, return None.
    pub(crate) fn append_entries_and_get_accepted_idx(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<u64>> {
        let append_res = self.state_cache.append_entries(entries);
        if let Some(flushed_entries) = append_res {
            let accepted_idx = self.append_entries_without_batching(flushed_entries)?;
            Ok(Some(accepted_idx))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn get_unicache(&self) -> T::UniCache {
        self.state_cache.unicache.clone()
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn set_unicache(&mut self, unicache: T::UniCache) {
        self.state_cache.unicache = unicache;
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn append_encoded_entries_and_get_accepted_idx(
        &mut self,
        encoded_entries: Vec<<T as Entry>::EncodeResult>,
    ) -> StorageResult<Option<u64>> {
        let entries = encoded_entries
            .into_iter()
            .map(|x| self.state_cache.unicache.decode(x))
            .collect();
        self.append_entries_and_get_accepted_idx(entries)
    }

    pub(crate) fn flush_batch(&mut self) -> StorageResult<u64> {
        #[cfg(feature = "unicache")]
        {
            // clear the processed batch
            self.state_cache.batched_processed_by_leader.clear();
        }
        let flushed_entries = self.state_cache.take_batched_entries();
        self.append_entries_without_batching(flushed_entries)
    }

    // Append entries without batching, return the accepted index
    pub(crate) fn append_entries_without_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<u64> {
        self.state_cache.real_log_len = self.storage.append_entries(entries)?;
        Ok(self.get_accepted_idx())
    }

    pub(crate) fn append_on_decided_prefix(&mut self, entries: Vec<T>) -> StorageResult<u64> {
        let decided_idx = self.get_decided_idx();
        let compacted_idx = self.get_compacted_idx();
        self.state_cache.real_log_len = self
            .storage
            .append_on_prefix(decided_idx - compacted_idx, entries)?;
        Ok(self.get_accepted_idx())
    }

    pub(crate) fn append_on_prefix(
        &mut self,
        from_idx: u64,
        entries: Vec<T>,
    ) -> StorageResult<u64> {
        let compacted_idx = self.get_compacted_idx();
        self.state_cache.real_log_len = self
            .storage
            .append_on_prefix(from_idx - compacted_idx, entries)?;
        Ok(self.get_accepted_idx())
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.state_cache.promise = n_prom;
        self.storage.set_promise(n_prom)
    }

    pub(crate) fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()> {
        self.state_cache.decided_idx = ld;
        self.storage.set_decided_idx(ld)
    }

    pub(crate) fn get_decided_idx(&self) -> u64 {
        self.state_cache.decided_idx
    }

    fn get_decided_idx_without_stopsign(&self) -> u64 {
        match self.stopsign_is_decided() {
            true => self.get_decided_idx() - 1,
            false => self.get_decided_idx(),
        }
    }

    pub(crate) fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.state_cache.accepted_round = na;
        self.storage.set_accepted_round(na)
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.state_cache.accepted_round
    }

    pub(crate) fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        let compacted_idx = self.get_compacted_idx();
        self.get_entries_with_real_idx(from - compacted_idx.min(from), to - compacted_idx.min(to))
    }

    /// Get entries with real physical log indexes i.e. the index with the compacted offset.
    fn get_entries_with_real_idx(
        &self,
        from_sfx_idx: u64,
        to_sfx_idx: u64,
    ) -> StorageResult<Vec<T>> {
        self.storage.get_entries(from_sfx_idx, to_sfx_idx)
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_accepted_idx(&self) -> u64 {
        self.state_cache.get_accepted_idx()
    }

    pub(crate) fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        let compacted_idx = self.get_compacted_idx();
        self.storage.get_suffix(from - compacted_idx.min(from))
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.state_cache.promise
    }

    pub(crate) fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.state_cache.stopsign = s.clone();
        self.storage.set_stopsign(s)
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSign> {
        self.state_cache.stopsign.clone()
    }

    // Returns whether a stopsign is decided
    pub(crate) fn stopsign_is_decided(&self) -> bool {
        self.state_cache.stopsign_is_decided()
    }

    pub(crate) fn create_snapshot(&mut self, compact_idx: u64) -> StorageResult<T::Snapshot> {
        let compacted_idx = self.get_compacted_idx();
        if compact_idx < compacted_idx {
            Err(CompactionErr::TrimmedIndex(compacted_idx))?
        }
        let entries = self.storage.get_entries(0, compact_idx - compacted_idx)?;
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
        from_idx: u64,
    ) -> StorageResult<(Option<SnapshotType<T>>, u64)> {
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

    pub(crate) fn reset_snapshot(&mut self) -> StorageResult<()> {
        self.storage.set_snapshot(None)
    }

    /// This operation is atomic, but non-reversible after completion
    pub(crate) fn set_snapshot(&mut self, idx: u64, snapshot: T::Snapshot) -> StorageResult<()> {
        let old_compacted_idx = self.get_compacted_idx();
        let old_snapshot = self.storage.get_snapshot()?;
        if idx > old_compacted_idx {
            self.set_compacted_idx(idx)?;
            if let Err(e) = self.storage.set_snapshot(Some(snapshot)) {
                self.set_compacted_idx(old_compacted_idx)?;
                return Err(e);
            }
            let old_log_len = self.state_cache.real_log_len;
            if let Err(e) = self.storage.trim(idx - old_compacted_idx) {
                self.set_compacted_idx(old_compacted_idx)?;
                self.storage.set_snapshot(old_snapshot)?;
                return Err(e);
            }
            self.state_cache.real_log_len =
                old_log_len - (idx - old_compacted_idx).min(old_log_len);
        }
        Ok(())
    }

    pub(crate) fn merge_snapshot(&mut self, idx: u64, delta: T::Snapshot) -> StorageResult<()> {
        let mut snapshot = if let Some(snap) = self.storage.get_snapshot()? {
            snap
        } else {
            self.create_decided_snapshot()?
        };
        snapshot.merge(delta);
        self.set_snapshot(idx, snapshot)
    }

    pub(crate) fn try_trim(&mut self, idx: u64) -> StorageResult<()> {
        let compacted_idx = self.get_compacted_idx();
        if idx <= compacted_idx {
            Ok(()) // already trimmed or snapshotted this index.
        } else {
            let decided_idx = self.get_decided_idx();
            if idx <= decided_idx {
                self.set_compacted_idx(idx)?;
                if let Err(e) = self.storage.trim(idx - compacted_idx) {
                    self.set_compacted_idx(compacted_idx)?;
                    Err(e)
                } else {
                    self.state_cache.real_log_len = self.storage.get_log_len()?;
                    Ok(())
                }
            } else {
                Err(CompactionErr::UndecidedIndex(decided_idx))?
            }
        }
    }

    pub(crate) fn set_compacted_idx(&mut self, idx: u64) -> StorageResult<()> {
        self.state_cache.compacted_idx = idx;
        self.storage.set_compacted_idx(idx)
    }

    pub(crate) fn get_compacted_idx(&self) -> u64 {
        self.state_cache.compacted_idx
    }

    pub(crate) fn try_snapshot(&mut self, snapshot_idx: Option<u64>) -> StorageResult<()> {
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
