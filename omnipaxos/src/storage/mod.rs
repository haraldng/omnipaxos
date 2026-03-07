pub(crate) mod internal_storage;
mod state_cache;

use super::ballot_leader_election::Ballot;
#[cfg(feature = "unicache")]
use crate::unicache::*;
use crate::ClusterConfig;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

#[derive(Debug)]
struct UnsupportedStorageOp {
    name: &'static str,
}

impl Display for UnsupportedStorageOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage operation not supported: {}", self.name)
    }
}

impl Error for UnsupportedStorageOp {}

fn unsupported<T>(name: &'static str) -> StorageResult<T> {
    Err(Box::new(UnsupportedStorageOp { name }))
}

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

/// The write operations of the storge implementation.
#[derive(Debug)]
pub enum StorageOp<T: Entry> {
    /// Appends an entry to the end of the log.
    AppendEntry(T),
    /// Appends entries to the end of the log.
    AppendEntries(Vec<T>),
    /// Appends entries to the log from the prefix specified by the given index.
    AppendOnPrefix(usize, Vec<T>),
    /// Sets the round that has been promised.
    SetPromise(Ballot),
    /// Sets the decided index in the log.
    SetDecidedIndex(usize),
    /// Sets the latest accepted round.
    SetAcceptedRound(Ballot),
    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    SetCompactedIdx(usize),
    /// Removes elements up to the given idx from storage.
    Trim(usize),
    /// Sets the StopSign used for reconfiguration.
    SetStopsign(Option<StopSign>),
    /// Sets the snapshot.
    SetSnapshot(Option<T::Snapshot>),
}

/// Trait for implementing the storage backend of Sequence Paxos.
pub trait Storage<T>
where
    T: Entry,
{
    /// **Atomically** perform all storage operations in order.
    /// For correctness, the operations must be atomic i.e., either all operations are performed
    /// successfully or all get rolled back. If the `StorageResult` returns as `Err`, the
    /// operations are assumed to have been rolled back to the previous state before this function
    /// call.
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()>;

    /// Appends an entry to the end of the log.
    fn append_entry(&mut self, entry: T) -> StorageResult<()>;

    /// Appends the entries of `entries` to the end of the log.
    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()>;

    /// Appends the entries of `entries` to the prefix from index `from_index` (inclusive) in the log.
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

    /// Returns the current length of the log (without the trimmed/snapshotted entries).
    fn get_log_len(&self) -> StorageResult<usize>;

    /// Returns the suffix of entries in the log from index `from` (inclusive).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
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

    // /*** Synced log (SynLog) ***/
    // /// Appends an entry to the end of the synced log.
    // fn synlog_append_entry(&mut self, _entry: T) -> StorageResult<()> {
    //     unsupported("synlog_append_entry")
    // }

    // /// Appends entries to the end of the synced log.
    // fn synlog_append_entries(&mut self, _entries: Vec<T>) -> StorageResult<()> {
    //     unsupported("synlog_append_entries")
    // }

    // /// Appends entries to the synced log from the prefix specified by the given index.
    // fn synlog_append_on_prefix(&mut self, _from_idx: usize, _entries: Vec<T>) -> StorageResult<()> {
    //     unsupported("synlog_append_on_prefix")
    // }

    // /// Returns the entries in the synced log in the index interval of [from, to).
    // fn synlog_get_entries(&self, _from: usize, _to: usize) -> StorageResult<Vec<T>> {
    //     unsupported("synlog_get_entries")
    // }

    // /// Returns the current length of the synced log (without the trimmed/snapshotted entries).
    // fn synlog_get_log_len(&self) -> StorageResult<usize> {
    //     unsupported("synlog_get_log_len")
    // }

    // /// Returns the suffix of entries in the synced log from index `from` (inclusive).
    // fn synlog_get_suffix(&self, _from: usize) -> StorageResult<Vec<T>> {
    //     unsupported("synlog_get_suffix")
    // }

    // /// Removes elements up to the given [`idx`] from the synced log.
    // fn synlog_trim(&mut self, _idx: usize) -> StorageResult<()> {
    //     unsupported("synlog_trim")
    // }

    // /// Sets the compacted (i.e. trimmed or snapshotted) index for the synced log.
    // fn synlog_set_compacted_idx(&mut self, _idx: usize) -> StorageResult<()> {
    //     unsupported("synlog_set_compacted_idx")
    // }

    // /// Returns the compacted index for the synced log.
    // fn synlog_get_compacted_idx(&self) -> StorageResult<usize> {
    //     unsupported("synlog_get_compacted_idx")
    // }

    /*** Unsynced log (UnsynLog) ***/
    /// Inserts or updates an unsynced entry with the given index.
    fn unsynlog_put(&mut self, _idx: usize, _entry: T) -> StorageResult<()> {
        unsupported("unsynlog_put")
    }

    /// Gets an unsynced entry by index.
    fn unsynlog_get(&self, _idx: usize) -> StorageResult<Option<T>> {
        unsupported("unsynlog_get")
    }

    /// Removes an unsynced entry by index.
    fn unsynlog_remove(&mut self, _idx: usize) -> StorageResult<()> {
        unsupported("unsynlog_remove")
    }

    /// Clears all unsynced entries.
    fn unsynlog_clear(&mut self) -> StorageResult<()> {
        unsupported("unsynlog_clear")
    }
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
