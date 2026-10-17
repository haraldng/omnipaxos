use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageOp, StorageResult},
};
use rocksdb::{ColumnFamilyDescriptor, ColumnFamilyRef, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use zerocopy::{AsBytes, FromBytes};

const DEFAULT: &str = "/default_storage/";
const LOG: &str = "log";
const NPROM: &[u8] = b"NPROM";
const ACC: &[u8] = b"ACC";
const DECIDE: &[u8] = b"DECIDE";
const TRIM: &[u8] = b"TRIM";
const STOPSIGN: &[u8] = b"STOPSIGN";
const SNAPSHOT: &[u8] = b"SNAPSHOT";

// Configuration for `PersistentStorage`.
/// # Fields
/// * `path`: Path to the storage directory
/// * `rocksdb_options`: Options for the RocksDB state store
/// * `log_options` : Options for the the RocksDB log store
/// * `sync_writes`: Whether every write is fsynced before returning. Defaults to `true`.
pub struct PersistentStorageConfig {
    path: String,
    rocksdb_options: Options,
    log_options: Options,
    sync_writes: bool,
}

impl PersistentStorageConfig {
    /// Returns the current path to the persistent storage.
    pub fn get_path(&self) -> &String {
        &self.path
    }

    /// Sets the path to the persistent storage.
    pub fn set_path(&mut self, path: String) {
        self.path = path;
    }

    /// Returns the RocksDB options for the log.
    pub fn get_log_options(&self) -> Options {
        self.log_options.clone()
    }

    /// Sets the RocksDB options for the log.
    pub fn set_log_options(&mut self, log_opts: Options) {
        self.log_options = log_opts;
    }

    /// Returns the RocksDB options for the state store.
    pub fn get_database_options(&self) -> Options {
        self.rocksdb_options.clone()
    }

    /// Sets the RocksDB options for the state store.
    pub fn set_database_options(&mut self, opts: Options) {
        self.rocksdb_options = opts;
    }

    /// Returns whether writes are fsynced before returning.
    pub fn get_sync_writes(&self) -> bool {
        self.sync_writes
    }

    /// Sets whether writes are fsynced before returning. Disabling this is faster, but writes
    /// acknowledged shortly before a machine crash or power loss may be lost. OmniPaxos relies on
    /// promises and accepted entries surviving such failures, so a node running with this disabled
    /// can violate consensus safety if its machine crashes.
    pub fn set_sync_writes(&mut self, sync_writes: bool) {
        self.sync_writes = sync_writes;
    }

    /// Creates a configuration for `PersistentStorage` with the given path and options for Commitlog and sled
    pub fn with(path: String, log_options: Options, rocksdb_options: Options) -> Self {
        Self {
            path,
            log_options,
            rocksdb_options,
            sync_writes: true,
        }
    }

    /// Creates a configuration for `PersistentStorage` with the given path and default configs
    pub fn with_path(path: String) -> Self {
        let mut rocksdb_options = Options::default();
        rocksdb_options.create_missing_column_families(true);
        rocksdb_options.create_if_missing(true);
        Self {
            path,
            log_options: Options::default(),
            rocksdb_options,
            sync_writes: true,
        }
    }
}

impl Default for PersistentStorageConfig {
    fn default() -> Self {
        let mut rocksdb_options = Options::default();
        rocksdb_options.create_missing_column_families(true);
        rocksdb_options.create_if_missing(true);
        Self {
            path: DEFAULT.to_string(),
            log_options: Options::default(),
            rocksdb_options,
            sync_writes: true,
        }
    }
}

/// A persistent storage implementation, lets sequence paxos write the log
/// and current state to disk. Log entries are serialized and de-serialized
/// into slice of bytes when read or written from the log.
pub struct PersistentStorage<T>
where
    T: Entry,
{
    /// Local RocksDB key-value store
    db: DB,
    /// Options applied to every write
    write_opts: WriteOptions,
    /// The index of the next log entry to be appended. Will be used as the key of the entry in big
    /// endian format.
    next_log_key: usize,
    /// A placeholder for the T: Entry
    t: PhantomData<T>,
}

impl<T: Entry> PersistentStorage<T>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    /// Creates or opens an existing storage
    pub fn open(storage_config: PersistentStorageConfig) -> Self {
        // Create database with log column
        let path = storage_config.path;
        let log_cf = ColumnFamilyDescriptor::new(LOG, storage_config.log_options);
        let db_opts = storage_config.rocksdb_options;
        let db = rocksdb::DB::open_cf_descriptors(&db_opts, path, vec![log_cf])
            .expect("Failed to create RocksDB");
        let log_handle = db
            .cf_handle(LOG)
            .expect("Failed to create RocksDB log column family");

        // Create next log key from the state of the database
        let mut log_iter = db.raw_iterator_cf(log_handle);
        log_iter.seek_to_last();
        let next_log_key = if log_iter.valid() {
            // There's a max key in the database. Next key is 1 greater.
            let key = log_iter.key().unwrap();
            assert_eq!(
                key.len(),
                8,
                "Couldn't recover storage: Log key has unexpected format."
            );
            usize::from_be_bytes([
                key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
            ]) + 1
        } else {
            // No max key in the database. Either there's no entry yet added or they have been
            // trimmed away.
            match db
                .get(TRIM)
                .expect("Couldn't recover storage: Reading compacted_idx failed.")
            {
                Some(bytes) => usize::read_from(bytes.as_bytes())
                    .expect("Couldn't recover storage: Commpacted index has unexpected format."),
                None => 0,
            }
        };
        drop(log_iter);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(storage_config.sync_writes);
        Self {
            db,
            write_opts,
            next_log_key,
            t: PhantomData,
        }
    }

    /// Creates a new storage instance, panics if a RocksDB instance already exists in the given path
    pub fn new(storage_config: PersistentStorageConfig) -> Self {
        std::fs::metadata(storage_config.path.clone()).expect_err(&format!(
            "Cannot create new instance, database already exists in {}",
            storage_config.path
        ));
        Self::open(storage_config)
    }

    /// Get handle to the log column family of the database
    fn get_log_handle(&self) -> ColumnFamilyRef<'_> {
        self.db
            .cf_handle(LOG)
            .expect("Couldn't find RocksDB log column family")
    }

    /// Adds `op` to `batch`. Advances `next_log_key` for appended entries; the caller must restore
    /// it if the batch is not written.
    fn add_to_batch(&mut self, batch: &mut WriteBatch, op: StorageOp<T>) -> StorageResult<()> {
        match op {
            StorageOp::AppendEntry(entry) => self.batch_append_entries(batch, vec![entry])?,
            StorageOp::AppendEntries(entries) => self.batch_append_entries(batch, entries)?,
            StorageOp::AppendOnPrefix(from_idx, entries) => {
                // Don't need to delete entries that will be overwritten.
                let delete_idx = from_idx + entries.len();
                if delete_idx < self.next_log_key {
                    let from_key = delete_idx.to_be_bytes();
                    let to_key = self.next_log_key.to_be_bytes();
                    batch.delete_range_cf(self.get_log_handle(), from_key, to_key);
                }
                self.next_log_key = from_idx;
                self.batch_append_entries(batch, entries)?
            }
            StorageOp::SetPromise(bal) => batch.put(NPROM, bincode::serialize(&bal)?),
            StorageOp::SetDecidedIndex(idx) => batch.put(DECIDE, usize::as_bytes(&idx)),
            StorageOp::SetAcceptedRound(bal) => batch.put(ACC, bincode::serialize(&bal)?),
            StorageOp::SetCompactedIdx(idx) => batch.put(TRIM, usize::as_bytes(&idx)),
            StorageOp::Trim(idx) => {
                let from_key = 0_usize.to_be_bytes();
                let to_key = idx.to_be_bytes();
                batch.delete_range_cf(self.get_log_handle(), from_key, to_key);
            }
            StorageOp::SetStopsign(ss) => batch.put(STOPSIGN, bincode::serialize(&ss)?),
            StorageOp::SetSnapshot(snap) => batch.put(SNAPSHOT, bincode::serialize(&snap)?),
        }
        Ok(())
    }

    fn batch_append_entries(
        &mut self,
        batch: &mut WriteBatch,
        entries: Vec<T>,
    ) -> StorageResult<()> {
        for entry in entries {
            batch.put_cf(
                self.get_log_handle(),
                self.next_log_key.to_be_bytes(),
                bincode::serialize(&entry)?,
            );
            self.next_log_key += 1;
        }
        Ok(())
    }

    /// Writes all `ops` in a single batch. If any op fails to serialize or the write fails, nothing
    /// is written and `next_log_key` is restored.
    fn write_batch(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        let prev_next_log_key = self.next_log_key;
        let mut batch = WriteBatch::default();
        let res = ops
            .into_iter()
            .try_for_each(|op| self.add_to_batch(&mut batch, op))
            .and_then(|()| Ok(self.db.write_opt(batch, &self.write_opts)?));
        if res.is_err() {
            self.next_log_key = prev_next_log_key;
        }
        res
    }

    fn put(&self, key: &[u8], value: impl AsRef<[u8]>) -> StorageResult<()> {
        Ok(self.db.put_opt(key, value, &self.write_opts)?)
    }
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Copy, Clone, Debug)]
pub struct ErrHelper {}
impl std::error::Error for ErrHelper {}
impl std::fmt::Display for ErrHelper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl<T> Storage<T> for PersistentStorage<T>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        self.write_batch(ops)
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.write_batch(vec![StorageOp::AppendEntry(entry)])
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        self.write_batch(vec![StorageOp::AppendEntries(entries)])
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.write_batch(vec![StorageOp::AppendOnPrefix(from_idx, entries)])
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        // Check if the log has entries up to the requested endpoint.
        if to > self.next_log_key || from >= to {
            return Ok(vec![]); // Do an early return
        }

        let mut iter = self.db.raw_iterator_cf(self.get_log_handle());
        let mut entries = Vec::with_capacity(to - from);
        iter.seek(from.to_be_bytes());
        for _ in from..to {
            let entry_bytes = iter.value().ok_or(ErrHelper {})?;
            entries.push(bincode::deserialize(entry_bytes)?);
            iter.next();
        }
        Ok(entries)
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.next_log_key - self.get_compacted_idx()?)
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.get_entries(from, self.next_log_key)
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        let promise = self.db.get_pinned(NPROM)?;
        match promise {
            Some(pinned_bytes) => Ok(Some(bincode::deserialize(&pinned_bytes)?)),
            None => Ok(None),
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.put(NPROM, bincode::serialize(&n_prom)?)
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        let decided = self.db.get_pinned(DECIDE)?;
        match decided {
            Some(ld_bytes) => Ok(usize::read_from(ld_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.put(DECIDE, usize::as_bytes(&ld))
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        let accepted = self.db.get_pinned(ACC)?;
        match accepted {
            Some(acc_bytes) => {
                let ballot = bincode::deserialize(&acc_bytes)?;
                Ok(Some(ballot))
            }
            None => Ok(None),
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.put(ACC, bincode::serialize(&na)?)
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        let trim = self.db.get(TRIM)?;
        match trim {
            Some(trim_bytes) => Ok(usize::read_from(trim_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_compacted_idx(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        self.put(TRIM, usize::as_bytes(&trimmed_idx))
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        let stopsign = self.db.get_pinned(STOPSIGN)?;
        match stopsign {
            Some(ss_bytes) => Ok(bincode::deserialize(&ss_bytes)?),
            None => Ok(None),
        }
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.put(STOPSIGN, bincode::serialize(&s)?)
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        let snapshot = self.db.get_pinned(SNAPSHOT)?;
        if let Some(snapshot_bytes) = snapshot {
            Ok(bincode::deserialize(snapshot_bytes.as_bytes())?)
        } else {
            Ok(None)
        }
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.put(SNAPSHOT, bincode::serialize(&snapshot)?)
    }

    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        self.write_batch(vec![StorageOp::Trim(trimmed_idx)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnipaxos::macros::Entry;
    use serde::{Deserializer, Serializer};
    use tempfile::TempDir;

    const UNSERIALIZABLE: u64 = u64::MAX;

    /// Fails to serialize when it holds `UNSERIALIZABLE`.
    #[derive(Clone, Debug, PartialEq, Entry)]
    struct TestEntry(u64);

    impl Serialize for TestEntry {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            if self.0 == UNSERIALIZABLE {
                return Err(serde::ser::Error::custom("unserializable entry"));
            }
            s.serialize_u64(self.0)
        }
    }

    impl<'de> Deserialize<'de> for TestEntry {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            u64::deserialize(d).map(TestEntry)
        }
    }

    fn open_storage(dir: &TempDir) -> PersistentStorage<TestEntry> {
        let path = dir.path().join("db").to_str().unwrap().to_string();
        PersistentStorage::open(PersistentStorageConfig::with_path(path))
    }

    fn entries(vals: &[u64]) -> Vec<TestEntry> {
        vals.iter().map(|v| TestEntry(*v)).collect()
    }

    #[test]
    fn sync_writes_enabled_by_default() {
        assert!(PersistentStorageConfig::default().get_sync_writes());
        assert!(PersistentStorageConfig::with_path("p".to_string()).get_sync_writes());
        assert!(PersistentStorageConfig::with(
            "p".to_string(),
            Options::default(),
            Options::default()
        )
        .get_sync_writes());
    }

    #[test]
    fn failed_write_atomically_leaves_no_partial_state() {
        let dir = TempDir::new().unwrap();
        let mut storage = open_storage(&dir);
        storage.append_entries(entries(&[1, 2])).unwrap();

        let failed = storage.write_atomically(vec![
            StorageOp::SetDecidedIndex(1),
            StorageOp::AppendEntries(entries(&[3, UNSERIALIZABLE])),
        ]);
        assert!(failed.is_err());
        assert_eq!(storage.get_decided_idx().unwrap(), 0);
        assert_eq!(storage.get_log_len().unwrap(), 2);

        // A later write must not commit anything left over from the failed one.
        storage
            .write_atomically(vec![StorageOp::AppendEntry(TestEntry(4))])
            .unwrap();
        assert_eq!(storage.get_decided_idx().unwrap(), 0);
        assert_eq!(storage.get_suffix(0).unwrap(), entries(&[1, 2, 4]));

        drop(storage);
        let reopened = open_storage(&dir);
        assert_eq!(reopened.get_decided_idx().unwrap(), 0);
        assert_eq!(reopened.get_suffix(0).unwrap(), entries(&[1, 2, 4]));
    }

    #[test]
    fn failed_append_entries_does_not_advance_log() {
        let dir = TempDir::new().unwrap();
        let mut storage = open_storage(&dir);
        storage.append_entries(entries(&[1])).unwrap();

        assert!(storage
            .append_entries(entries(&[2, UNSERIALIZABLE]))
            .is_err());
        storage.append_entry(TestEntry(3)).unwrap();
        assert_eq!(storage.get_suffix(0).unwrap(), entries(&[1, 3]));
    }
}
