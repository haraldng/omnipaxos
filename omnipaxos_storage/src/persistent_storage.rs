use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageResult},
};
use serde::{Deserialize, Serialize};
use rocksdb::{Options, DB, ColumnFamilyDescriptor, WriteBatchWithTransaction, ColumnFamilyRef};
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
pub struct PersistentStorageConfig {
    path: String,
    rocksdb_options: Options,
    log_options: Options,
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

    /// Creates a configuration for `PersistentStorage` with the given path and options for Commitlog and sled
    pub fn with(path: String, log_options: Options, rocksdb_options: Options) -> Self {
        Self {
            path: path,
            log_options,
            rocksdb_options,
        }
    }

    /// Creates a configuration for `PersistentStorage` with the given path and default configs
    pub fn with_path(path: String) -> Self {
        let mut config = Self::default();
        config.path = path;
        config
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
    /// The index of the next log entry to be appended
    next_log_key: usize,
    /// A placeholder for the T: Entry
    t: PhantomData<T>,
}

impl<T: Entry> PersistentStorage<T> {
    /// Creates or opens an existing storage
    pub fn open(storage_config: PersistentStorageConfig) -> Self {
        // Create database with log column
        let path = storage_config.path;
        let log_cf = ColumnFamilyDescriptor::new(LOG, storage_config.log_options);
        let db_opts = storage_config.rocksdb_options;
        let db = rocksdb::DB::open_cf_descriptors(&db_opts, path, vec![log_cf]).expect("Failed to create RocksDB");
        let log_handle = db.cf_handle(LOG).expect("Failed to create RocksDB log column family");

        // Create next log key from the current max log key
        let mut log_iter = db.raw_iterator_cf(log_handle);
        log_iter.seek_to_last();
        let next_log_key = if log_iter.valid(){
            let key = log_iter.key().unwrap();
            assert_eq!(key.len(), 8, "Couldn't recover storage: Log key has unexpected format.");
            usize::from_be_bytes([key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7]]) + 1
        } else {
            match db.get(TRIM).expect("Couldn't recover storage: Reading compacted_idx failed.") {
                Some(bytes) => usize::read_from(bytes.as_bytes()).expect("Couldn't recover storage: Commpacted index has unexpected format."),
                None => 0,
            }
        };
        drop(log_iter);

        Self {
            db,
            next_log_key,
            t: PhantomData,
        }
    }

    /// Creates a new storage instance, panics if a RocksDB instance already exists in the given path
    pub fn new(storage_config: PersistentStorageConfig) -> Self {
        std::fs::metadata(format!("{}", storage_config.path)).expect_err(&format!(
                "Cannot create new instance, database already exists in {}",
                storage_config.path
                ));
        Self::open(storage_config)
    }

    /// Get handle to the log column family of the database
    fn get_log_handle(&self) -> ColumnFamilyRef {
        self.db.cf_handle(LOG).expect("Failed to create RocksDB log column family")
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
    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        let entry_bytes = bincode::serialize(&entry)?;
        self.db.put_cf(self.get_log_handle(), self.next_log_key.to_be_bytes(), entry_bytes)?;
        self.next_log_key += 1;
        Ok(())
    }

    // TODO: if this fails self.log_len will be incorrect
    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let mut batch = WriteBatchWithTransaction::<false>::default();
        for entry in entries {
            batch.put_cf(self.get_log_handle(), self.next_log_key.to_be_bytes(), bincode::serialize(&entry)?);
            self.next_log_key += 1;
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        if from_idx < self.next_log_key {
            let from_key = from_idx.to_be_bytes();
            let to_key = self.next_log_key.to_be_bytes();
            self.db.delete_range_cf(self.get_log_handle(), from_key, to_key)?;
            self.next_log_key = from_idx;
        }
        self.append_entries(entries)
    }

    fn get_entries(&self, mut from: usize, to: usize) -> StorageResult<Vec<T>> {
        // Check if the log has entries up to the requested endpoint.
        if to > self.next_log_key || from >= to {
            return Ok(vec![]); // Do an early return
        }

        let mut iter = self.db.raw_iterator_cf(self.get_log_handle());
        let mut entries = Vec::with_capacity(to - from);
        iter.seek(from.to_be_bytes());
        loop {
            let entry_bytes = iter.value().ok_or(ErrHelper {})?;
            entries.push(bincode::deserialize(entry_bytes)?);
            from += 1;
            if from == to {
                break;
            }
            iter.next();
        }
        Ok(entries)
    }

    // TODO: What should this return? Maybe remove real_log_len from internal_storage
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
        let prom_bytes = bincode::serialize(&n_prom)?;
        self.db.put(NPROM, prom_bytes)?;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        let decided = self.db.get_pinned(DECIDE)?;
        match decided {
            Some(ld_bytes) => Ok(usize::read_from(ld_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        let ld_bytes = usize::as_bytes(&ld);
        self.db.put(DECIDE, ld_bytes)?;
        Ok(())
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
        let acc_bytes = bincode::serialize(&na)?;
        self.db.put(ACC, acc_bytes)?;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        let trim = self.db.get(TRIM)?;
        match trim {
            Some(trim_bytes) => Ok(usize::read_from(trim_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_compacted_idx(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let trim_bytes = usize::as_bytes(&trimmed_idx);
        self.db.put(TRIM, trim_bytes)?;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        let stopsign = self.db.get_pinned(STOPSIGN)?;
        match stopsign {
            Some(ss_bytes) => Ok(bincode::deserialize(&ss_bytes)?),
            None => Ok(None),
        }
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        let stopsign = bincode::serialize(&s)?;
        self.db.put(STOPSIGN, stopsign)?;
        Ok(())
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
        let stopsign = bincode::serialize(&snapshot)?;
        self.db.put(SNAPSHOT, stopsign)?;
        Ok(())
    }

    // TODO: if compact_idx isn't set atomically with this then self.next_log_key could get out of
    // sync on recovery
    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let from_key = 0_usize.to_be_bytes();
        let to_key = trimmed_idx.to_be_bytes();
        self.db.delete_range_cf(self.get_log_handle(), from_key, to_key)?;
        Ok(())
    }
}
