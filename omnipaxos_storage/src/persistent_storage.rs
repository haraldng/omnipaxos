use commitlog::{
    message::{MessageBuf, MessageSet},
    CommitLog, LogOptions, ReadLimit,
};
use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageResult},
};
use serde::{Deserialize, Serialize};
use sled::{Config, Db};
use std::{iter::FromIterator, marker::PhantomData};
use zerocopy::{AsBytes, FromBytes};

const DEFAULT: &str = "/default_storage/";
const COMMITLOG: &str = "/commitlog/";
const DATABASE: &str = "/database/";
const NPROM: &[u8] = b"NPROM";
const ACC: &[u8] = b"ACC";
const DECIDE: &[u8] = b"DECIDE";
const TRIM: &[u8] = b"TRIM";
const STOPSIGN: &[u8] = b"STOPSIGN";
const SNAPSHOT: &[u8] = b"SNAPSHOT";

// Configuration for `PersistentStorage`.
/// # Fields
/// * `path`: Path to the Commitlog and state storage
/// * `commitlog_options`: Options for the Commitlog
/// * `sled_options` : Options for the sled store, enabled by default
pub struct PersistentStorageConfig {
    path: Option<String>,
    commitlog_options: LogOptions,
    sled_options: Config,
}

impl PersistentStorageConfig {
    /// Returns the current path to the persistent storage.
    pub fn get_path(&self) -> Option<&String> {
        self.path.as_ref()
    }

    /// Sets the path to the persistent storage.
    pub fn set_path(&mut self, path: String) {
        self.path = Some(path);
    }

    /// Returns the options for the Commitlog.
    pub fn get_commitlog_options(&self) -> LogOptions {
        self.commitlog_options.clone()
    }

    /// Sets the options for the Commitlog.
    pub fn set_commitlog_options(&mut self, commitlog_opts: LogOptions) {
        self.commitlog_options = commitlog_opts;
    }

    /// Returns the options for the sled store.
    pub fn get_database_options(&self) -> Config {
        self.sled_options.clone()
    }

    /// Sets the options for the sled store.
    pub fn set_database_options(&mut self, opts: Config) {
        self.sled_options = opts;
    }

    /// Creates a configuration for `PersistentStorage` with the given path and options for Commitlog and sled
    pub fn with(path: String, commitlog_options: LogOptions, sled_options: Config) -> Self {
        Self {
            path: Some(path),
            commitlog_options,
            sled_options,
        }
    }
}

impl Default for PersistentStorageConfig {
    fn default() -> Self {
        let commitlog_options = LogOptions::new(format!("{DEFAULT}{COMMITLOG}"));

        Self {
            path: Some(DEFAULT.to_string()),
            commitlog_options,
            sled_options: Config::new(),
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
    /// Disk-based commit log for entries
    commitlog: CommitLog,
    /// The path to the directory containing a commitlog
    log_path: String,
    /// Local sled key-value store, enabled by default
    sled: Db,
    /// A placeholder for the T: Entry
    t: PhantomData<T>,
}

impl<T: Entry> PersistentStorage<T> {
    /// Creates or opens an existing storage
    pub fn open(storage_config: PersistentStorageConfig) -> Self {
        let path = storage_config.path.expect("No path found in config");

        let commitlog =
            CommitLog::new(storage_config.commitlog_options).expect("Failed to create Commitlog");

        Self {
            commitlog,
            log_path: format!("{path}{COMMITLOG}"),
            sled: {
                let opts = storage_config
                    .sled_options
                    .path(format!("{path}{DATABASE}"));
                Config::open(&opts).expect("Failed to create sled database")
            },
            t: PhantomData::default(),
        }
    }

    /// Creates a new storage instance, panics if a commitlog or sled instance already exists in the given path
    pub fn new(storage_config: PersistentStorageConfig) -> Self {
        let path = storage_config
            .path
            .as_ref()
            .expect("No path found in config");

        std::fs::metadata(format!("{path}{COMMITLOG}")).expect_err(&format!(
            "Cannot create new instance, commitlog already exists in {}",
            path
        ));
        std::fs::metadata(format!("{path}{DATABASE}")).expect_err(&format!(
            "Cannot create new instance, database already exists in {}",
            path
        ));

        Self::open(storage_config)
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
    fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
        let entry_bytes = bincode::serialize(&entry)?;
        let offset = self.commitlog.append_msg(entry_bytes)?;
        self.commitlog.flush()?; // ensure durable writes
        Ok(offset + 1) // +1 as commitlog returns the offset the entry was appended at, while we should return the index that the entry got in the log.
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64> {
        // Required check because Commitlog has a bug where appending an empty set of entries will
        // always return an offset.first() with 0 despite entries being in the log.
        if entries.is_empty() {
            return self.get_log_len();
        }
        let mut serialized = vec![];
        for entry in entries {
            serialized.push(bincode::serialize(&entry)?)
        }
        let offset = self
            .commitlog
            .append(&mut MessageBuf::from_iter(serialized))?;
        self.commitlog.flush()?; // ensure durable writes
        Ok(offset.first() + offset.len() as u64)
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
        if from_idx > 0 && from_idx < self.get_log_len()? {
            self.commitlog.truncate(from_idx)?;
        }
        self.append_entries(entries)
    }

    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        // Check if the commit log has entries up to the requested endpoint.
        if to > self.commitlog.next_offset() || from >= to {
            return Ok(vec![]); // Do an early return
        }

        let buffer = self.commitlog.read(from, ReadLimit::default())?;
        let mut entries = Vec::<T>::with_capacity((to - from) as usize);
        let mut iter = buffer.iter();
        for _ in from..to {
            let msg = iter.next().ok_or(ErrHelper {})?;
            entries.push(bincode::deserialize(msg.payload())?);
        }
        Ok(entries)
    }

    fn get_log_len(&self) -> StorageResult<u64> {
        Ok(self.commitlog.next_offset())
    }

    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        self.get_entries(from, self.commitlog.next_offset())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        {
            let promised = self.sled.get(NPROM)?;
            match promised {
                Some(prom_bytes) => {
                    let ballot = bincode::deserialize(&prom_bytes)?;
                    Ok(Some(ballot))
                }
                None => Ok(Some(Ballot::default())),
            }
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        let prom_bytes = bincode::serialize(&n_prom)?;
        self.sled.insert(NPROM, prom_bytes)?;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<u64> {
        let decided = self.sled.get(DECIDE)?;
        match decided {
            Some(ld_bytes) => Ok(u64::read_from(ld_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()> {
        let ld_bytes = u64::as_bytes(&ld);
        self.sled.insert(DECIDE, ld_bytes)?;
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        let accepted = self.sled.get(ACC)?;
        match accepted {
            Some(acc_bytes) => {
                let ballot = bincode::deserialize(&acc_bytes)?;
                Ok(Some(ballot))
            }
            None => Ok(Some(Ballot::default())),
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        let acc_bytes = bincode::serialize(&na)?;
        self.sled.insert(ACC, acc_bytes)?;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<u64> {
        let trim = self.sled.get(TRIM)?;
        match trim {
            Some(trim_bytes) => Ok(u64::read_from(trim_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        let trim_bytes = u64::as_bytes(&trimmed_idx);
        self.sled.insert(TRIM, trim_bytes)?;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        let stopsign = self.sled.get(STOPSIGN)?;
        match stopsign {
            Some(ss_bytes) => Ok(bincode::deserialize(&ss_bytes)?),
            None => Ok(None),
        }
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        let stopsign = bincode::serialize(&s)?;
        self.sled.insert(STOPSIGN, stopsign)?;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        let snapshot = self.sled.get(SNAPSHOT)?;
        if let Some(snapshot_bytes) = snapshot {
            Ok(bincode::deserialize(snapshot_bytes.as_bytes())?)
        } else {
            Ok(None)
        }
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        let stopsign = bincode::serialize(&snapshot)?;
        self.sled.insert(SNAPSHOT, stopsign)?;
        Ok(())
    }

    // TODO: A way to trim the commitlog without deleting and recreating the log
    fn trim(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        let trimmed_log: Vec<T> = self.get_entries(trimmed_idx, self.commitlog.next_offset())?; // get the log entries from 'trimmed_idx' to latest
        std::fs::remove_dir_all(&self.log_path)?; // remove old log
        let c_opts = LogOptions::new(&self.log_path);
        self.commitlog = CommitLog::new(c_opts)?; // create new commitlog
        self.append_entries(trimmed_log)?;
        Ok(())
    }
}
