#[cfg(all(feature = "rocksdb", feature = "sled"))]
compile_error!("Cannot enable features \"rocksdb\" and \"sled\" at the same time");

use commitlog::{
    message::{MessageBuf, MessageSet},
    CommitLog, LogOptions, ReadLimit,
};
use omnipaxos_core::{
    ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, StopSign, StopSignEntry, Storage},
};
use serde::{Deserialize, Serialize};
use std::{iter::FromIterator, marker::PhantomData};
use zerocopy::{AsBytes, FromBytes};

#[cfg(feature = "rocksdb")]
use rocksdb::{Options, DB};
#[cfg(feature = "sled")]
use sled::{Config, Db};

const DEFAULT: &str = "/default_storage/";
const COMMITLOG: &str = "/commitlog/";
const DATABASE: &str = "/database/";
const NPROM: &[u8] = b"NPROM";
const ACC: &[u8] = b"ACC";
const DECIDE: &[u8] = b"DECIDE";
const TRIM: &[u8] = b"TRIM";
const STOPSIGN: &[u8] = b"STOPSIGN";
const SNAPSHOT: &[u8] = b"SNAPSHOT";

/// Wrapper struct that represents a `Ballot` type. Implements AsBytes and FromBytes.
#[repr(packed)]
#[derive(Clone, Copy, AsBytes, FromBytes)]
struct BallotStorage {
    n: u32,
    priority: u64,
    pid: u64,
}

impl BallotStorage {
    fn with(b: Ballot) -> Self {
        BallotStorage {
            n: b.n,
            priority: b.priority,
            pid: b.pid,
        }
    }
}

/// Wrapper struct that represents a `StopSignEntry` type. Implements Serialize and Deserialize.
#[derive(Clone, Serialize, Deserialize)]
struct StopSignEntryStorage {
    ss: StopSignStorage,
    decided: bool,
}

impl StopSignEntryStorage {
    fn with(ss_entry: StopSignEntry) -> Self {
        StopSignEntryStorage {
            ss: StopSignStorage::with(ss_entry.stopsign),
            decided: ss_entry.decided,
        }
    }
}

/// Wrapper struct that represents a `StopSign` type. Implements Serialize and Deserialize.
#[derive(Clone, Serialize, Deserialize)]
struct StopSignStorage {
    config_id: u32,
    nodes: Vec<u64>,
    metadata: Option<Vec<u8>>,
}

impl StopSignStorage {
    fn with(ss: StopSign) -> Self {
        StopSignStorage {
            config_id: ss.config_id,
            nodes: ss.nodes,
            metadata: ss.metadata,
        }
    }
}

// Configuration for `PersistentStorage`.
/// # Fields
/// * `path`: Path to the Commitlog and state storage
/// * `commitlog_options`: Options for the Commitlog
/// * `rocksdb_options` : Options for the rocksDB store, must be enabled
/// * `sled_options` : Options for the sled store, enabled by default
pub struct PersistentStorageConfig {
    path: Option<String>,
    commitlog_options: LogOptions,
    #[cfg(feature = "rocksdb")]
    rocksdb_options: Options,
    #[cfg(feature = "sled")]
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

    #[cfg(feature = "rocksdb")]
    /// Returns the options for the rocksDB store.
    pub fn get_database_options(&self) -> Options {
        self.rocksdb_options.clone()
    }

    #[cfg(feature = "rocksdb")]
    /// Sets the options for the rocksDB store.
    pub fn set_database_options(&mut self, opts: Options) {
        self.rocksdb_options = opts;
    }

    #[cfg(feature = "sled")]
    /// Returns the options for the sled store.
    pub fn get_database_options(&self) -> Config {
        self.sled_options.clone()
    }

    #[cfg(feature = "sled")]
    /// Sets the options for the sled store.
    pub fn set_database_options(&mut self, opts: Config) {
        self.sled_options = opts;
    }

    #[cfg(feature = "rocksdb")]
    /// Creates a configuration for `PersistentStorage` with the given path and options for Commitlog and rocksDB
    pub fn with(path: String, commitlog_options: LogOptions, rocksdb_options: Options) -> Self {
        Self {
            path: Some(path),
            commitlog_options,
            rocksdb_options,
        }
    }

    #[cfg(feature = "sled")]
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
            #[cfg(feature = "rocksdb")]
            rocksdb_options: {
                let mut opts = Options::default();
                opts.create_if_missing(true);
                opts
            },
            #[cfg(feature = "sled")]
            sled_options: Config::new(),
        }
    }
}

/// A persistent storage implementation, lets sequence paxos write the log
/// and current state to disk. Log entries are serialized and de-serialized
/// into slice of bytes when read or written from the log.
pub struct PersistentStorage<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Disk-based commit log for entries
    commitlog: CommitLog,
    /// The path to the directory containing a commitlog
    log_path: String,
    /// Local RocksDB key-value store, must be enabled as a feature
    #[cfg(feature = "rocksdb")]
    rocksdb: DB,
    /// Local sled key-value store, enabled by default
    #[cfg(feature = "sled")]
    sled: Db,
    /// A placeholder for the T: Entry
    t: PhantomData<T>,
    /// A placeholder for the S: Snapshot<T>
    s: PhantomData<S>,
}

impl<T: Entry, S: Snapshot<T>> PersistentStorage<T, S> {
    /// Creates or opens an existing storage
    pub fn open(storage_config: PersistentStorageConfig) -> Self {
        let path = storage_config.path.expect("No path found in config");

        let commitlog =
            CommitLog::new(storage_config.commitlog_options).expect("Failed to create Commitlog");

        Self {
            commitlog,
            log_path: format!("{path}{COMMITLOG}"),
            #[cfg(feature = "rocksdb")]
            rocksdb: {
                DB::open(&storage_config.rocksdb_options, format!("{path}{DATABASE}"))
                    .expect("Failed to create rocksDB database")
            },
            #[cfg(feature = "sled")]
            sled: {
                let opts = storage_config
                    .sled_options
                    .path(format!("{path}{DATABASE}"));
                Config::open(&opts).expect("Failed to create sled database")
            },
            t: PhantomData::default(),
            s: PhantomData::default(),
        }
    }

    /// Creates a new storage instance, panics if a commitlog or rocksDB/sled instance exists in the given path
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

impl<T, S> Storage<T, S> for PersistentStorage<T, S>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    S: Snapshot<T> + Serialize + for<'a> Deserialize<'a>,
{
    fn append_entry(&mut self, entry: T) -> u64 {
        let entry_bytes = bincode::serialize(&entry).expect("Failed to serialize log entry");
        let offset = self
            .commitlog
            .append_msg(entry_bytes)
            .expect("Failed to append log entry");
        self.commitlog.flush().expect("Failed to flush Commitlog"); // ensure durable writes
        offset + 1 // +1 as commitlog returns the offset the entry was appended at, while we should return the index that the entry got in the log.
    }

    fn append_entries(&mut self, entries: Vec<T>) -> u64 {
        let serialized = entries
            .into_iter()
            .map(|entry| bincode::serialize(&entry).expect("Failed to serialize log entries"));
        let offset = self
            .commitlog
            .append(&mut MessageBuf::from_iter(serialized))
            .expect("Falied to append log entries");
        self.commitlog.flush().expect("Failed to flush Commitlog"); // ensure durable writes
        offset.first() + offset.len() as u64
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
        if from_idx > 0 {
            self.commitlog
                .truncate(from_idx)
                .expect("Failed to truncate log");
        }
        self.append_entries(entries)
    }

    fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
        // Check if the commit log has entries up to the requested endpoint.
        if to > self.commitlog.next_offset() {
            return vec![]; // Do an early return
        }

        let buffer = self
            .commitlog
            .read(from, ReadLimit::default())
            .expect("Failed to read from replicated log");
        let mut entries = Vec::<T>::with_capacity((to - from) as usize);
        let mut iter = buffer.iter();
        for _ in from..to {
            let msg = iter.next().expect("Failed to get log entry from iterator");
            entries.push(
                bincode::deserialize(msg.payload()).expect("Failed to deserialize log entries"),
            );
        }
        entries
    }

    fn get_log_len(&self) -> u64 {
        self.commitlog.next_offset()
    }

    fn get_suffix(&self, from: u64) -> Vec<T> {
        self.get_entries(from, self.commitlog.next_offset())
    }

    fn get_promise(&self) -> Ballot {
        #[cfg(feature = "rocksdb")]
        {
            let promised = self.rocksdb.get(NPROM).expect("Failed to retrieve 'NPROM'");
            match promised {
                Some(prom_bytes) => {
                    let b_store = BallotStorage::read_from(prom_bytes.as_slice())
                        .expect("Failed to deserialize the promised ballot");
                    Ballot::with(b_store.n, b_store.priority, b_store.pid)
                }
                None => Ballot::default(),
            }
        }
        #[cfg(feature = "sled")]
        {
            let promised = self.sled.get(NPROM).expect("Failed to retrieve 'NPROM'");
            match promised {
                Some(prom_bytes) => {
                    let b_store = BallotStorage::read_from(prom_bytes.as_ref())
                        .expect("Failed to deserialize the promised ballot");
                    Ballot::with(b_store.n, b_store.priority, b_store.pid)
                }
                None => Ballot::default(),
            }
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        let ballot_store = BallotStorage::with(n_prom);
        let prom_bytes = ballot_store.as_bytes();
        #[cfg(feature = "rocksdb")]
        {
            self.rocksdb
                .put(NPROM, prom_bytes)
                .expect("Failed to set 'NPROM'");
        }
        #[cfg(feature = "sled")]
        {
            self.sled
                .insert(NPROM, prom_bytes)
                .expect("Failed to set 'NPROM'");
        }
    }

    fn get_decided_idx(&self) -> u64 {
        #[cfg(feature = "rocksdb")]
        {
            let decided = self
                .rocksdb
                .get(DECIDE)
                .expect("Failed to retrieve 'DECIDE'");
            match decided {
                Some(ld_bytes) => u64::read_from(ld_bytes.as_slice())
                    .expect("Failed to deserialize the decided index"),
                None => 0,
            }
        }
        #[cfg(feature = "sled")]
        {
            let decided = self.sled.get(DECIDE).expect("Failed to retrieve 'DECIDE'");
            match decided {
                Some(ld_bytes) => u64::read_from(ld_bytes.as_bytes())
                    .expect("Failed to deserialize the decided index"),
                None => 0,
            }
        }
    }

    fn set_decided_idx(&mut self, ld: u64) {
        let ld_bytes = u64::as_bytes(&ld);
        #[cfg(feature = "rocksdb")]
        {
            self.rocksdb
                .put(DECIDE, ld_bytes)
                .expect("Failed to set 'DECIDE'");
        }
        #[cfg(feature = "sled")]
        {
            self.sled
                .insert(DECIDE, ld_bytes)
                .expect("Failed to set 'DECIDE'");
        }
    }

    fn get_accepted_round(&self) -> Ballot {
        #[cfg(feature = "rocksdb")]
        {
            let accepted = self.rocksdb.get(ACC).expect("Failed to retrieve 'ACC'");
            match accepted {
                Some(acc_bytes) => {
                    let b_store = BallotStorage::read_from(acc_bytes.as_slice())
                        .expect("Failed to deserialize the accepted ballot");
                    Ballot::with(b_store.n, b_store.priority, b_store.pid)
                }
                None => Ballot::default(),
            }
        }
        #[cfg(feature = "sled")]
        {
            let accepted = self.sled.get(ACC).expect("Failed to retrieve 'ACC'");
            match accepted {
                Some(acc_bytes) => {
                    let b_store = BallotStorage::read_from(acc_bytes.as_bytes())
                        .expect("Failed to deserialize the accepted ballot");
                    Ballot::with(b_store.n, b_store.priority, b_store.pid)
                }
                None => Ballot::default(),
            }
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        let ballot_store = BallotStorage::with(na);
        let acc_bytes = ballot_store.as_bytes();
        #[cfg(feature = "rocksdb")]
        {
            self.rocksdb
                .put(ACC, acc_bytes)
                .expect("Failed to set 'ACC'");
        }
        #[cfg(feature = "sled")]
        {
            self.sled
                .insert(ACC, acc_bytes)
                .expect("Failed to set 'ACC'");
        }
    }

    fn get_compacted_idx(&self) -> u64 {
        #[cfg(feature = "rocksdb")]
        {
            let trim = self.rocksdb.get(TRIM).expect("Failed to retrieve 'TRIM'");
            match trim {
                Some(trim_bytes) => u64::read_from(trim_bytes.as_slice())
                    .expect("Failed to deserialize the compacted index"),
                None => 0,
            }
        }
        #[cfg(feature = "sled")]
        {
            let trim = self.sled.get(TRIM).expect("Failed to retrieve 'TRIM'");
            match trim {
                Some(trim_bytes) => u64::read_from(trim_bytes.as_bytes())
                    .expect("Failed to deserialize the compacted index"),
                None => 0,
            }
        }
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) {
        let trim_bytes = u64::as_bytes(&trimmed_idx);
        #[cfg(feature = "rocksdb")]
        {
            self.rocksdb
                .put(TRIM, trim_bytes)
                .expect("Failed to set 'TRIM'");
        }
        #[cfg(feature = "sled")]
        {
            self.sled
                .insert(TRIM, trim_bytes)
                .expect("Failed to set 'TRIM'");
        }
    }

    fn get_stopsign(&self) -> Option<StopSignEntry> {
        #[cfg(feature = "rocksdb")]
        {
            let stopsign = self
                .rocksdb
                .get(STOPSIGN)
                .expect("Failed to retrieve 'STOPSIGN'");
            match stopsign {
                Some(ss_bytes) => {
                    let ss_entry_storage: StopSignEntryStorage = bincode::deserialize(&ss_bytes)
                        .expect("Failed to deserialize the stopsign");
                    Some(StopSignEntry::with(
                        StopSign::with(
                            ss_entry_storage.ss.config_id,
                            ss_entry_storage.ss.nodes,
                            ss_entry_storage.ss.metadata,
                        ),
                        ss_entry_storage.decided,
                    ))
                }
                None => None,
            }
        }
        #[cfg(feature = "sled")]
        {
            let stopsign = self
                .sled
                .get(STOPSIGN)
                .expect("Failed to retrieve 'STOPSIGN'");
            match stopsign {
                Some(ss_bytes) => {
                    let ss_entry_storage: StopSignEntryStorage = bincode::deserialize(&ss_bytes)
                        .expect("Failed to deserialize the stopsign");
                    Some(StopSignEntry::with(
                        StopSign::with(
                            ss_entry_storage.ss.config_id,
                            ss_entry_storage.ss.nodes,
                            ss_entry_storage.ss.metadata,
                        ),
                        ss_entry_storage.decided,
                    ))
                }
                None => None,
            }
        }
    }

    fn set_stopsign(&mut self, s: StopSignEntry) {
        let ss_storage = StopSignEntryStorage::with(s);
        let stopsign = bincode::serialize(&ss_storage).expect("Failed to serialize Stopsign entry");
        #[cfg(feature = "rocksdb")]
        {
            self.rocksdb
                .put(STOPSIGN, stopsign)
                .expect("Failed to set 'STOPSIGN'");
        }
        #[cfg(feature = "sled")]
        {
            self.sled
                .insert(STOPSIGN, stopsign)
                .expect("Failed to set 'STOPSIGN'");
        }
    }

    fn get_snapshot(&self) -> Option<S> {
        #[cfg(feature = "rocksdb")]
        {
            let snapshot = self
                .rocksdb
                .get(SNAPSHOT)
                .expect("Failed to retrieve 'SNAPSHOT'");
            snapshot.map(|snapshot_bytes| {
                bincode::deserialize(snapshot_bytes.as_slice())
                    .expect("Failed to deserialize snapshot")
            })
        }
        #[cfg(feature = "sled")]
        {
            let snapshot = self
                .sled
                .get(SNAPSHOT)
                .expect("Failed to retrieve 'SNAPSHOT'");
            snapshot.map(|snapshot_bytes| {
                bincode::deserialize(snapshot_bytes.as_bytes())
                    .expect("Failed to deserialize snapshot")
            })
        }
    }

    fn set_snapshot(&mut self, snapshot: S) {
        let stopsign = bincode::serialize(&snapshot).expect("Failed to serialize snapshot");
        #[cfg(feature = "rocksdb")]
        {
            self.rocksdb
                .put(SNAPSHOT, stopsign)
                .expect("Failed to set 'SNAPSHOT'");
        }
        #[cfg(feature = "sled")]
        {
            self.sled
                .insert(SNAPSHOT, stopsign)
                .expect("Failed to set 'SNAPSHOT'");
        }
    }

    // TODO: A way to trim the commitlog without deleting and recreating the log
    fn trim(&mut self, trimmed_idx: u64) {
        let trimmed_log: Vec<T> = self.get_entries(trimmed_idx, self.commitlog.next_offset()); // get the log entries from 'trimmed_idx' to latest
        let _ = std::fs::remove_dir_all(&self.log_path); // remove old log
        let c_opts = LogOptions::new(&self.log_path);
        self.commitlog = CommitLog::new(c_opts).expect("Failed to recreate commitlog"); // create new commitlog
        self.append_entries(trimmed_log);
    }
}
