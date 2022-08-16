use commitlog::{
    message::{MessageBuf, MessageSet},
    CommitLog, LogOptions, ReadLimit,
};
use omnipaxos_core::{
    ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, StopSign, StopSignEntry, Storage},
};
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::{iter::FromIterator, marker::PhantomData};
use zerocopy::{AsBytes, FromBytes};

const DEFAULT: &str = "/default_storage/";
const COMMITLOG: &str = "/commitlog/";
const ROCKSDB: &str = "/rocksDB/";
const NPROM: &[u8] = b"NPROM";
const ACC: &[u8] = b"ACC";
const DECIDE: &[u8] = b"DECIDE";
const TRIM: &[u8] = b"TRIM";
const STOPSIGN: &[u8] = b"STOPSIGN";
const SNAPSHOT: &[u8] = b"SNAPSHOT";

/// Wrapper struct that represents `Ballot` struct. Implements AsBytes and FromBytes.
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

/// Wrapper struct that represents `StopSignEntry` struct. Implements Serialize and Deserialize.
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

/// Wrapper struct that represents `StopSign` struct. Implements Serialize and Deserialize.
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
/// * `path`: Path to the commitlog and rocksDB store
/// * `log_config`: Configuration of the commitlog
/// * `rocksdb_options` : Configuration of the rocksDB store
pub struct PersistentStorageConfig {
    path: Option<String>,
    commitlog_options: LogOptions,
    rocksdb_options: Options,
}

impl PersistentStorageConfig {
    pub fn get_path(&self) -> Option<&String> {
        self.path.as_ref()
    }

    pub fn set_path(&mut self, path: String) {
        self.path = Some(path);
    }

    pub fn get_commitlog_options(&self) -> LogOptions {
        self.commitlog_options.clone()
    }

    pub fn set_commitlog_options(&mut self, commitlog_opts: LogOptions) {
        self.commitlog_options = commitlog_opts;
    }

    pub fn get_rocksdb_options(&self) -> Options {
        self.rocksdb_options.clone()
    }

    pub fn set_rocksdb_options(&mut self, rocksdb_opts: Options) {
        self.rocksdb_options = rocksdb_opts;
    }

    pub fn with(path: String, commitlog_options: LogOptions, rocksdb_options: Options) -> Self {
        Self {
            path: Some(path),
            commitlog_options,
            rocksdb_options,
        }
    }
}

impl Default for PersistentStorageConfig {
    fn default() -> Self {
        let commitlog_options = LogOptions::new(format!("{DEFAULT}{COMMITLOG}"));
        let mut rocksdb_options = Options::default();
        rocksdb_options.create_if_missing(true); // Creates rocksdb store if it is missing

        Self {
            path: Some(DEFAULT.to_string()),
            commitlog_options,
            rocksdb_options,
        }
    }
}

/// A persistent storage implementation, lets sequence paxos write the log
/// and variables (e.g. promised, accepted) to disk. Log entries are serialized
/// and de-serialized into slice of bytes when read or written from the log.
pub struct PersistentStorage<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// disk-based commit log for entries
    commitlog: CommitLog,
    /// the path to the directory containing a commitlog
    log_path: String,
    /// local RocksDB key-value store
    rocksdb: DB,
    /// A placeholder for the T: Entry
    t: PhantomData<T>,
    /// A placeholder for the S: Snapshot<T>
    s: PhantomData<S>,
}

impl<T: Entry, S: Snapshot<T>> PersistentStorage<T, S> {

    // Creates or opens an existing storage
    pub fn open(storage_config: PersistentStorageConfig) -> Self {
        let path = storage_config.path.expect("No path found in config");

        let commitlog =
            CommitLog::new(storage_config.commitlog_options).expect("Failed to create Commitlog");
        let rocksdb = DB::open(&storage_config.rocksdb_options, format!("{path}{ROCKSDB}"))
            .expect("Failed to create rocksDB store");

        Self {
            commitlog,
            log_path: format!("{path}{COMMITLOG}"),
            rocksdb,
            t: PhantomData::default(),
            s: PhantomData::default(),
        }
    }

    // Creates a new storage instance, panics if a commitlog or rocksDB instance exists at the given path
    pub fn new(storage_config: PersistentStorageConfig) -> Self {
        let path = storage_config.path.expect("No path found in config");

        std::fs::metadata(format!("{path}{COMMITLOG}"))
            .expect_err(&format!("Cannot create new instance, commitlog already exists in {}", path));
        std::fs::metadata(format!("{path}{ROCKSDB}"))
            .expect_err(&format!("Cannot create new instance, rocksDB store already exists in {}", path));

        let commitlog =
            CommitLog::new(storage_config.commitlog_options).expect("Failed to create Commitlog");
        let rocksdb = DB::open(&storage_config.rocksdb_options, format!("{path}{ROCKSDB}"))
            .expect("Failed to create rocksDB store");

        Self {
            commitlog,
            log_path: format!("{path}{COMMITLOG}"),
            rocksdb,
            t: PhantomData::default(),
            s: PhantomData::default(),
        }
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
        self.commitlog.flush().expect("Failed to flush Commitlog"); // makes sure all writes are durable
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
        self.commitlog.flush().expect("Failed to flush Commitlog");
        offset.first() + offset.len() as u64
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
        self.commitlog
            .truncate(from_idx)
            .expect("Failed to truncate log");
        self.append_entries(entries)
    }

    fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
        let buffer = self
            .commitlog
            .read(from, ReadLimit::default())
            .expect("Failed to read from replicated log");
        let mut entries = Vec::<T>::with_capacity((to - from) as usize);
        let mut iter = buffer.iter();
        for _ in from..to {
            match iter.next() {
                Some(msg) => {
                    entries.push(
                        bincode::deserialize(msg.payload())
                            .expect("Failed to deserialize log entries"),
                    );
                }
                None => {
                    return vec![]; // early return here
                }
            }
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
        let promised = self.rocksdb.get(NPROM).expect("Failed to retrieve 'NPROM'");
        match promised {
            Some(prom_bytes) => {
                let b_store: BallotStorage = FromBytes::read_from(prom_bytes.as_slice())
                    .expect("Failed to deserialize the promised ballot");
                Ballot::with(b_store.n, b_store.priority, b_store.pid)
            }
            None => Ballot::default(),
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        let ballot_store = BallotStorage::with(n_prom);
        let prom_bytes = AsBytes::as_bytes(&ballot_store);
        self.rocksdb
            .put(NPROM, prom_bytes)
            .expect("Failed to set 'NPROM'");
    }

    fn get_decided_idx(&self) -> u64 {
        let decided = self
            .rocksdb
            .get(DECIDE)
            .expect("Failed to retrieve 'DECIDE'");
        match decided {
            Some(ld_bytes) => FromBytes::read_from(ld_bytes.as_slice())
                .expect("Failed to deserialize the decided index"),
            None => 0,
        }
    }

    fn set_decided_idx(&mut self, ld: u64) {
        let ld_bytes = AsBytes::as_bytes(&ld);
        self.rocksdb
            .put(DECIDE, ld_bytes)
            .expect("Failed to set 'DECIDE'");
    }

    fn get_accepted_round(&self) -> Ballot {
        let accepted = self.rocksdb.get(ACC).expect("Failed to retrieve 'ACC'");
        match accepted {
            Some(acc_bytes) => {
                let b_store: BallotStorage = FromBytes::read_from(acc_bytes.as_slice())
                    .expect("Failed to deserialize the accepted ballot");
                Ballot::with(b_store.n, b_store.priority, b_store.pid)
            }
            None => Ballot::default(),
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        let ballot_store = BallotStorage::with(na);
        let acc_bytes = AsBytes::as_bytes(&ballot_store);
        self.rocksdb
            .put(ACC, acc_bytes)
            .expect("Failed to set 'ACC'");
    }

    fn get_compacted_idx(&self) -> u64 {
        let trim = self.rocksdb.get(TRIM).expect("Failed to retrieve 'TRIM'");
        match trim {
            Some(trim_bytes) => FromBytes::read_from(trim_bytes.as_slice())
                .expect("Failed to deserialize the compacted index"),
            None => 0,
        }
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) {
        let trim_bytes = AsBytes::as_bytes(&trimmed_idx);
        self.rocksdb
            .put(TRIM, trim_bytes)
            .expect("Failed to set 'TRIM'");
    }

    fn get_stopsign(&self) -> Option<StopSignEntry> {
        let stopsign = self
            .rocksdb
            .get(STOPSIGN)
            .expect("Failed to retrieve 'STOPSIGN'");
        match stopsign {
            Some(ss_bytes) => {
                let ss_storage: StopSignEntryStorage =
                    bincode::deserialize(&ss_bytes).expect("Failed to deserialize the stopsign");
                Some(StopSignEntry::with(
                    StopSign::with(
                        ss_storage.ss.config_id,
                        ss_storage.ss.nodes,
                        ss_storage.ss.metadata,
                    ),
                    ss_storage.decided,
                ))
            }
            None => None,
        }
    }

    fn set_stopsign(&mut self, s: StopSignEntry) {
        let ss_storage = StopSignEntryStorage::with(s);
        let stopsign = bincode::serialize(&ss_storage).expect("Failed to serialize Stopsign entry");
        self.rocksdb
            .put(STOPSIGN, stopsign)
            .expect("Failed to set 'STOPSIGN'");
    }

    fn get_snapshot(&self) -> Option<S> {
        let snapshot = self
            .rocksdb
            .get(SNAPSHOT)
            .expect("Failed to retrieve 'SNAPSHOT'");
        snapshot.map(|snapshot_bytes| {
            bincode::deserialize(snapshot_bytes.as_slice()).expect("Failed to deserialize snapshot")
        })
    }

    fn set_snapshot(&mut self, snapshot: S) {
        let stopsign = bincode::serialize(&snapshot).expect("Failed to serialize snapshot");
        self.rocksdb
            .put(SNAPSHOT, stopsign)
            .expect("Failed to set 'SNAPSHOT'");
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
