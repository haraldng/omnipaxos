#[allow(missing_docs)]
pub mod persistent_storage {
    use commitlog::{message::MessageSet, CommitLog, LogOptions, ReadLimit};
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSign, StopSignEntry, Storage},
    };
    use rocksdb::{Options, DB};
    use serde::{Deserialize, Serialize};
    use std::marker::PhantomData;
    use zerocopy::{AsBytes, FromBytes};

    const COMMITLOG: &str = "commitlog/";
    const ROCKSDB: &str = "rocksDB/";
    const NPROM: &[u8] = &[1];
    const ACC: &[u8] = &[2];
    const DECIDE: &[u8] = &[3];
    const TRIM: &[u8] = &[4];
    const STOPSIGN: &[u8] = &[5];
    const SNAPSHOT: &[u8] = &[6];

    /// Structs that can be serialized or deserialized from bytes
    /// Each struct represents a certain type in 'omnipaxos_core' 
    /// that must be serialised before storing the data.
    #[repr(packed)]
    #[derive(Clone, Copy, AsBytes, FromBytes)]
    pub struct BallotStorage {
        n: u32,
        priority: u64,
        pid: u64,
    }

    impl BallotStorage {
        pub fn with(b: Ballot) -> Self {
            BallotStorage {
                n: b.n,
                priority: b.priority,
                pid: b.pid,
            }
        }
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub struct StopSignEntryStorage {
        stopsign: StopSignStorage,
        decided: bool,
    }

    impl StopSignEntryStorage {
        pub fn with(ss_entry: StopSignEntry) -> Self {
            StopSignEntryStorage {
                stopsign: StopSignStorage::with(ss_entry.stopsign),
                decided: ss_entry.decided,
            }
        }
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub struct StopSignStorage {
        config_id: u32,
        nodes: Vec<u64>,
        metadata: Option<Vec<u8>>,
    }

    impl StopSignStorage {
        pub fn with(ss: StopSign) -> Self {
            StopSignStorage {
                config_id: ss.config_id,
                nodes: ss.nodes,
                metadata: ss.metadata,
            }
        }
    }

    // Configuration for `PersistentStorage`.
    /// # Fields
    /// * `clog_path`: The path to the directory of the commit log
    /// * `rocksdb_path`: The path to the rocksDB key-value store
    /// * `log_config`: Configuration of the 'commitlog'
    pub struct PersistentStorageConfig {
        log_path: Option<String>,
        log_seg_max_bytes: usize,
        log_index_max_bytes: usize,
        log_entry_max_bytes: usize,
        rocksdb_path: Option<String>,
        rocksdb_options: Options,
    }

    impl PersistentStorageConfig {
        pub fn get_commitlog_path(&self) -> Option<&String> {
            self.log_path.as_ref()
        }

        pub fn set_commitlog_path(mut self, path: String) {
            self.log_path = Some(path);
        }

        pub fn get_rocksdb_path(&self) -> Option<&String> {
            self.rocksdb_path.as_ref()
        }

        pub fn set_rocksdb_path(mut self, path: String) {
            self.rocksdb_path = Some(path);
        }

        pub fn get_entry_max_bytes(&self) -> usize {
            self.log_entry_max_bytes
        }

        pub fn set_entry_max_bytes(mut self, entry_bytes: usize) {
            self.log_entry_max_bytes = entry_bytes;
        }

        pub fn get_index_max_bytes(&self) -> usize {
            self.log_index_max_bytes
        }

        pub fn set_index_max_bytes(mut self, index_bytes: usize) {
            self.log_index_max_bytes = index_bytes;
        }

        pub fn get_segment_max_bytes(&self) -> usize {
            self.log_seg_max_bytes
        }

        pub fn set_segment_max_bytes(mut self, segment_bytes: usize) {
            self.log_seg_max_bytes = segment_bytes;
        }

        pub fn get_rocksdb_options(&self) -> Options {
            self.rocksdb_options.clone()
        }

        pub fn set_rocksdb_options(mut self, rocksdb_opts: Options) {
            self.rocksdb_options = rocksdb_opts;
        }
    }

    impl Default for PersistentStorageConfig {
        fn default() -> Self {
            let mut rocksdb_options = Options::default();
            rocksdb_options.increase_parallelism(4); // Set the amount threads for rocksDB compaction and flushing
            rocksdb_options.create_if_missing(true); // Creates an database if its missing in the path

            Self {
                log_path: Some(COMMITLOG.to_string()),
                log_seg_max_bytes: 0x10000, // 64 kB for each segment (entry)
                log_index_max_bytes: 5000,  // Max 10,000 log entries in the commitlog
                log_entry_max_bytes: 64000, // Max 64 kilobytes for each message
                rocksdb_path: Some(ROCKSDB.to_string()),
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
        c_log: CommitLog,
        /// the path to the directory containing a commitlog
        c_path: String,
        /// local RocksDB key-value store
        db: DB,
        /// A placeholder for the T: Entry
        t: PhantomData<T>,
        /// A placeholder for the S: Snapshot<T>
        s: PhantomData<S>,
    }

    impl<T: Entry, S: Snapshot<T>> PersistentStorage<T, S> {
        pub fn with(storage_config: PersistentStorageConfig, replica_id: &str) -> Self {
            // Paths to commitlog and rocksDB store
            let c_path: String = storage_config.log_path.unwrap() + replica_id;
            let db_path = storage_config.rocksdb_path.unwrap() + replica_id;

            // Check if storage already exists on the paths
            std::fs::metadata(&c_path).expect_err("commitlog already exists in path");
            std::fs::metadata(&db_path).expect_err("rocksDB store already exists in path");

            // set options
            let mut c_opts = LogOptions::new(&c_path);
            c_opts.segment_max_bytes(storage_config.log_seg_max_bytes);
            c_opts.index_max_items(storage_config.log_index_max_bytes);
            c_opts.message_max_bytes(storage_config.log_entry_max_bytes);

            // Initialize commitlog for entries and rocksDB
            let c_log = CommitLog::new(c_opts).unwrap();
            let db = DB::open(&storage_config.rocksdb_options, &db_path).unwrap();

            Self {
                c_log,
                c_path,
                db,
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
            let entry_bytes = bincode::serialize(&entry).unwrap();
            match self.c_log.append_msg(entry_bytes) {
                Ok(x) => x,
                Err(_e) => panic!(),
            }
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            for entry in entries {
                self.append_entry(entry);
            }
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.c_log
                .truncate(from_idx)
                .expect("Failed to truncate commitlog");
            self.append_entries(entries)
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            let buffer = match self.c_log.read(from, ReadLimit::default()) {
                Ok(buf) => buf,
                Err(_) => panic!(),
            };
            let mut entries = Vec::<T>::with_capacity((to - from) as usize);
            let mut iter = buffer.iter();
            for _ in from..to {
                let msg = iter.next().unwrap();
                entries.push(bincode::deserialize(msg.payload()).unwrap());
            }
            entries
        }

        fn get_log_len(&self) -> u64 {
            self.c_log.next_offset()
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            self.get_entries(from, self.get_log_len())
        }

        fn get_promise(&self) -> Ballot {
            match self.db.get(NPROM) {
                Ok(Some(prom_bytes)) => {
                    let b_store: BallotStorage =
                        FromBytes::read_from(prom_bytes.as_slice()).unwrap();
                    Ballot {
                        n: b_store.n,
                        priority: b_store.priority,
                        pid: b_store.pid,
                    }
                }
                Ok(None) => Ballot {
                    n: 0,
                    priority: 0,
                    pid: 0,
                },
                Err(_e) => panic!(),
            }
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            let ballot_store = BallotStorage::with(n_prom);
            let prom_bytes = AsBytes::as_bytes(&ballot_store);
            self.db.put(NPROM, prom_bytes).unwrap()
        }

        fn get_decided_idx(&self) -> u64 {
            match self.db.get(DECIDE) {
                Ok(Some(ld_bytes)) => bincode::deserialize(&ld_bytes).unwrap(),
                Ok(None) => 0,
                Err(_e) => panic!(),
            }
        }

        fn set_decided_idx(&mut self, ld: u64) {
            let ld_bytes = bincode::serialize(&ld).unwrap();
            self.db.put(DECIDE, ld_bytes).unwrap();
        }

        fn get_accepted_round(&self) -> Ballot {
            match self.db.get(ACC) {
                Ok(Some(prom_bytes)) => {
                    let b_store: BallotStorage =
                        FromBytes::read_from(prom_bytes.as_slice()).unwrap();
                    Ballot {
                        n: b_store.n,
                        priority: b_store.priority,
                        pid: b_store.pid,
                    }
                }
                Ok(None) => Ballot {
                    n: 0,
                    priority: 0,
                    pid: 0,
                },
                Err(_e) => panic!(),
            }
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            let ballot_store = BallotStorage::with(na);
            let acc_bytes = AsBytes::as_bytes(&ballot_store);
            self.db.put(ACC, acc_bytes).unwrap();
        }

        fn get_compacted_idx(&self) -> u64 {
            match self.db.get(TRIM) {
                Ok(Some(trim_bytes)) => bincode::deserialize(&trim_bytes).unwrap(),
                Ok(None) => 0,
                Err(_e) => panic!(),
            }
        }

        fn set_compacted_idx(&mut self, trimmed_idx: u64) {
            let trim_bytes = bincode::serialize(&trimmed_idx).unwrap();
            self.db.put(TRIM, trim_bytes).unwrap();
        }

        fn get_stopsign(&self) -> Option<StopSignEntry> {
            match self.db.get(STOPSIGN) {
                Ok(Some(ss_bytes)) => {
                    let ss_store: StopSignEntryStorage = bincode::deserialize(&ss_bytes).unwrap();
                    Some(StopSignEntry {
                        stopsign: StopSign {
                            config_id: ss_store.stopsign.config_id,
                            metadata: ss_store.stopsign.metadata,
                            nodes: ss_store.stopsign.nodes,
                        },
                        decided: ss_store.decided,
                    })
                }
                Ok(None) => None,
                Err(_e) => panic!(),
            }
        }

        fn set_stopsign(&mut self, s: StopSignEntry) {
            let ss_storage = StopSignEntryStorage::with(s);
            let stopsign = bincode::serialize(&ss_storage).unwrap();
            self.db.put(STOPSIGN, stopsign).unwrap(); 
        }

        fn get_snapshot(&self) -> Option<S> {
            match self.db.get(SNAPSHOT) {
                Ok(Some(trim_bytes)) => Some(bincode::deserialize(&trim_bytes).unwrap()),
                Ok(None) => None,
                Err(_e) => panic!(),
            }
        }

        fn set_snapshot(&mut self, snapshot: S) {
            let stopsign = bincode::serialize(&snapshot).unwrap();
            self.db.put(SNAPSHOT, stopsign).unwrap();
        }

        // TODO: A way to trim the comitlog without deleting and recreating the log
        fn trim(&mut self, trimmed_idx: u64) {
            let trimmed_log: Vec<T> = self.get_entries(trimmed_idx, self.c_log.next_offset()); // get the log entries from 'trimmed_idx' to latest
            let _ = std::fs::remove_dir_all(&self.c_path); // remove old log
            let c_opts = LogOptions::new(&self.c_path); // create new commitlog, insert the log into it
            self.c_log = CommitLog::new(c_opts).unwrap();
            self.append_entries(trimmed_log);
        }
    }
}

pub mod memory_storage {
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSignEntry, Storage},
    };
    /// An in-memory storage implementation for SequencePaxos.
    #[derive(Clone)]
    pub struct MemoryStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        /// Vector which contains all the logged entries in-memory.
        log: Vec<T>,
        /// Last promised round.
        n_prom: Ballot,
        /// Last accepted round.
        acc_round: Ballot,
        /// Length of the decided log.
        ld: u64,
        /// Garbage collected index.
        trimmed_idx: u64,
        /// Stored snapshot
        snapshot: Option<S>,
        /// Stored StopSign
        stopsign: Option<StopSignEntry>,
    }

    impl<T, S> Storage<T, S> for MemoryStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        fn append_entry(&mut self, entry: T) -> u64 {
            self.log.push(entry);
            self.get_log_len()
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let mut e = entries;
            self.log.append(&mut e);
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.log.truncate(from_idx as usize);
            self.append_entries(entries)
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
        }

        fn set_decided_idx(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn get_decided_idx(&self) -> u64 {
            self.ld
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_round(&self) -> Ballot {
            self.acc_round
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            self.log
                .get(from as usize..to as usize)
                .unwrap_or(&[])
                .to_vec()
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            match self.log.get(from as usize..) {
                Some(s) => s.to_vec(),
                None => vec![],
            }
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }

        fn set_stopsign(&mut self, s: StopSignEntry) {
            self.stopsign = Some(s);
        }

        fn get_stopsign(&self) -> Option<StopSignEntry> {
            self.stopsign.clone()
        }

        fn trim(&mut self, trimmed_idx: u64) {
            self.log.drain(0..trimmed_idx as usize);
        }

        fn set_compacted_idx(&mut self, trimmed_idx: u64) {
            self.trimmed_idx = trimmed_idx;
        }

        fn get_compacted_idx(&self) -> u64 {
            self.trimmed_idx
        }

        fn set_snapshot(&mut self, snapshot: S) {
            self.snapshot = Some(snapshot);
        }

        fn get_snapshot(&self) -> Option<S> {
            self.snapshot.clone()
        }
    }

    impl<T: Entry, S: Snapshot<T>> Default for MemoryStorage<T, S> {
        fn default() -> Self {
            Self {
                log: vec![],
                n_prom: Ballot::default(),
                acc_round: Ballot::default(),
                ld: 0,
                trimmed_idx: 0,
                snapshot: None,
                stopsign: None,
            }
        }
    }
}
