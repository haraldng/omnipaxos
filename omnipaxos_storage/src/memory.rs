use zerocopy::{FromBytes, AsBytes};

/// Stores some of the contents of a 
/// StopSignEntry in an rocksDB storage
#[repr(packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes)]
pub struct StopSignStorage {
    pub decided: u8,
    pub config_id: u32
}

#[allow(missing_docs)]
pub mod persistent_storage {
    use commitlog::{
        message::{MessageBuf, MessageSet},
        CommitLog, LogOptions, ReadLimit,
    };
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSign, StopSignEntry, Storage},
    };
    use rocksdb::{Options, DB};
    use std::marker::PhantomData;
    use zerocopy::{AsBytes, FromBytes};

    use super::StopSignStorage;

    const COMMITLOG: &str = "commitlog/";
    const ROCKSDB: &str = "rocksDB/";

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
        // every single getter and setter
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

        // todo: see if it works
        // pub fn get_commitlog_options(&self) -> LogOptions {
        //     self.log_options.clone()
        // }

        // pub fn set_commitlog_options(mut self, log_opts: LogOptions) {
        //     self.log_options = log_opts;
        // }

        pub fn get_rocksdb_options(&self) -> Options {
            self.rocksdb_options.clone()
        }

        pub fn set_rocksdb_options(mut self, rocksdb_opts: Options) {
            self.rocksdb_options = rocksdb_opts;
        }
    }

    impl Default for PersistentStorageConfig {
        fn default() -> Self {
            let mut db_opts = Options::default();
            db_opts.set_write_buffer_size(0); // Set amount data to build up in single memtable before converting to on-disk file
            db_opts.set_db_write_buffer_size(0); // Set total amount of data to build up in all memtables before writing to disk
            db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4); // set compression type
            db_opts.set_max_write_buffer_number(1); // Max buffers for writing in memory
            db_opts.set_max_write_buffer_size_to_maintain(0); // max size of write buffers to maintain in memory
            db_opts.set_use_direct_reads(true); // data read from disk will not be cache or buffered
            db_opts.set_use_direct_io_for_flush_and_compaction(true); // data flushed or compacted will not be cached or buffered
            db_opts.increase_parallelism(4); // Set the amount threads for rocksDB compaction and flushing
            db_opts.create_if_missing(true); // Creates an database if its missing in the path

            Self {
                log_path: Some(COMMITLOG.to_string()),
                log_seg_max_bytes: 0x10000, // 64 kB for each segment (entry)
                log_index_max_bytes: 5000,  // Max 10,000 log entries in the commitlog
                log_entry_max_bytes: 64000, // Max 64 kilobytes for each message
                rocksdb_path: Some(ROCKSDB.to_string()),
                rocksdb_options: db_opts,
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
        c_log_path: String,
        /// local RocksDB key-value store
        db: DB,
        /// Stored snapshot
        snapshot: Option<S>,
        /// Stored StopSign
        stopsign: Option<StopSignEntry>,
        /// A placeholder for the T: Entry
        t: PhantomData<T>,
    }

    impl<T: Entry, S: Snapshot<T>> PersistentStorage<T, S> {
        pub fn with(storage_config: PersistentStorageConfig, replica_id: &str) -> Self {
            // Paths to commitlog and rocksDB store
            let c_path: String = storage_config.log_path.unwrap() + &replica_id.to_string();
            let db_path = storage_config.rocksdb_path.unwrap() + &replica_id.to_string();

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
                c_log: c_log,
                c_log_path: c_path,
                db: db,
                snapshot: None,
                stopsign: None,
                t: PhantomData::default(),
            }
        }
    }

    impl<T, S> Storage<T, S> for PersistentStorage<T, S>
    where
        T: Entry + AsBytes + FromBytes,
        S: Snapshot<T>,
    {
        fn append_entry(&mut self, entry: T) -> u64 {
            let entry_bytes = AsBytes::as_bytes(&entry);
            match self.c_log.append_msg(entry_bytes) {
                Ok(x) => x,
                Err(_e) => panic!(),
            }
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let mut buf: MessageBuf = MessageBuf::default();
            for entry in entries {
                match buf.push(AsBytes::as_bytes(&entry)) {
                    Ok(()) => (),
                    Err(_) => panic!(),
                }
            }

            match self.c_log.append(&mut buf) {
                Ok(_offset) => self.get_log_len(),
                Err(_) => panic!(),
            }
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            let _ = self.c_log.truncate(from_idx);
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
                entries.push(FromBytes::read_from(msg.payload()).unwrap());
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
            match self.db.get(b"n_prom") {
                Ok(Some(prom_bytes)) => FromBytes::read_from(&prom_bytes as &[u8]).unwrap(),
                Ok(None) => Ballot::default(),
                Err(_e) => panic!(),
            }
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            let prom_bytes = AsBytes::as_bytes(&n_prom);
            self.db.put(b"n_prom", prom_bytes).unwrap()
        }

        fn get_decided_idx(&self) -> u64 {
            match self.db.get(b"ld") {
                Ok(Some(ld_bytes)) => FromBytes::read_from(&ld_bytes as &[u8]).unwrap(),
                Ok(None) => 0,
                Err(_e) => panic!(),
            }
        }

        fn set_decided_idx(&mut self, ld: u64) {
            let ld_bytes = AsBytes::as_bytes(&ld);
            self.db.put(b"ld", ld_bytes).unwrap();
        }

        fn get_accepted_round(&self) -> Ballot {
            match self.db.get(b"acc_round") {
                Ok(Some(acc_bytes)) => FromBytes::read_from(&acc_bytes as &[u8]).unwrap(),
                Ok(None) => Ballot::default(),
                Err(_e) => panic!(),
            }
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            let acc_bytes = AsBytes::as_bytes(&na);
            self.db.put(b"n_prom", acc_bytes).unwrap();
        }

        fn get_compacted_idx(&self) -> u64 {
            match self.db.get(b"trim_idx") {
                Ok(Some(trim_bytes)) => FromBytes::read_from(&trim_bytes as &[u8]).unwrap(),
                Ok(None) => 0,
                Err(_e) => panic!(),
            }
        }

        fn set_compacted_idx(&mut self, trimmed_idx: u64) {
            let trim_bytes = AsBytes::as_bytes(&trimmed_idx);
            self.db.put(b"trim_idx", trim_bytes).unwrap();
        }

        fn get_stopsign(&self) -> Option<StopSignEntry> {
            self.stopsign.clone()
            // let ss_storage = match self.db.get(b"ss_storage") {
            //     Ok(Some(storage_bytes)) => FromBytes::read_from(&storage_bytes as &[u8]).unwrap(),
            //     Ok(None) => StopSignStorage { decided: 0, config_id: 0 },
            //     Err(_e) => panic!(),
            // };
            // let ss_nodes= match self.db.get(b"ss_nodes") {
            //     Ok(Some(nodes_bytes)) => FromBytes::read_from(&nodes_bytes as &[u8]).unwrap(),
            //     Ok(None) => panic!(), // todo figure out
            //     Err(_e) => panic!(),
            // };
            // let ss_meta = match self.db.get(b"ss_storage") {
            //     Ok(Some(storage_bytes)) => FromBytes::read_from(&storage_bytes as &[u8]).unwrap(),
            //     Ok(None) => panic!(), 
            //     Err(_e) => panic!(),
            // };

            // let mut ss_nodes: Vec<u64> = ss_nodes as
        }

        fn set_stopsign(&mut self, s: StopSignEntry) {
            self.stopsign = Some(s);
            // let ss_storage: StopSignStorage = StopSignStorage { 
            //     decided: s.decided as u8, 
            //     config_id: s.stopsign.config_id 
            // };
            // let ss_storage = AsBytes::as_bytes(&ss_storage);
            // let ss_nodes = AsBytes::as_bytes(&s.stopsign.nodes as &[u64]);
            // let binding = s.stopsign.metadata.unwrap();
            // let ss_meta =  AsBytes::as_bytes(&binding as &[u8]);
            
            // self.db.put(b"ss_storage", ss_storage).unwrap();
            // self.db.put(b"ss_nodes", ss_nodes).unwrap();
            // self.db.put(b"ss_meta", ss_meta).unwrap();
        }


        fn get_snapshot(&self) -> Option<S> {
            self.snapshot.clone()
        }

        fn set_snapshot(&mut self, snapshot: S) {
            self.snapshot = Some(snapshot);
        }

        // TODO: A way to trim the comitlog without deleting and recreating the log
        fn trim(&mut self, trimmed_idx: u64) {
            let trimmed_log: Vec<T> = self.get_entries(trimmed_idx, self.c_log.next_offset()); // get the entire log, drain it until trimmed_idx
            let _ = std::fs::remove_dir_all(&self.c_log_path); // remove old log

            let c_opts = LogOptions::new(&self.c_log_path); // create new, insert the log into it
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
