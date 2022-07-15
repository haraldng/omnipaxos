
/// An persistent storage implementation, lets sequencepaxos write the log
/// and variables (promised, accepted) to disk. Every log entry is serialized into
/// a slice of bytes before and de-serialized when read from the log. 
#[allow(missing_docs)]
pub mod persistent_storage {
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSignEntry, Storage},
    };
    use commitlog::{
        message::{MessageSet}, CommitLog, LogOptions, ReadLimit,
    };
    use rocksdb::{Options, DB};
    use zerocopy::{AsBytes, FromBytes};

    //#[derive(Debug)]
    pub struct PersistentState<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        /// a disk-based commit log for entries
        c_log: CommitLog,
        /// a struct for accessing local RocksDB database
        db: DB,
        /// Garbage collected index.
        trimmed_idx: u64,
        /// Stored snapshot
        snapshot: Option<S>,
        /// Stored StopSign
        stopsign: Option<StopSignEntry>,

        //todo: remain until T is removed
        // /// Vector which contains all the logged entries in-memory.
        log: Vec<T>,
    }

    impl<T: Entry, S: Snapshot<T>> PersistentState<T, S> {
        pub fn with(replica_id: &str) -> Self {
            // initialize a commit log for entries
            let c_opts = LogOptions::new("commitlog/".to_string() + &replica_id.to_string());
            let c_log = CommitLog::new(c_opts).unwrap();

            // create a path and options for DB
            let path = "rocksDB/".to_string() + &replica_id.to_string();
            let mut db_opts = Options::default();
            let lru = rocksdb::Cache::new_lru_cache(1200 as usize).unwrap();
           
            // setting options for DB
            db_opts.set_row_cache(&lru);
            db_opts.increase_parallelism(2);
            db_opts.set_max_write_buffer_number(16);
            db_opts.create_if_missing(true);

            let db = DB::open(&db_opts, &path).unwrap();
            Self {
                c_log: c_log,
                db: db,
                log: vec![],
                trimmed_idx: 0,
                snapshot: None,
                stopsign: None,
            }
        }
    }

    impl<T, S> Storage<T, S> for PersistentState<T, S>
    where
        T: Entry + zerocopy::AsBytes + zerocopy::FromBytes,
        S: Snapshot<T>,
    {
        // // Todo: a function for destroying the database in the given path, also flushes the commitlog. 
        // fn close_db(&self) {
        //     let _ = DB::destroy(&Options::default(), &self.path);
        //     fs::remove_dir("commitlog").expect("CANNOT REMOVE COMMITLOG");
        //     print!("REMOVED COMMITLOG")
        // }

        fn append_entry(&mut self, entry: T) -> u64 {
            println!("append entry! {:?}", entry);
            let entry_bytes = AsBytes::as_bytes(&entry);
            let offset = self.c_log.append_msg(entry_bytes);
            match offset {
                Err(_e) => 0,
                Ok(x) => {
                    x
                },
            }
        }

        // todo: perhaps replace with Commitlog::append, read more about MessageSetMut
        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            println!("append entries!");
            for e in entries {
                self.append_entry(e);
            }
            self.c_log.next_offset()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            println!("append on prefix!"); 
            let _ = self.c_log.truncate(from_idx-1); // truncate removes entries from 'from_idx'
            let offset = self.append_entries(entries);
            offset
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            // let res = self.c_log.read(from, ReadLimit::max_bytes(to as usize)).unwrap_or(MessageBuf::default()).iter()
            // .map(|msg| {FromBytes::read_from(msg.payload()).unwrap()}).collect();
            // println!("result from get_entries {:?}", res);
            // res
           
            //println!("get_entries FROM and TO: {:?} -> {:?} ", from, to);
            let buffer = self.c_log.read(from, ReadLimit::default()).unwrap(); //todo 32 is the magic number
            let mut entries = vec![];
            for (idx, msg) in buffer.iter().enumerate() {
                if idx >= to as usize{ break }
                let entry = FromBytes::read_from(msg.payload()).unwrap();
                entries.push(entry);       
            }
            //println!("res from get_entries {:?}", entries);
            entries   
        }

        fn get_log_len(&self) -> u64 {
            let res = self.c_log.next_offset();
            println!("log length: {:?}", res);
            res
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            println!("get suffix!");
            let max: u64 = self.get_log_len();
            self.get_entries(from, max)
        }

        /// Last promised round.
        fn get_promise(&self) -> Ballot {
            match self.db.get(b"n_prom") {
                Ok(Some(value)) => {
                    let mut prom_bytes = value;
                    let prom_bytes: &mut [u8] = &mut prom_bytes;
                    let res: Ballot = FromBytes::read_from(prom_bytes).unwrap();
                    res
                }
                Ok(None) => Ballot::default(), 
                Err(_e) => Ballot::default(),
            }
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            let prom_bytes = AsBytes::as_bytes(&n_prom);
            self.db.put(b"n_prom", prom_bytes).unwrap()
        }

        /// Length of the decided log.
        fn get_decided_idx(&self) -> u64 {
            match self.db.get(b"ld") {
                Ok(Some(value)) => {
                    let ld_bytes: &[u8] = &value;
                    let res: u64 = FromBytes::read_from(ld_bytes).unwrap();
                    res
                }
                Ok(None) => 0,
                Err(_e) => todo!(),
            }
        }

        fn set_decided_idx(&mut self, ld: u64) {
            let ld_bytes = AsBytes::as_bytes(&ld);
            self.db.put(b"ld", ld_bytes).unwrap();
        }

        /// Last accepted round.
        fn get_accepted_round(&self) -> Ballot {
            match self.db.get(b"acc_round") {
                Ok(Some(value)) => {
                    let mut acc_bytes = value;
                    let acc_bytes: &mut [u8] = &mut acc_bytes;
                    let res: Ballot = FromBytes::read_from(acc_bytes).unwrap();
                    res
                }
                Ok(None) => Ballot::default(), 
                Err(_e) => Ballot::default(),
            }
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            let acc_bytes = AsBytes::as_bytes(&na);
            self.db.put(b"n_prom", acc_bytes).unwrap();
        }

        fn set_stopsign(&mut self, s: StopSignEntry) {
            self.stopsign = Some(s);
        }

        fn get_stopsign(&self) -> Option<StopSignEntry> {
            self.stopsign.clone()
        }

        // TODO: solve the bug with truncate not removing the first element in commitlog
        fn trim(&mut self, trimmed_idx: u64) {
            println!("trim!");
            let len = self.c_log.next_offset();
            let trimmed_log: Vec<T> = self.get_entries(trimmed_idx, len);

            println!("amount entries that should be in trimmed log {:?}", len - trimmed_idx);
            println!("the trimmed log {:?}", trimmed_log);
            println!("length before truncate {:?}", self.c_log.next_offset());
            let _ = self.c_log.truncate(0);
            println!("length after truncate {:?}", self.c_log.next_offset());
            self.append_entries(trimmed_log);
        }

        fn set_compacted_idx(&mut self, trimmed_idx: u64) {
            //println!("set_compacted_idx!, {}", trimmed_idx);
            self.trimmed_idx = trimmed_idx;
        }

        fn get_compacted_idx(&self) -> u64 {
            //println!("get_compacted_idx!, {}", self.trimmed_idx);
            self.trimmed_idx
        }

        fn set_snapshot(&mut self, snapshot: S) {
            self.snapshot = Some(snapshot);
        }

        fn get_snapshot(&self) -> Option<S> {
            self.snapshot.clone()
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
        // fn close_db(&self) {}

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
            self.log.get(from as usize..to as usize).unwrap_or(&[]).to_vec() // todo added to_vec 
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
