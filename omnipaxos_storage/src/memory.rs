//use omnipaxos_core::ballot_leader_election::Ballot;
// use zerocopy::{AsBytes, FromBytes};

use std::{fmt::Debug, marker::PhantomData};
/// Type of the entries stored in the log.

pub trait Entry: Clone + Debug {}

impl<T> Entry for T where T: Clone + Debug {}

/// A StopSign entry that marks the end of a configuration. Used for reconfiguration.
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct StopSignEntry {
    pub stopsign: StopSign,
    pub decided: bool,
}

impl StopSignEntry {
    /// Creates a [`StopSign`].
    pub fn with(stopsign: StopSign, decided: bool) -> Self {
        StopSignEntry { stopsign, decided }
    }
}

/// A StopSign entry that marks the end of a configuration. Used for reconfiguration.
#[derive(Clone, Debug)]
pub struct StopSign {
    /// The identifier for the new configuration.
    pub config_id: u32,
    /// The process ids of the new configuration.
    pub nodes: Vec<u64>,
    /// Metadata for the reconfiguration. Can be used for pre-electing leader for the new configuration and skip prepare phase when starting the new configuration with the given leader.
    pub metadata: Option<Vec<u8>>,
}

impl StopSign {
    /// Creates a [`StopSign`].
    pub fn with(config_id: u32, nodes: Vec<u64>, metadata: Option<Vec<u8>>) -> Self {
        StopSign {
            config_id,
            nodes,
            metadata,
        }
    }
}

impl PartialEq for StopSign {
    fn eq(&self, other: &Self) -> bool {
        self.config_id == other.config_id && self.nodes == other.nodes
    }
}

/// Snapshot type. A `Complete` snapshot contains all snapshotted data while `Delta` has snapshotted changes since an earlier snapshot.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub enum SnapshotType<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    Complete(S),
    Delta(S),
    _Phantom(PhantomData<T>),
}

/// Functions required by Sequence Paxos to implement snapshot operations for `T`. If snapshot is not desired to be used, use the unit type `()` as the Snapshot parameter in `SequencePaxos`.
pub trait Snapshot<T>: Clone
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

#[allow(missing_docs)]
pub mod persistent_storage {
    use commitlog::{
        message::{MessageSet, MessageBuf}, CommitLog, LogOptions, ReadLimit,
    };
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSignEntry, Storage},
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
        /// struct for accessing local RocksDB
        db: DB,
        /// path to the rocksDB storage (needed to close)
        path: String,

        //todo: replace varaibles with above storage
        // /// Vector which contains all the logged entries in-memory.
        log: Vec<T>,
        // /// Last promised round.
        // n_prom: Ballot,
        // /// Last accepted round.
        // acc_round: Ballot,
        // /// Length of the decided log.
        // ld: u64,
        /// Garbage collected index.
        trimmed_idx: u64,
        /// Stored snapshot
        snapshot: Option<S>,
        /// Stored StopSign
        stopsign: Option<StopSignEntry>,
    }

    impl<T: Entry, S: Snapshot<T>> PersistentState<T, S> {
        pub fn with(replica_id: &str) -> Self {
            // initialize a commit log for entries
            let opts = LogOptions::new("commitlog");
            let c_log = CommitLog::new(opts).unwrap();

            // create a path and options for DB
            let path = "rocksDB/".to_string() + &replica_id.to_string();
            let mut opts = Options::default();
            //let lru = rocksdb::Cache::new_lru_cache(1200 as usize).unwrap();
            //opts.set_row_cache(&lru);
            opts.increase_parallelism(2);
            opts.set_max_write_buffer_number(16);
            opts.create_if_missing(true);

            // create DB
            let db = DB::open(&opts, &path).unwrap();

            // return the struct
            Self {
                c_log: c_log,
                db: db,
                path: path,
                log: vec![],
                // n_prom: Ballot::default(),
                // acc_round: Ballot::default(),
                // ld: 0,
                trimmed_idx: 0,
                snapshot: None,
                stopsign: None,
            }
        }

        // Todo: a function for destroying the database in the given path, also flushes the commitlog. 
        pub fn close_db(&mut self) {
            let _ = DB::destroy(&Options::default(), &self.path);
            self.c_log.flush();
        }
    }

    impl<T, S> Storage<T, S> for PersistentState<T, S>
    where
        T: Entry + zerocopy::AsBytes + zerocopy::FromBytes,
        S: Snapshot<T>,
    {
        fn append_entry(&mut self, entry: T) -> u64 {
            // self.log.push(entry);
            // self.get_log_len()
            println!("entry from append_entry {:?}", entry);
            let entry_bytes = AsBytes::as_bytes(&entry);
            let offset = self.c_log.append_msg(entry_bytes);
            match offset {
                Err(_e) => 0,
                Ok(x) => {
                    println!("offset from append_entry {:?}", x);
                    x
                },
            }
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            // let mut e = entries;
            // self.log.append(&mut e);
            // self.get_log_len()
            println!("append_Entries!");

            for e in entries {
                self.append_entry(e);
            }
            self.c_log.next_offset()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            // self.log.truncate(from_idx as usize);
            // self.append_entries(entries)
            println!("append_on_prefix!");

            let _ = self.c_log.truncate(from_idx);
            let offset = self.append_entries(entries);
            offset
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            //self.log.get(from as usize..to as usize).unwrap_or(&[])

            // let res = self.c_log.read(from, ReadLimit::max_bytes(to as usize)).unwrap_or(MessageBuf::default()).iter()
            // .map(|msg| {FromBytes::read_from(msg.payload()).unwrap()}).collect();
            // println!("result from get_entries {:?}", res);
            // res
           

            //another, longer variant
            println!("get_entries FROM and TO: {:?} -> {:?} ", from, to);
            let buffer = self.c_log.read(from, ReadLimit::default()).unwrap(); 
            let testread = ReadLimit::max_bytes(to as usize);
            println!("READLIMIT to: {:?}", testread);
            println!("BUFFER: {:?}", testread);

            let mut entries = vec![];
            for msg in buffer.iter() {
                    let temp = FromBytes::read_from(msg.payload()).unwrap();
                    entries.push(temp);
            }
            println!("res from get_entries {:?}", entries);
            entries
            
            //old - did not work becuase arrray is fixed
            // let entries: &mut [T] = &mut [];
            // for (idx, msg) in buffer.iter().enumerate() {
            //     let temp = FromBytes::read_from(msg.payload()).unwrap();
            //     entries[idx] = temp;
            // }
            
        }

        fn get_log_len(&self) -> u64 {
            //self.log.len() as u64

            let res = self.c_log.next_offset();
            println!("log_len from get_log_len {:?}", res);
            res
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            let max: u64 = self.get_log_len();
            println!("get_suffix!");
            self.get_entries(from, max)

            // match self.log.get(from as usize..) {
            //     Some(s) => s,
            //     None => &[],
            // }
        }

        /// Last promised round.
        fn get_promise(&self) -> Ballot {
            match self.db.get(b"n_prom") {
                Ok(Some(value)) => {
                    let mut value = value;
                    let slice: &mut [u8] = &mut value;
                    let opt: Ballot = FromBytes::read_from(slice).unwrap();
                    opt
                }
                Ok(None) => Ballot::default(), // should never happen
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
                Ok(Some(x)) => {
                    let c: &[u8] = &x;
                    let opt: u64 = FromBytes::read_from(c).unwrap();
                    opt
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
                    let mut value = value;
                    let slice: &mut [u8] = &mut value;
                    let opt: Ballot = FromBytes::read_from(slice).unwrap();
                    opt
                }
                Ok(None) => Ballot::default(), // should never happen
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

        //todo adjust to the current type of log
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
            self.log.get(from as usize..to as usize).unwrap_or(&[]).to_vec() // todo vec temp
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

impl<T: Entry> Snapshot<T> for () {
    fn create(_: &[T]) -> Self {
        unimplemented!()
    }

    fn merge(&mut self, _: Self) {
        unimplemented!()
    }

    fn use_snapshots() -> bool {
        false
    }
}
