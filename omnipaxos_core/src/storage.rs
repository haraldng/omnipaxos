use super::ballot_leader_election::Ballot;
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

/// Trait for implementing the storage backend of Sequence Paxos.
pub trait Storage<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// Appends an entry to the end of the log and returns the log length.
    fn append_entry(&mut self, entry: T) -> u64;

    /// Appends the entries of `entries` to the end of the log and returns the log length.
    fn append_entries(&mut self, entries: Vec<T>) -> u64;

    /// Appends the entries of `entries` to the prefix from index `from_index` in the log and returns the log length.
    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, n_prom: Ballot);

    /// Sets the decided index in the log.
    fn set_decided_idx(&mut self, ld: u64);

    /// Returns the decided index in the log.
    fn get_decided_idx(&self) -> u64;

    /// Sets the latest accepted round.
    fn set_accepted_round(&mut self, na: Ballot);

    /// Returns the latest round in which entries have been accepted.
    fn get_accepted_round(&self) -> Ballot;

    /// Returns the entries in the log in the index interval of [from, to)
    fn get_entries(&self, from: u64, to: u64) -> &[T];

    /// Returns the current length of the log.
    fn get_log_len(&self) -> u64;

    /// Returns the suffix of entries in the log from index `from`.
    fn get_suffix(&self, from: u64) -> &[T];

    /// Returns the round that has been promised.
    fn get_promise(&self) -> Ballot;

    /// Sets the StopSign used for reconfiguration.
    fn set_stopsign(&mut self, s: StopSignEntry);

    /// Returns the stored StopSign.
    fn get_stopsign(&self) -> Option<StopSignEntry>;

    /// Removes elements up to the given [`idx`] from storage.
    fn trim(&mut self, idx: u64);

    /// Sets the compacted (i.e. trimmed or snapshotted) index.
    fn set_compacted_idx(&mut self, idx: u64);

    /// Returns the garbage collector index from storage.
    fn get_compacted_idx(&self) -> u64;

    /// Sets the snapshot.
    fn set_snapshot(&mut self, snapshot: S);

    /// Returns the stored snapshot.
    fn get_snapshot(&self) -> Option<S>;
}

#[allow(missing_docs)]
pub mod persistent_storage {
    //use commitlog::{*, message::MessageSet};
    use rocksdb::{DB, Options, Error, DBWithThreadMode};
    use std::process;
    use crate::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSignEntry, Storage},
    };

    #[derive(Debug)]
    pub struct PersistentState<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        /// struct for accessing local RocksDB
        /// a disk-based commit log for entries
        db: DB,
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

    impl<T: Entry, S: Snapshot<T>> PersistentState<T, S> {
        pub fn with(replica_id: u32) -> Self {
            // initialize a commit log for entries
            // let opts = LogOptions::new("log");
            // let mut c_log = CommitLog::new(opts).unwrap();

            // create a path and options for DB
            let path = "rocksDB/".to_owned() + &replica_id.to_string();
            //let lru = rocksdb::Cache::new_lru_cache(1200 as usize).unwrap();
            let mut opts = Options::default();
            //opts.increase_parallelism(2);
            opts.set_max_write_buffer_number(16);
            opts.create_if_missing(true);
            //opts.set_row_cache(&lru);

            // create DB
            let db = DB::open(&opts, path).unwrap();
            //db.put(b"my key", 1.to_string()).unwrap();

            // return the struct
            Self {
                db: db,
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

    impl<T, S> Storage<T, S> for PersistentState<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        // todo: redo all 3 append get/set later
        fn append_entry(&mut self, entry: T) -> u64 {
            self.log.push(entry);
            self.get_log_len()
            
            // let offset = self.log.append_msg(entry);
            // match offset.unwrap()  {
            //     X => X + 1,
            //     AppendError => 0
            // }
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let mut e = entries;
            self.log.append(&mut e);
            self.get_log_len()
            
            // for mut e in &entries {
            //     self.log.append_msg(&mut e);
            // }
            // match self.log.last_offset() {
            //     Some(X) => X + 1,
            //     None => 0
            // }
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.log.truncate(from_idx as usize);
            self.append_entries(entries)
        }

        // todo: how to convert from buf to Vec<T>
        fn get_entries(&self, from: u64, to: u64) -> &[T] {
            self.log.get(from as usize..to as usize).unwrap_or(&[])

            //let buffer = self.log.read(from, ReadLimit::max_bytes(to as usize)).unwrap();
            //let entries = vec![];
            // for b in buffer.iter() {
            //     entries.push(b.payload())
            // }
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64

            // match self.log.last_offset() {
            //     Some(X) => X + 1,
            //     None => 0
            // }
        }

        fn get_suffix(&self, from: u64) -> &[T] {
            match self.log.get(from as usize..) {
                Some(s) => s,
                None => &[],
            }
            
            //let buff = self.log.read(from, ReadLimit::default()).unwrap();
            // let v = vec![];
            // let i = buff.iter();
        }

        /// Last promised round.
        fn get_promise(&self) -> Ballot {
            // match self.db.get("n_prom") {
            //     Ok(Some(x)) => Ballot::with(x[0].into(), x[1].into(), x[2].into()),
            //     Ok(None) => Ballot::default(),
            //     Err(_e) => todo!()
            // };
            self.n_prom
            
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
            // let promise_n = n_prom.n.to_string();
            // let promise_pid = n_prom.pid.to_string();
            // let promise_priority = n_prom.priority.to_string();
            // #[warn(unused_must_use)]
            // self.db.put("n_prom_n", promise_n).unwrap();
            // self.db.put("n_prom_pid", promise_pid).unwrap();
            // self.db.put("n_prom_priority", promise_priority).unwrap();
        }

        /// Length of the decided log.
        fn get_decided_idx(&self) -> u64 {
            // match self.db.get("ld") {
            //     Ok(Some(x)) => x[0] as u64,
            //     Ok(None) => 0,
            //     Err(_e) => todo!()
            // }
            self.ld
        }

        fn set_decided_idx(&mut self, ld: u64) {
            self.ld = ld;
            // self.db.put("ld", ld.to_string()).unwrap();
        }

        /// Last accepted round.
        fn get_accepted_round(&self) -> Ballot {
            // match self.db.get("acc_round") {
            //     Ok(Some(x)) => Ballot::with(x[0].into(), x[1].into(), x[2].into()),
            //     Ok(None) => Ballot::default(),
            //     Err(_e) => todo!()
            // }
            self.acc_round
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;

            // let na_n = na.n.to_string();
            // let na_pid = na.pid.to_string();
            // let na_priority = na.priority.to_string();
            // self.db.put("na_n", na_n).unwrap();
            // self.db.put("na_pid", na_pid).unwrap();
            // self.db.put("na_priority", na_priority).unwrap();
        } 

        //todo: check later with harald
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
}


#[allow(missing_docs)]
pub mod memory_storage {
    use crate::{
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

        fn get_entries(&self, from: u64, to: u64) -> &[T] {
            self.log.get(from as usize..to as usize).unwrap_or(&[])
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64
        }

        fn get_suffix(&self, from: u64) -> &[T] {
            match self.log.get(from as usize..) {
                Some(s) => s,
                None => &[],
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
