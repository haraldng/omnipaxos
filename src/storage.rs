use crate::leader_election::ballot_leader_election::Ballot;
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

/// An entry in the replicated log.
#[derive(Clone, Debug, PartialEq)]
pub enum Entry<T>
where
    T: Clone,
{
    /// A normal entry proposed by the client.
    Normal(T),
    /// A StopSign entry used for reconfiguration. See [`StopSign`].
    StopSign(StopSign),
}

impl<T> Entry<T>
where
    T: Clone,
{
    /// Returns true if the entry is a stopsign else, returns false.
    pub fn is_stopsign(&self) -> bool {
        matches!(self, Entry::StopSign(_))
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

/// Trait to implement a back-end for the log replicated by an Omni-Paxos replica.
pub trait Sequence<T>
where
    T: Clone,
{
    /// Creates an empty log.
    fn new() -> Self;

    /// Creates a log that is preloaded with the entries of `seq`.
    fn new_with_sequence(seq: Vec<Entry<T>>) -> Self;

    /// Appends an entry to the end of the log.
    fn append_entry(&mut self, entry: Entry<T>);

    /// Appends the entries of `seq` to the end of the log.
    fn append_sequence(&mut self, seq: &mut Vec<Entry<T>>);

    /// Appends the entries of `seq` to the prefix from index `from_index` in the log.
    fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry<T>>);

    /// Returns the entries in the log in the index interval of [from, to)
    fn get_entries(&self, from: u64, to: u64) -> &[Entry<T>];

    /// Returns the suffix of entries in the log from index `from`.
    fn get_suffix(&self, from: u64) -> Vec<Entry<T>>;

    /// Returns the current length of the log.
    fn get_sequence_len(&self) -> u64;

    /// Returns true if the log contains a StopSign or a StopSign already has been decided.
    /// Note that the log could have a StopSign that later gets overwritten, and thus this function might first return true and later false.
    fn stopped(&self) -> bool;

    /// Removes elements up to the given [`idx`] from storage.
    fn garbage_collect(&mut self, idx: u64);
}

/// Trait to implement a back-end for the internal state used by an Omni-Paxos replica.
pub trait PaxosState {
    /// Creates an empty initial state.
    fn new() -> Self;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, nprom: Ballot);

    /// Sets the decided index in the log.
    fn set_decided_len(&mut self, ld: u64);

    /// Sets the latest accepted round.
    fn set_accepted_round(&mut self, na: Ballot);

    /// Returns the latest round in which entries have been accepted.
    fn get_accepted_round(&self) -> Ballot;

    /// Returns the index in the log that has been decided up to.
    fn get_decided_len(&self) -> u64;

    /// Returns the round that has been promised.
    fn get_promise(&self) -> Ballot;

    /// Sets the garbage collected index.
    fn set_gc_idx(&mut self, index: u64);

    /// Returns the garbage collected index.
    fn get_gc_idx(&self) -> u64;
}

enum PaxosSequence<S, T>
where
    S: Sequence<T>,
    T: Clone,
{
    Active(S),
    Stopped(Arc<S>),
    None,
    _Never(PhantomData<T>),
}

/// A storage back-end to be used for Omni-Paxos.
pub(crate) struct Storage<T, S, P>
where
    T: Clone,
    S: Sequence<T>,
    P: PaxosState,
{
    sequence: PaxosSequence<S, T>,
    paxos_state: P,
}

impl<T, S, P> Storage<T, S, P>
where
    T: Clone,
    S: Sequence<T>,
    P: PaxosState,
{
    /// Creates a [`Storage`] back-end for Omni-Paxos.
    /// The storage is divided into a [`Sequence`] and [`PaxosState`] allows for the log and the state to use different implementations.
    pub fn with(seq: S, paxos_state: P) -> Storage<T, S, P> {
        let sequence = PaxosSequence::Active(seq);
        Storage {
            sequence,
            paxos_state,
        }
    }

    /// Appends an entry to the end of the log.
    pub fn append_entry(&mut self, entry: Entry<T>) -> u64 {
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                s.append_entry(entry);
                s.get_sequence_len()
            }
            PaxosSequence::Stopped(_) => {
                panic!("Sequence should not be modified after reconfiguration");
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    /// Appends the entries of `seq` to the end of the log.
    pub fn append_sequence(&mut self, seq: &mut Vec<Entry<T>>) -> u64 {
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                s.append_sequence(seq);
                s.get_sequence_len()
            }
            PaxosSequence::Stopped(_) => {
                panic!("Sequence should not be modified after reconfiguration");
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    /// Appends the entries of `seq` to the prefix from index `from_index` in the log.
    pub fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry<T>>) -> u64 {
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                s.append_on_prefix(from_idx, seq);
                s.get_sequence_len()
            }
            PaxosSequence::Stopped(s) => {
                assert!(seq.is_empty());
                s.get_sequence_len()
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    /// Appends the entries of `seq` to the decided prefix in the log.
    pub fn append_on_decided_prefix(&mut self, seq: Vec<Entry<T>>) {
        let from_idx = self.get_decided_len();
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                let mut sequence = seq;
                s.append_on_prefix(from_idx, &mut sequence);
            }
            PaxosSequence::Stopped(_) => {
                if !seq.is_empty() {
                    panic!("Sequence should not be modified after reconfiguration");
                }
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    /// Sets the round that has been promised.
    pub fn set_promise(&mut self, nprom: Ballot) {
        self.paxos_state.set_promise(nprom);
    }

    /// Sets the decided index in the log.
    pub fn set_decided_len(&mut self, ld: u64) {
        self.paxos_state.set_decided_len(ld);
    }

    /// Sets the latest accepted round.
    pub fn set_accepted_round(&mut self, na: Ballot) {
        self.paxos_state.set_accepted_round(na);
    }

    /// Returns the latest round in which entries have been accepted.
    pub fn get_accepted_round(&self) -> Ballot {
        self.paxos_state.get_accepted_round()
    }

    /// Returns the entries in the log in the index interval of [from, to)
    pub fn get_entries(&self, from: u64, to: u64) -> &[Entry<T>] {
        match &self.sequence {
            PaxosSequence::Active(s) => s.get_entries(from, to),
            PaxosSequence::Stopped(s) => s.get_entries(from, to),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_entries"),
        }
    }

    /// Returns the current length of the log.
    pub fn get_sequence_len(&self) -> u64 {
        match self.sequence {
            PaxosSequence::Active(ref s) => s.get_sequence_len(),
            PaxosSequence::Stopped(ref arc_s) => arc_s.get_sequence_len(),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_sequence_len"),
        }
    }

    /// Returns the index in the log that has been decided up to.
    pub fn get_decided_len(&self) -> u64 {
        self.paxos_state.get_decided_len()
    }

    /// Returns the suffix of entries in the log from index `from`.
    pub fn get_suffix(&self, from: u64) -> Vec<Entry<T>> {
        match self.sequence {
            PaxosSequence::Active(ref s) => s.get_suffix(from),
            PaxosSequence::Stopped(ref arc_s) => arc_s.get_suffix(from),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_suffix"),
        }
    }

    /// Returns the round that has been promised.
    pub fn get_promise(&self) -> Ballot {
        self.paxos_state.get_promise()
    }

    /// Returns true if the log contains a StopSign or a StopSign already has been decided.
    /// Note that the log could have a StopSign that later gets overwritten, and thus this function might first return true and later false.
    pub fn stopped(&self) -> bool {
        match self.sequence {
            PaxosSequence::Active(ref s) => s.stopped(),
            PaxosSequence::Stopped(_) => true,
            _ => panic!("Got unexpected intermediate PaxosSequence::None in stopped()"),
        }
    }

    /// Stops any new writes to the log and returns the whole log as an [`Arc`]. This should **only be used when a [`StopSign`]
    /// i.e. a reconfiguration has been **decided.
    pub fn stop_and_get_sequence(&mut self) -> Arc<S> {
        let a = std::mem::replace(&mut self.sequence, PaxosSequence::None);
        match a {
            PaxosSequence::Active(s) => {
                let arc_s = Arc::from(s);
                self.sequence = PaxosSequence::Stopped(arc_s.clone());
                arc_s
            }
            _ => panic!("Storage should already have been stopped!"),
        }
    }

    /// Removes elements up to the given [`idx`] from storage.
    pub fn garbage_collect(&mut self, idx: u64) {
        match self.sequence {
            PaxosSequence::Active(ref mut s) => {
                s.garbage_collect(idx - self.paxos_state.get_gc_idx());
                self.paxos_state.set_gc_idx(idx);
            }
            PaxosSequence::Stopped(_) => {} // todo what to do when paxos is stopped?
            _ => panic!("Got unexpected intermediate PaxosSequence::None in stopped()"),
        }
    }

    /// Returns the garbage collector index from storage.
    pub fn get_gc_idx(&self) -> u64 {
        self.paxos_state.get_gc_idx()
    }
}

/// An in-memory storage implementation for Paxos.
pub mod memory_storage {
    use crate::{
        leader_election::ballot_leader_election::Ballot,
        storage::{Entry, PaxosState, Sequence},
    };

    /// Stores all the accepted entries inside a vector.
    #[derive(Debug)]
    pub struct MemorySequence<T>
    where
        T: Clone,
    {
        /// Vector which contains all the logged entries in-memory.
        sequence: Vec<Entry<T>>,
    }

    impl<T> Sequence<T> for MemorySequence<T>
    where
        T: Clone,
    {
        fn new() -> Self {
            MemorySequence { sequence: vec![] }
        }

        fn new_with_sequence(seq: Vec<Entry<T>>) -> Self {
            MemorySequence { sequence: seq }
        }

        fn append_entry(&mut self, entry: Entry<T>) {
            self.sequence.push(entry);
        }

        fn append_sequence(&mut self, seq: &mut Vec<Entry<T>>) {
            self.sequence.append(seq);
        }

        fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry<T>>) {
            self.sequence.truncate(from_idx as usize);
            self.sequence.append(seq);
        }

        fn get_entries(&self, from: u64, to: u64) -> &[Entry<T>] {
            match self.sequence.get(from as usize..to as usize) {
                Some(ents) => ents,
                None => panic!(
                    "get_entries out of bounds. From: {}, To: {}, len: {}",
                    from,
                    to,
                    self.sequence.len()
                ),
            }
        }

        fn get_suffix(&self, from: u64) -> Vec<Entry<T>> {
            match self.sequence.get(from as usize..) {
                Some(s) => s.to_vec(),
                None => vec![],
            }
        }

        fn get_sequence_len(&self) -> u64 {
            self.sequence.len() as u64
        }

        fn stopped(&self) -> bool {
            match self.sequence.last() {
                Some(entry) => entry.is_stopsign(),
                None => false,
            }
        }

        fn garbage_collect(&mut self, idx: u64) {
            self.sequence.drain(0..idx as usize);
        }
    }

    /// Stores the state of a paxos replica in-memory.
    #[derive(Debug)]
    pub struct MemoryState {
        /// Last promised round.
        n_prom: Ballot,
        /// Last accepted round.
        acc_round: Ballot,
        /// Length of the decided sequence.
        ld: u64,
        /// Garbage collected index.
        gc_idx: u64,
    }

    impl PaxosState for MemoryState {
        fn new() -> Self {
            let r = Ballot::default();
            MemoryState {
                n_prom: r,
                acc_round: r,
                ld: 0,
                gc_idx: 0,
            }
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
        }

        fn set_decided_len(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_round(&self) -> Ballot {
            self.acc_round
        }

        fn get_decided_len(&self) -> u64 {
            self.ld
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }

        fn set_gc_idx(&mut self, index: u64) {
            self.gc_idx = index;
        }

        fn get_gc_idx(&self) -> u64 {
            self.gc_idx
        }
    }
}
