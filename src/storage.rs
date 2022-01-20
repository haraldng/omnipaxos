use crate::leader_election::ballot_leader_election::Ballot;
use std::{fmt::Debug, marker::PhantomData};
/*
/// An entry in the replicated log.
#[derive(Clone, Debug, PartialEq)]
pub enum T
where
    T: Clone,
{
    /// A normal entry proposed by the client.
    Normal(T),
    /// A StopSign entry used for reconfiguration. See [`StopSign`].
    StopSign(StopSign),
}

impl<T> T
where
    T: Clone,
{
    /// Returns true if the entry is a stopsign else, returns false.
    pub fn is_stopsign(&self) -> bool {
        matches!(self, Entry::StopSign(_))
    }
}
*/

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

#[derive(Clone, Debug)]
pub enum SnapshotType<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    Complete(S),
    Delta(S),
    _Phantom(PhantomData<T>),
}

pub trait Snapshot<T>: Clone
where
    T: Clone,
{
    fn create(entries: &[T]) -> Self;

    //fn create_delta(&self, other: Self) -> Self    // TODO create delta snapshot that can be merged() with other to become self.

    fn merge(&mut self, delta: Self);

    fn snapshottable() -> bool; // TODO: somehow check if user is using snapshots statically?

    //fn size_hint() -> u64;  // TODO: To let the system know trade-off of using entries vs snapshot?
}

// TODO create an internal storage struct that calls these user provided functions to hide logic from user e.g. stopped()
pub trait Storage<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    /// Appends an entry to the end of the log and returns the log length.
    fn append_entry(&mut self, entry: T) -> u64;

    /// Appends the entries of `entries` to the end of the log and returns the log length.
    fn append_entries(&mut self, entries: Vec<T>) -> u64;

    /// Appends the entries of `entries` to the prefix from index `from_index` in the log and returns the log length.
    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, nprom: Ballot);

    /// Sets the decided index in the log.
    fn set_decided_len(&mut self, ld: u64);

    fn get_decided_len(&self) -> u64;

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

    fn set_stopsign(&mut self, ss: StopSign);

    fn get_stopsign(&self) -> Option<StopSign>;

    /// Removes elements up to the given [`idx`] from storage.
    fn trim(&mut self, trimmed_idx: u64);

    fn set_trimmed_idx(&mut self, trimmed_idx: u64);

    /// Returns the garbage collector index from storage.
    fn get_trimmed_idx(&self) -> u64;

    fn set_snapshot(&mut self, trimmed_idx: u64, snapshot: S) -> Result<(), ()>;

    fn merge_snapshot(&mut self, trimmed_idx: u64, delta_snapshot: S);

    fn get_snapshot(&self) -> Option<(u64, S)>;
}
/*
/// An in-memory storage implementation for Paxos.
pub mod memory_storage {
    use crate::{
        leader_election::ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, Storage},
    };

    #[derive(Clone)]
    pub struct MemoryStorage<T, S>
    where
        T: Clone,
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
        gc_idx: u64,
        // TODO index?
        snapshot: S,
    }

    impl<T, S> Storage<T, S> for MemoryStorage<T, S>
    where
        T: Clone,
        S: Snapshot<T>,
    {
        fn append_entry(&mut self, entry: T) -> u64 {
            self.log.push(entry);
            self.get_decided_len()
        }

        fn append_entries(&mut self, entries: &mut Vec<T>) -> u64 {
            self.log.append(entries);
            self.get_decided_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: &mut Vec<T>) -> u64 {
            self.log.truncate(from_idx as usize);
            self.log.append(entries);
            self.get_decided_len()
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
        }

        fn set_decided_len(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn get_decided_len(&self) -> u64 {
            self.ld
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_round(&self) -> Ballot {
            self.acc_round
        }

        fn get_entries(&self, from: u64, to: u64) -> &[T] {
            match self.log.get(from as usize..to as usize) {
                Some(ents) => ents,
                None => panic!(
                    "get_entries out of bounds. From: {}, To: {}, len: {}",
                    from,
                    to,
                    self.log.len()
                ),
            }
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

        fn stopped(&self) -> bool {
            todo!()
        }

        fn trim(&mut self, idx: u64) {
            todo!()
        }

        fn get_trim_idx(&self) -> u64 {
            todo!()
        }

        fn set_snapshot(&mut self, snapshot: S) -> Result<(), ()> {
            todo!()
        }

        fn get_snapshot(&self) -> Option<S> {
            todo!()
        }
    }
}
*/
