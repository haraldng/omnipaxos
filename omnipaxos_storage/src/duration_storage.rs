use std::time::Duration;
use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageOp, StorageResult},
};
use crate::memory_storage::MemoryStorage;

/// An in-memory storage implementation for SequencePaxos.
#[derive(Clone)]
pub struct DurationStorage<T>
    where
        T: Entry,
{
    duration: Duration,
    memory_storage: MemoryStorage<T>,
}

impl<T> Storage<T> for DurationStorage<T>
    where
        T: Entry,
{
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        std::thread::sleep(self.duration);
        self.memory_storage.write_atomically(ops)
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        std::thread::sleep(self.duration);
        self.memory_storage.append_entry(entry)
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        std::thread::sleep(self.duration);
        self.memory_storage.append_entries(entries)
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        std::thread::sleep(self.duration);
        self.memory_storage.append_on_prefix(from_idx, entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.memory_storage.set_promise(n_prom)
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.memory_storage.set_decided_idx(ld)
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        self.memory_storage.get_decided_idx()
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.memory_storage.set_accepted_round(na)
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        self.memory_storage.get_accepted_round()
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        self.memory_storage.get_entries(from, to)
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        self.memory_storage.get_log_len()
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.memory_storage.get_suffix(from)
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        self.memory_storage.get_promise()
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.memory_storage.set_stopsign(s)
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        self.memory_storage.get_stopsign()
    }

    fn trim(&mut self, idx: usize) -> StorageResult<()> {
        self.memory_storage.trim(idx)
    }

    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.memory_storage.set_compacted_idx(idx)
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        self.memory_storage.get_compacted_idx()
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.memory_storage.set_snapshot(snapshot)
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        self.memory_storage.get_snapshot()
    }
}

impl<T> DurationStorage<T>
    where
        T: Entry,
{
    /// Creates a new `DurationStorage` with the given `duration`.
    pub fn new(duration: Duration) -> Self {
        DurationStorage {
            duration,
            memory_storage: MemoryStorage::default(),
        }
    }
}
