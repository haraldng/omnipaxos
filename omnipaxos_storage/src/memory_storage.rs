use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageResult},
};
/// An in-memory storage implementation for SequencePaxos.
#[derive(Clone)]
pub struct MemoryStorage<T>
where
    T: Entry,
{
    /// Vector which contains all the logged entries in-memory.
    log: Vec<T>,
    /// Last promised round.
    n_prom: Option<Ballot>,
    /// Last accepted round.
    acc_round: Option<Ballot>,
    /// Length of the decided log.
    ld: usize,
    /// Garbage collected index.
    trimmed_idx: usize,
    /// Stored compact index
    compacted_idx: usize,
    /// Stored snapshot
    snapshot: Option<T::Snapshot>,
    /// Stored StopSign
    stopsign: Option<StopSign>,
}

impl<T> Storage<T> for MemoryStorage<T>
where
    T: Entry,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.log.push(entry);
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let mut e = entries;
        self.log.append(&mut e);
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.log.truncate(from_idx - self.trimmed_idx);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.n_prom = Some(n_prom);
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.ld = ld;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        Ok(self.ld)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.acc_round = Some(na);
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.acc_round)
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        let from = from - self.trimmed_idx;
        let to = to - self.trimmed_idx;
        Ok(self.log.get(from..to).unwrap_or(&[]).to_vec())
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.log.len())
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        Ok(match self.log.get((from - self.trimmed_idx)..) {
            Some(s) => s.to_vec(),
            None => vec![],
        })
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.n_prom)
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.stopsign = s;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.stopsign.clone())
    }

    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let to_trim = (trimmed_idx - self.trimmed_idx).min(self.log.len());
        self.log.drain(0..to_trim);
        self.trimmed_idx = trimmed_idx;
        Ok(())
    }

    fn set_compacted_idx(&mut self, compact_idx: usize) -> StorageResult<()> {
        self.compacted_idx = compact_idx;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        Ok(self.compacted_idx)
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.snapshot = snapshot;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.snapshot.clone())
    }
}

impl<T: Entry> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self {
            log: vec![],
            n_prom: None,
            acc_round: None,
            ld: 0,
            trimmed_idx: 0,
            compacted_idx: 0,
            snapshot: None,
            stopsign: None,
        }
    }
}
