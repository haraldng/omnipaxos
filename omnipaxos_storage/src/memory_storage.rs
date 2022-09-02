use omnipaxos_core::{
    ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, StopSignEntry, Storage, StorageErr},
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
    fn append_entry(&mut self, entry: T) -> Result<u64, StorageErr> {
        self.log.push(entry);
        self.get_log_len()
    }

    fn append_entries(&mut self, entries: Vec<T>) -> Result<u64, StorageErr> {
        let mut e = entries;
        self.log.append(&mut e);
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> Result<u64, StorageErr> {
        self.log.truncate(from_idx as usize);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> Result<(), StorageErr> {
        self.n_prom = n_prom;
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: u64) -> Result<(), StorageErr> {
        self.ld = ld;
        Ok(())
    }

    fn get_decided_idx(&self) -> Result<u64, StorageErr> {
        Ok(self.ld)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> Result<(), StorageErr> {
        self.acc_round = na;
        Ok(())
    }

    fn get_accepted_round(&self) -> Result<Ballot, StorageErr> {
        Ok(self.acc_round)
    }

    fn get_entries(&self, from: u64, to: u64) -> Result<Vec<T>, StorageErr> {
        Ok(self
            .log
            .get(from as usize..to as usize)
            .unwrap_or(&[])
            .to_vec())
    }

    fn get_log_len(&self) -> Result<u64, StorageErr> {
        Ok(self.log.len() as u64)
    }

    fn get_suffix(&self, from: u64) -> Result<Vec<T>, StorageErr> {
        match self.log.get(from as usize..) {
            Some(s) => Ok(s.to_vec()),
            None => Ok(vec![]),
        }
    }

    fn get_promise(&self) -> Result<Ballot, StorageErr> {
        Ok(self.n_prom)
    }

    fn set_stopsign(&mut self, s: StopSignEntry) -> Result<(), StorageErr> {
        self.stopsign = Some(s);
        Ok(())
    }

    fn get_stopsign(&self) -> Result<std::option::Option<StopSignEntry>, StorageErr> {
        Ok(self.stopsign.clone())
    }

    fn trim(&mut self, trimmed_idx: u64) -> Result<(), StorageErr> {
        self.log.drain(0..trimmed_idx as usize);
        Ok(())
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) -> Result<(), StorageErr> {
        self.trimmed_idx = trimmed_idx;
        Ok(())
    }

    fn get_compacted_idx(&self) -> Result<u64, StorageErr> {
        Ok(self.trimmed_idx)
    }

    fn set_snapshot(&mut self, snapshot: S) -> Result<(), StorageErr> {
        self.snapshot = Some(snapshot);
        Ok(())
    }

    fn get_snapshot(&self) -> Result<std::option::Option<S>, StorageErr> {
        Ok(self.snapshot.clone())
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
