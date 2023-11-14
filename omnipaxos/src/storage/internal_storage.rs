use super::state_cache::StateCache;
use crate::{
    ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, SnapshotType, StopSign, Storage, StorageOp, StorageResult},
    util::{AcceptedMetaData, IndexEntry, LogEntry, LogSync, SnapshottedEntry},
    CompactionErr,
};
#[cfg(feature = "unicache")]
use crate::{unicache::*, util::NodeId};
use std::{
    cmp::Ordering,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

pub(crate) struct InternalStorageConfig {
    pub(crate) batch_size: usize,
}

/// Internal representation of storage. Serves as the interface between Sequence Paxos and the
/// storage back-end.
pub(crate) struct InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    storage: I,
    state_cache: StateCache<T>,
    _t: PhantomData<T>,
}

impl<I, T> InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    pub(crate) fn with(
        storage: I,
        config: InternalStorageConfig,
        #[cfg(feature = "unicache")] pid: NodeId,
    ) -> Self {
        let mut internal_store = InternalStorage {
            storage,
            state_cache: StateCache::new(
                config,
                #[cfg(feature = "unicache")]
                pid,
            ),
            _t: Default::default(),
        };
        internal_store.load_cache();
        internal_store
    }

    fn load_cache(&mut self) {
        self.state_cache.promise = self
            .storage
            .get_promise()
            .expect("Failed to load cache from storage.")
            .unwrap_or_default();
        self.state_cache.decided_idx = self.storage.get_decided_idx().unwrap();
        self.state_cache.accepted_round = self
            .storage
            .get_accepted_round()
            .unwrap()
            .unwrap_or_default();
        self.state_cache.compacted_idx = self.storage.get_compacted_idx().unwrap();
        self.state_cache.stopsign = self.storage.get_stopsign().unwrap();
        self.state_cache.accepted_idx =
            self.storage.get_log_len().unwrap() + self.state_cache.compacted_idx;
        if self.state_cache.stopsign.is_some() {
            self.state_cache.accepted_idx += 1;
        }
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub(crate) fn read_decided_suffix(
        &self,
        from_idx: usize,
    ) -> StorageResult<Option<Vec<LogEntry<T>>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx)
        } else {
            Ok(None)
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub(crate) fn read<R>(&self, r: R) -> StorageResult<Option<Vec<LogEntry<T>>>>
    where
        R: RangeBounds<usize>,
    {
        let from_idx = match r.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(e) => *e + 1,
            Bound::Unbounded => 0,
        };
        let to_idx = match r.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => self.get_accepted_idx(),
        };
        if to_idx == 0 {
            return Ok(None);
        }
        let compacted_idx = self.get_compacted_idx();
        let accepted_idx = self.get_accepted_idx();
        // use to_idx-1 when getting the entry type as to_idx is exclusive
        let to_type = match self.get_entry_type(to_idx - 1, compacted_idx, accepted_idx)? {
            Some(IndexEntry::Compacted) => {
                return Ok(Some(vec![self.create_compacted_entry(compacted_idx)?]))
            }
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        let from_type = match self.get_entry_type(from_idx, compacted_idx, accepted_idx)? {
            Some(from_type) => from_type,
            _ => return Ok(None),
        };
        match (from_type, to_type) {
            (IndexEntry::Entry, IndexEntry::Entry) => {
                Ok(Some(self.create_read_log_entries(from_idx, to_idx)?))
            }
            (IndexEntry::Entry, IndexEntry::StopSign(ss)) => {
                let mut entries = self.create_read_log_entries(from_idx, to_idx - 1)?;
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::Entry) => {
                let mut entries = Vec::with_capacity(to_idx - compacted_idx + 1);
                let compacted = self.create_compacted_entry(compacted_idx)?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries(compacted_idx, to_idx)?;
                entries.append(&mut e);
                Ok(Some(entries))
            }
            (IndexEntry::Compacted, IndexEntry::StopSign(ss)) => {
                let mut entries = Vec::with_capacity(to_idx - compacted_idx + 1);
                let compacted = self.create_compacted_entry(compacted_idx)?;
                entries.push(compacted);
                let mut e = self.create_read_log_entries(compacted_idx, to_idx - 1)?;
                entries.append(&mut e);
                entries.push(LogEntry::StopSign(ss, self.stopsign_is_decided()));
                Ok(Some(entries))
            }
            (IndexEntry::StopSign(ss), IndexEntry::StopSign(_)) => {
                Ok(Some(vec![LogEntry::StopSign(
                    ss,
                    self.stopsign_is_decided(),
                )]))
            }
            e => {
                unimplemented!("{}", format!("Unexpected read combination: {:?}", e))
            }
        }
    }

    fn get_entry_type(
        &self,
        idx: usize,
        compacted_idx: usize,
        accepted_idx: usize,
    ) -> StorageResult<Option<IndexEntry>> {
        if idx < compacted_idx {
            Ok(Some(IndexEntry::Compacted))
        } else if idx + 1 < accepted_idx {
            Ok(Some(IndexEntry::Entry))
        } else if idx + 1 == accepted_idx {
            match self.get_stopsign() {
                Some(ss) => Ok(Some(IndexEntry::StopSign(ss))),
                _ => Ok(Some(IndexEntry::Entry)),
            }
        } else {
            Ok(None)
        }
    }

    fn create_read_log_entries(&self, from: usize, to: usize) -> StorageResult<Vec<LogEntry<T>>> {
        let decided_idx = self.get_decided_idx();
        let entries = self
            .get_entries(from, to)?
            .into_iter()
            .enumerate()
            .map(|(idx, e)| {
                let log_idx = idx + from;
                if log_idx < decided_idx {
                    LogEntry::Decided(e)
                } else {
                    LogEntry::Undecided(e)
                }
            })
            .collect();
        Ok(entries)
    }

    fn create_compacted_entry(&self, compacted_idx: usize) -> StorageResult<LogEntry<T>> {
        self.storage.get_snapshot().map(|snap| match snap {
            Some(s) => LogEntry::Snapshotted(SnapshottedEntry::with(compacted_idx, s)),
            None => LogEntry::Trimmed(compacted_idx),
        })
    }

    // Append entry, if the batch size is reached, flush the batch and return the actual
    // accepted index (not including the batched entries)
    pub(crate) fn append_entry_with_batching(
        &mut self,
        entry: T,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entry(entry);
        self.flush_if_full_batch(append_res)
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index and the flushed entries. If the batch size is not reached, return None.
    pub(crate) fn append_entries_with_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entries(entries);
        self.flush_if_full_batch(append_res)
    }

    // Flushes batched entries and appends a stopsign to the log. Returns the AcceptedMetaData
    // associated with any flushed entries if there were any.
    pub(crate) fn append_stopsign(
        &mut self,
        ss: StopSign,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_stopsign(ss.clone());
        let accepted_entries_metadata = self.flush_if_full_batch(append_res)?;
        self.storage.set_stopsign(Some(ss))?;
        self.state_cache.accepted_idx += 1;
        Ok(accepted_entries_metadata)
    }

    fn flush_if_full_batch(
        &mut self,
        append_res: Option<Vec<T>>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        if let Some(flushed_entries) = append_res {
            let accepted_idx = self.append_entries_without_batching(flushed_entries.clone())?;
            Ok(Some(AcceptedMetaData {
                accepted_idx,
                #[cfg(not(feature = "unicache"))]
                entries: flushed_entries,
                #[cfg(feature = "unicache")]
                entries: self.state_cache.take_batched_processed(),
            }))
        } else {
            Ok(None)
        }
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index. If the batch size is not reached, return None.
    pub(crate) fn append_entries_and_get_accepted_idx(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<usize>> {
        let append_res = self.state_cache.append_entries(entries);
        if let Some(flushed_entries) = append_res {
            let accepted_idx = self.append_entries_without_batching(flushed_entries)?;
            Ok(Some(accepted_idx))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn decode_entries(
        &mut self,
        encoded_entries: Vec<<T as Entry>::EncodeResult>,
    ) -> Vec<T> {
        encoded_entries
            .into_iter()
            .map(|x| self.state_cache.unicache.decode(x))
            .collect()
    }

    pub(crate) fn flush_batch(&mut self) -> StorageResult<usize> {
        #[cfg(feature = "unicache")]
        {
            // clear the processed batch
            self.state_cache.batched_processed_by_leader.clear();
        }
        let flushed_entries = self.state_cache.take_batched_entries();
        self.append_entries_without_batching(flushed_entries)
    }

    // Append entries without batching, return the accepted index
    pub(crate) fn append_entries_without_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<usize> {
        let num_new_entries = entries.len();
        self.storage.append_entries(entries)?;
        self.state_cache.accepted_idx += num_new_entries;
        Ok(self.state_cache.accepted_idx)
    }

    pub(crate) fn sync_log(
        &mut self,
        accepted_round: Ballot,
        decided_idx: usize,
        log_sync: Option<LogSync<T>>,
    ) -> StorageResult<usize> {
        self.state_cache.accepted_round = accepted_round;
        self.state_cache.decided_idx = decided_idx;
        let mut sync_txn: Vec<StorageOp<T>> = vec![
            StorageOp::SetAcceptedRound(accepted_round),
            StorageOp::SetDecidedIndex(decided_idx),
        ];
        if let Some(sync) = log_sync {
            match sync.decided_snapshot {
                Some(SnapshotType::Complete(c)) => {
                    self.state_cache.compacted_idx = sync.sync_idx;
                    sync_txn.push(StorageOp::Trim(sync.sync_idx));
                    sync_txn.push(StorageOp::SetCompactedIdx(sync.sync_idx));
                    sync_txn.push(StorageOp::SetSnapshot(Some(c)));
                }
                Some(SnapshotType::Delta(d)) => {
                    let mut snapshot = self.create_decided_snapshot()?;
                    snapshot.merge(d);
                    self.state_cache.compacted_idx = sync.sync_idx;
                    sync_txn.push(StorageOp::Trim(sync.sync_idx));
                    sync_txn.push(StorageOp::SetCompactedIdx(sync.sync_idx));
                    sync_txn.push(StorageOp::SetSnapshot(Some(snapshot)));
                }
                None => (),
            }
            self.state_cache.accepted_idx = sync.sync_idx + sync.suffix.len();
            sync_txn.push(StorageOp::AppendOnPrefix(sync.sync_idx, sync.suffix));
            match sync.stopsign {
                Some(ss) => {
                    self.state_cache.stopsign = Some(ss.clone());
                    self.state_cache.accepted_idx += 1;
                    sync_txn.push(StorageOp::SetStopsign(Some(ss)));
                }
                None if self.state_cache.stopsign.is_some() => {
                    self.state_cache.stopsign = None;
                    sync_txn.push(StorageOp::SetStopsign(None));
                }
                None => (),
            }
        }
        self.storage.write_atomically(sync_txn)?;
        Ok(self.state_cache.accepted_idx)
    }

    fn create_decided_snapshot(&mut self) -> StorageResult<T::Snapshot> {
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        self.create_snapshot(log_decided_idx)
    }

    pub(crate) fn create_snapshot(&self, compact_idx: usize) -> StorageResult<T::Snapshot> {
        let current_compacted_idx = self.get_compacted_idx();
        if compact_idx < current_compacted_idx {
            Err(CompactionErr::TrimmedIndex(current_compacted_idx))?
        }
        let entries = self
            .storage
            .get_entries(current_compacted_idx, compact_idx)?;
        let delta = T::Snapshot::create(entries.as_slice());
        match self.storage.get_snapshot()? {
            Some(mut s) => {
                s.merge(delta);
                Ok(s)
            }
            None => Ok(delta),
        }
    }

    // Creates a Delta snapshot of entries from `from_idx` to the end of the decided log and also
    // returns the compacted idx of the created snapshot. If the range of entries contains entries
    // which have already been compacted a valid delta cannot be created, so creates a Complete
    // snapshot of the entire decided log instead.
    pub(crate) fn create_diff_snapshot(
        &self,
        from_idx: usize,
    ) -> StorageResult<(Option<SnapshotType<T>>, usize)> {
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let compacted_idx = self.get_compacted_idx();
        let snapshot = if from_idx <= compacted_idx {
            // Some entries in range are compacted, snapshot entire decided log
            if compacted_idx < log_decided_idx {
                Some(SnapshotType::Complete(
                    self.create_snapshot(log_decided_idx)?,
                ))
            } else {
                // Entire decided log already snapshotted
                self.get_snapshot()?.map(|s| SnapshotType::Complete(s))
            }
        } else {
            let diff_entries = self.get_entries(from_idx, log_decided_idx)?;
            Some(SnapshotType::Delta(T::Snapshot::create(
                diff_entries.as_slice(),
            )))
        };
        Ok((snapshot, log_decided_idx))
    }

    pub(crate) fn try_trim(&mut self, idx: usize) -> StorageResult<()> {
        let decided_idx = self.get_decided_idx();
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let new_compacted_idx = match idx.cmp(&decided_idx) {
            Ordering::Less => idx,
            Ordering::Equal => log_decided_idx,
            Ordering::Greater => Err(CompactionErr::UndecidedIndex(decided_idx))?,
        };
        if new_compacted_idx > self.get_compacted_idx() {
            self.storage.write_atomically(vec![
                StorageOp::Trim(new_compacted_idx),
                StorageOp::SetCompactedIdx(new_compacted_idx),
            ])?;
            self.state_cache.compacted_idx = new_compacted_idx;
        }
        Ok(())
    }

    pub(crate) fn try_snapshot(&mut self, snapshot_idx: Option<usize>) -> StorageResult<()> {
        let decided_idx = self.get_decided_idx();
        let log_decided_idx = self.get_decided_idx_without_stopsign();
        let new_compacted_idx = match snapshot_idx {
            Some(i) => match i.cmp(&decided_idx) {
                Ordering::Less => i,
                Ordering::Equal => log_decided_idx,
                Ordering::Greater => Err(CompactionErr::UndecidedIndex(decided_idx))?,
            },
            None => log_decided_idx,
        };
        if new_compacted_idx > self.get_compacted_idx() {
            let snapshot = self.create_snapshot(new_compacted_idx)?;
            self.storage.write_atomically(vec![
                StorageOp::Trim(new_compacted_idx),
                StorageOp::SetCompactedIdx(new_compacted_idx),
                StorageOp::SetSnapshot(Some(snapshot)),
            ])?;
            self.state_cache.compacted_idx = new_compacted_idx;
        }
        Ok(())
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.state_cache.promise = n_prom;
        self.storage.set_promise(n_prom)
    }

    pub(crate) fn set_decided_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.state_cache.decided_idx = idx;
        self.storage.set_decided_idx(idx)
    }

    pub(crate) fn get_decided_idx(&self) -> usize {
        self.state_cache.decided_idx
    }

    fn get_decided_idx_without_stopsign(&self) -> usize {
        match self.stopsign_is_decided() {
            true => self.get_decided_idx() - 1,
            false => self.get_decided_idx(),
        }
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.state_cache.accepted_round
    }

    pub(crate) fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        self.storage.get_entries(from, to)
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_accepted_idx(&self) -> usize {
        self.state_cache.accepted_idx
    }

    pub(crate) fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.storage.get_suffix(from)
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.state_cache.promise
    }

    pub(crate) fn set_stopsign(&mut self, ss: Option<StopSign>) -> StorageResult<usize> {
        if ss.is_some() && self.state_cache.stopsign.is_none() {
            self.state_cache.accepted_idx += 1;
        } else if ss.is_none() && self.state_cache.stopsign.is_some() {
            self.state_cache.accepted_idx -= 1;
        }
        self.state_cache.stopsign = ss.clone();
        self.storage.set_stopsign(ss)?;
        Ok(self.state_cache.accepted_idx)
    }

    pub(crate) fn get_stopsign(&self) -> Option<StopSign> {
        self.state_cache.stopsign.clone()
    }

    // Returns whether a stopsign is decided
    pub(crate) fn stopsign_is_decided(&self) -> bool {
        self.state_cache.stopsign_is_decided()
    }

    pub(crate) fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        self.storage.get_snapshot()
    }

    pub(crate) fn get_compacted_idx(&self) -> usize {
        self.state_cache.compacted_idx
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn get_unicache(&self) -> T::UniCache {
        self.state_cache.unicache.clone()
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn set_unicache(&mut self, unicache: T::UniCache) {
        self.state_cache.unicache = unicache;
    }
}
