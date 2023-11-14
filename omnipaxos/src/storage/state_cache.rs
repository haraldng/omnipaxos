use super::{internal_storage::InternalStorageConfig, Entry, StopSign};
use crate::ballot_leader_election::Ballot;
#[cfg(feature = "unicache")]
use crate::{unicache::*, util::NodeId};

/// A simple in-memory storage for simple state values of OmniPaxos.
pub(super) struct StateCache<T>
where
    T: Entry,
{
    #[cfg(feature = "unicache")]
    /// Id of this node
    pub pid: NodeId,
    /// The maximum number of entries to batch.
    pub batch_size: usize,
    /// Vector which contains all the logged entries in-memory.
    pub batched_entries: Vec<T>,
    /// Last promised round.
    pub promise: Ballot,
    /// Last accepted round.
    pub accepted_round: Ballot,
    /// Length of the decided log.
    pub decided_idx: usize,
    /// Length of the accepted log.
    pub accepted_idx: usize,
    /// Garbage collected index.
    pub compacted_idx: usize,
    /// Stopsign entry.
    pub stopsign: Option<StopSign>,
    #[cfg(feature = "unicache")]
    /// Batch of entries that are processed (i.e., maybe encoded). Only used by the leader.
    pub batched_processed_by_leader: Vec<T::EncodeResult>,
    #[cfg(feature = "unicache")]
    pub unicache: T::UniCache,
}

impl<T> StateCache<T>
where
    T: Entry,
{
    pub(super) fn new(
        config: InternalStorageConfig,
        #[cfg(feature = "unicache")] pid: NodeId,
    ) -> Self {
        StateCache {
            #[cfg(feature = "unicache")]
            pid,
            batch_size: config.batch_size,
            batched_entries: Vec::with_capacity(config.batch_size),
            promise: Ballot::default(),
            accepted_round: Ballot::default(),
            decided_idx: 0,
            accepted_idx: 0,
            compacted_idx: 0,
            stopsign: None,
            #[cfg(feature = "unicache")]
            batched_processed_by_leader: Vec::with_capacity(config.batch_size),
            #[cfg(feature = "unicache")]
            unicache: T::UniCache::new(),
        }
    }

    // Appends an entry to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    pub(super) fn append_entry(&mut self, entry: T) -> Option<Vec<T>> {
        #[cfg(feature = "unicache")]
        {
            let processed = self.unicache.try_encode(&entry);
            self.batched_processed_by_leader.push(processed);
        }
        self.batched_entries.push(entry);
        self.take_entries_if_batch_is_full()
    }

    // Appends entries to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    pub(super) fn append_entries(&mut self, entries: Vec<T>) -> Option<Vec<T>> {
        #[cfg(feature = "unicache")]
        {
            if self.promise.pid == self.pid {
                // only try encoding if we're the leader
                for entry in &entries {
                    let processed = self.unicache.try_encode(entry);
                    self.batched_processed_by_leader.push(processed);
                }
            }
        }
        self.batched_entries.extend(entries);
        self.take_entries_if_batch_is_full()
    }

    // Flushes batched entries and appends a stopsign to the log. Returns the flushed
    // entries if there were any
    pub(super) fn append_stopsign(&mut self, ss: StopSign) -> Option<Vec<T>> {
        self.stopsign = Some(ss);
        if self.batched_entries.is_empty() {
            None
        } else {
            Some(self.take_batched_entries())
        }
    }

    // Return batched entries if the batch is full that need to be flushed in to storage.
    fn take_entries_if_batch_is_full(&mut self) -> Option<Vec<T>> {
        if self.batched_entries.len() >= self.batch_size {
            Some(self.take_batched_entries())
        } else {
            None
        }
    }

    // Clears the batched entries and returns the cleared entries. If the batch is empty,
    // return an empty vector.
    pub(super) fn take_batched_entries(&mut self) -> Vec<T> {
        std::mem::take(&mut self.batched_entries)
    }

    #[cfg(feature = "unicache")]
    pub(super) fn take_batched_processed(&mut self) -> Vec<T::EncodeResult> {
        std::mem::take(&mut self.batched_processed_by_leader)
    }

    // Returns whether a stopsign is decided
    pub(super) fn stopsign_is_decided(&self) -> bool {
        self.stopsign.is_some() && self.decided_idx == self.accepted_idx
    }
}
