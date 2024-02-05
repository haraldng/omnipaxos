use crate::{
    ballot_leader_election::Ballot,
    ithaca::util::{DataId, PendingSlot},
    messages::sequence_paxos::Promise,
    storage::Entry,
    util::{LogSync, NodeId, Quorum, SequenceNumber},
};
use std::{cmp::Ordering, collections::HashMap};
type Connected = bool;

#[derive(Debug, Clone, Default)]
/// Promise without the log update
pub(crate) struct PromiseMetaData {
    pub n_accepted: Ballot,
    pub accepted_idx: usize,
    pub decided_idx: usize,
    pub pid: NodeId,
    pub pending_slots: Vec<PendingSlot>,
    pub connected: Connected,
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
        {
            Ordering::Equal
        } else if self.n_accepted > other.n_accepted
            || (self.n_accepted == other.n_accepted && self.accepted_idx > other.accepted_idx)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
    }
}

#[derive(Debug, Clone)]
/// The promise state of a node.
pub(crate) enum PromiseState {
    /// Not promised to any leader
    NotPromised,
    /// Promised to my ballot. Used while in Prepare phase
    PreparePromised(PromiseMetaData),
    /// Promised to my ballot. Used while in Accept phase.
    AcceptPromised(Connected),
    /// Promised to a leader who's ballot is greater than mine
    PromisedHigher,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub n_leader: Ballot,
    pub(crate) promises_meta: Vec<PromiseState>,
    // the sequence number of accepts for each follower where AcceptSync has sequence number = 1
    follower_seq_nums: Vec<SequenceNumber>,
    pub accepted_indexes: Vec<usize>,
    max_promise_meta: PromiseMetaData,
    max_promise_sync: Option<LogSync<T>>,
    batch_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub max_pid: usize,
    // The number of promises needed in the prepare phase to become synced and
    // the number of accepteds needed in the accept phase to decide an entry.
    pub quorum: Quorum,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub fn with(n_leader: Ballot, max_pid: usize, quorum: Quorum) -> Self {
        Self {
            n_leader,
            promises_meta: vec![PromiseState::NotPromised; max_pid],
            follower_seq_nums: vec![SequenceNumber::default(); max_pid],
            accepted_indexes: vec![0; max_pid],
            max_promise_meta: PromiseMetaData::default(),
            max_promise_sync: None,
            batch_accept_meta: vec![None; max_pid],
            max_pid,
            quorum,
            // connections: e.votes
        }
    }

    fn pid_to_idx(pid: NodeId) -> usize {
        (pid - 1) as usize
    }

    fn idx_to_pid(idx: usize) -> NodeId {
        (idx + 1) as NodeId
    }

    pub fn use_accept_promises(&mut self) {
        self.promises_meta.iter_mut().for_each(|p| match p {
            PromiseState::PreparePromised(m) => {
                *p = PromiseState::AcceptPromised(m.connected);
            }
            _ => {}
        });
    }

    // Resets `pid`'s accept sequence to indicate they are in the next session of accepts
    pub fn increment_seq_num_session(&mut self, pid: NodeId) {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].session += 1;
        self.follower_seq_nums[idx].counter = 0;
    }

    pub fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].counter += 1;
        self.follower_seq_nums[idx]
    }

    pub fn get_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        self.follower_seq_nums[Self::pid_to_idx(pid)]
    }

    fn set_promise_maybe_check_majority(
        &mut self,
        prom: Promise<T>,
        pid: NodeId,
        connected: bool,
        check_max_prom: bool,
    ) -> bool {
        let promise_meta = PromiseMetaData {
            n_accepted: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            decided_idx: prom.decided_idx,
            pid,
            pending_slots: prom.slots,
            connected,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise_sync = prom.log_sync;
        }
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::PreparePromised(promise_meta);
        let num_promised = self
            .promises_meta
            .iter()
            .filter(|p| matches!(p, PromiseState::PreparePromised(_)))
            .count();
        self.quorum.is_prepare_quorum(num_promised)
    }

    pub fn set_promise_check_majority(
        &mut self,
        prom: Promise<T>,
        pid: NodeId,
        connected: bool,
    ) -> bool {
        self.set_promise_maybe_check_majority(prom, pid, connected, true)
    }

    pub fn set_promise(&mut self, prom: Promise<T>, pid: NodeId, connected: bool) {
        let _ = self.set_promise_maybe_check_majority(prom, pid, connected, false);
    }

    pub fn reset_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::NotPromised;
    }

    /// Node `pid` seen with ballot greater than my ballot
    pub fn lost_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::PromisedHigher;
    }

    pub fn take_max_promise_sync(&mut self) -> Option<LogSync<T>> {
        std::mem::take(&mut self.max_promise_sync)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn get_chosen_data(&self) -> Vec<DataId> {
        let mut data: HashMap<DataId, usize> = HashMap::new();
        for pm in &self.promises_meta {
            match pm {
                PromiseState::PreparePromised(p) => {
                    for ps in &p.pending_slots {
                        let data_id = ps.proposal.data_id;
                        match data.get_mut(&data_id) {
                            Some(mut count) => {
                                *count += 1;
                            }
                            None => {
                                data.insert(data_id, 1);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        data.iter()
            .filter_map(|(data_id, count)| {
                if self.quorum.is_accept_quorum(*count) {
                    Some(*data_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_max_decided_idx(&self) -> usize {
        self.promises_meta
            .iter()
            .filter_map(|p| match p {
                PromiseState::PreparePromised(m) => Some(m.decided_idx),
                _ => None,
            })
            .max()
            .unwrap_or_default()
    }

    pub fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        match &self.promises_meta[Self::pid_to_idx(pid)] {
            PromiseState::PreparePromised(metadata) => metadata,
            _ => panic!("No Metadata found for promised follower"),
        }
    }

    pub fn get_min_all_accepted_idx(&self) -> &usize {
        self.accepted_indexes
            .iter()
            .min()
            .expect("Should be all initialised to 0!")
    }

    pub fn reset_batch_accept_meta(&mut self) {
        self.batch_accept_meta = vec![None; self.max_pid];
    }

    pub fn get_promised_followers(&self) -> Vec<(NodeId, Connected)> {
        self.promises_meta
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                PromiseState::AcceptPromised(connected)
                    if idx != Self::pid_to_idx(self.n_leader.pid) =>
                {
                    Some((Self::idx_to_pid(idx), *connected))
                }
                PromiseState::PreparePromised(p) if idx != Self::pid_to_idx(self.n_leader.pid) => {
                    Some((Self::idx_to_pid(idx), p.connected))
                }
                _ => None,
            })
            .collect()
    }

    /// The pids of peers which have not promised a higher ballot than mine.
    pub fn get_preparable_peers(&self) -> Vec<NodeId> {
        self.promises_meta
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                PromiseState::NotPromised => Some((idx + 1) as NodeId),
                _ => None,
            })
            .collect()
    }

    pub fn set_batch_accept_meta(&mut self, pid: NodeId, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.batch_accept_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub fn set_accepted_idx(&mut self, pid: NodeId, idx: usize) {
        self.accepted_indexes[Self::pid_to_idx(pid)] = idx;
    }

    pub fn get_batch_accept_meta(&self, pid: NodeId) -> Option<(Ballot, usize)> {
        self.batch_accept_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }

    pub fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(Self::pid_to_idx(pid)).unwrap() {
            PromiseState::PreparePromised(metadata) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub fn get_accepted_idx(&self, pid: NodeId) -> usize {
        *self.accepted_indexes.get(Self::pid_to_idx(pid)).unwrap()
    }

    pub fn is_chosen(&self, idx: usize) -> bool {
        let num_accepted = self
            .accepted_indexes
            .iter()
            .filter(|la| **la >= idx)
            .count();
        self.quorum.is_accept_quorum(num_accepted)
    }
}
