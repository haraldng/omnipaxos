use crate::{ballot_leader_election::Ballot, storage::Entry, util::NodeId};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};

pub type DataId = (NodeId, usize);

#[derive(Debug, Clone)]
pub struct Data<T: Entry> {
    pub(crate) data: Option<T>,
    pub(crate) status: DataStatus,
}

#[derive(Debug, Clone)]
pub enum DataStatus {
    ReplicateAcks(usize),
    DecidedWithSlot(usize),
}

#[derive(Debug, Clone)]
pub struct ReplicatedData<T: Entry>(HashMap<DataId, Data<T>>);

impl<T: Entry> ReplicatedData<T> {
    pub fn get(&self, data_id: &DataId) -> Option<&Data<T>> {
        self.0.get(data_id)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ReplicatedData(HashMap::with_capacity(capacity))
    }

    pub fn remove_and_take_decided_data(&mut self, data_id: &DataId) -> Option<T> {
        match self.0.remove(data_id) {
            Some(Data {
                data,
                status: DataStatus::DecidedWithSlot(_),
            }) => data,
            _ => None,
        }
    }

    pub fn get_mut(&mut self, data_id: &DataId) -> Option<&mut Data<T>> {
        self.0.get_mut(data_id)
    }

    pub fn get_decided_slot_idx(&self, data_id: &DataId) -> Option<usize> {
        match self.0.get(data_id) {
            Some(Data {
                status: DataStatus::DecidedWithSlot(idx),
                ..
            }) => Some(*idx),
            _ => None,
        }
    }

    pub fn contains_key(&self, data_id: &DataId) -> bool {
        self.0.contains_key(data_id)
    }

    pub fn increment_replicate_acks(&mut self, data_id: &DataId) -> usize {
        match self.0.get_mut(data_id) {
            Some(Data {
                status: DataStatus::ReplicateAcks(count),
                ..
            }) => {
                *count += 1;
                *count
            }
            None => {
                self.0.insert(
                    *data_id,
                    Data {
                        data: None,
                        status: DataStatus::ReplicateAcks(1),
                    },
                );
                1
            }
            _ => panic!("Expected ReplicatedBy"),
        }
    }

    pub fn set_decided_slot(&mut self, data_id: &DataId, slot_idx: usize) -> bool {
        let status = DataStatus::DecidedWithSlot(slot_idx);
        match self.0.get_mut(data_id) {
            Some(x) => {
                x.status = status;
                true
            }
            None => {
                self.0.insert(*data_id, Data { data: None, status });
                false
            }
        }
    }

    pub fn insert(&mut self, data_id: DataId, data: Data<T>) {
        self.0.insert(data_id, data);
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Slots {
    pub slots: HashMap<usize, SlotStatus>,
}

impl Slots {
    pub fn insert(&mut self, idx: usize, status: SlotStatus) {
        self.slots.insert(idx, status);
    }

    pub fn get(&self, idx: &usize) -> Option<&SlotStatus> {
        self.slots.get(idx)
    }

    pub fn remove(&mut self, idx: &usize) -> Option<SlotStatus> {
        self.slots.remove(idx)
    }

    pub fn get_mut(&mut self, idx: &usize) -> Option<&mut SlotStatus> {
        self.slots.get_mut(idx)
    }

    pub fn get_max_decided_slot(&self) -> usize {
        self.slots
            .iter()
            .filter_map(|(slot_idx, x)| match x {
                SlotStatus::Decided(_) => Some(*slot_idx),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }

    pub fn get_completed_and_decided_idx<T: Entry>(
        &self,
        old_decided_idx: usize,
        replicated_data: &mut ReplicatedData<T>,
    ) -> SlotEntries<T> {
        let mut i = old_decided_idx;
        let mut completed_idx = old_decided_idx;
        let mut completed_entries = vec![];
        let mut in_completed_sequence = true;
        loop {
            match self.get(&i) {
                Some(SlotStatus::Completed(data_id)) if in_completed_sequence => {
                    let data = replicated_data
                        .remove_and_take_decided_data(data_id)
                        .unwrap_or_else(|| {
                            panic!(
                                "Expected data with data_id: {:?}, replicated_data: {:?}",
                                data_id, replicated_data
                            )
                        });
                    completed_entries.push(data);
                    i += 1;
                    completed_idx = i; //
                }
                Some(SlotStatus::Decided(_)) => {
                    in_completed_sequence = false;
                    i += 1;
                }
                _ => {
                    break;
                }
            }
        }
        SlotEntries {
            completed_entries,
            completed_idx,
            decided_idx: i,
        }
    }

    pub fn get_pending_slots(&self) -> Vec<PendingSlot> {
        self.slots
            .iter()
            .filter_map(|(idx, s)| match s {
                SlotStatus::SlowAcks(p, _) => Some(PendingSlot {
                    idx: *idx,
                    proposal: *p,
                    decided: false,
                }),
                SlotStatus::FastVotes(ps) => {
                    let p = ps.0.iter().max().unwrap();
                    Some(PendingSlot {
                        idx: *idx,
                        proposal: *p,
                        decided: false,
                    })
                }
                SlotStatus::Voted(p) => Some(PendingSlot {
                    idx: *idx,
                    proposal: *p,
                    decided: false,
                }),
                SlotStatus::Decided(data_id) => {
                    let p = Proposal {
                        data_id: *data_id,
                        n: Ballot::default(),
                        version: 0,
                    };
                    Some(PendingSlot {
                        idx: *idx,
                        proposal: p,
                        decided: true,
                    })
                }
                _ => None,
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub enum SlotStatus {
    Voted(Proposal),
    FastVotes(Proposals),
    SlowAcks(Proposal, usize), // slow path
    Decided(DataId),
    Completed(DataId),
    Recovery(Proposals),
}

#[derive(Copy, Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PendingSlot {
    pub idx: usize,
    pub proposal: Proposal,
    pub decided: bool,
}

#[derive(Copy, Clone, Debug, Ord, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Proposal {
    pub n: Ballot,
    pub version: usize,
    pub data_id: DataId,
}

impl PartialEq<Self> for Proposal {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n && self.version == other.version && self.data_id == other.data_id
    }
}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n == other.n && self.version == other.version {
            Ordering::Equal
        } else if self.n > other.n || (self.n == other.n && self.version > other.version) {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

#[derive(Debug, Clone)]
pub struct Proposals(pub Vec<Proposal>);

impl Proposals {
    pub fn add_proposal(&mut self, p: Proposal) {
        self.0.push(p);
    }

    pub fn check_result(&self, quorum: usize, super_quorum: usize) -> ProposalResult {
        let unanimous_proposals = self.0.iter().all(|&x| x == self.0[0]);
        if self.0.len() == quorum && !unanimous_proposals {
            let max = self.0.iter().max().unwrap();
            ProposalResult::SlowPath(max.data_id, max.version)
        } else if self.0.len() == super_quorum {
            if unanimous_proposals {
                ProposalResult::FastPath(self.0[0])
            } else {
                let max = self.0.iter().max().unwrap();
                ProposalResult::SlowPath(max.data_id, max.version)
            }
        } else {
            ProposalResult::Pending
        }
    }

    pub fn get_recovery_result(&self) -> DataId {
        let mut max_proposal = self.0.first().unwrap();
        let mut max_proposal_count = self.0.iter().filter(|x| x == &max_proposal).count();
        for p in &self.0 {
            if (p.n, p.version) >= (max_proposal.n, max_proposal.version) {
                let count = self.0.iter().filter(|x| x == &p).count();
                if count > max_proposal_count {
                    max_proposal = p;
                    max_proposal_count = count;
                }
            }
        }
        max_proposal.data_id
    }
}

#[derive(Debug, Clone)]
pub struct SlotEntries<T> {
    pub(crate) completed_entries: Vec<T>,
    pub(crate) completed_idx: usize,
    pub(crate) decided_idx: usize,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Mode {
    FastPaxos,
    SPaxos,
    OmniPaxos,
    PathPaxos,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ProposalResult {
    Pending,
    FastPath(Proposal),
    SlowPath(DataId, usize), // DataId and version
}
