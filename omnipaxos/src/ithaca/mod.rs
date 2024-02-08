use crate::{
    ithaca::util::*,
    messages::sequence_paxos::*,
    sequence_paxos::{
        leader::state::{LeaderState, PromiseState},
        *,
    },
    storage::{Entry, Storage},
    util::WRITE_ERROR_MSG,
};
use std::collections::HashMap;

pub(crate) mod leader_election;
pub mod util;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) fn replicate_data(&mut self, data_id: DataId, entry: T, slot_vote: SlotVote) {
        match self.replicated_data.get_mut(&data_id) {
            Some(d) => {
                let Data { data, status } = d;
                if data.is_none() {
                    *data = Some(entry);
                }
                match status {
                    DataStatus::DecidedWithSlot(slot_idx) => {
                        // #[cfg(feature = "logging")]
                        // info!(self.logger, "Completed slot: {}", slot_idx);
                        self.slot_status
                            .insert(*slot_idx, SlotStatus::Completed(data_id));
                        return;
                    }
                    _ => {
                        // ignore
                        return;
                    }
                }
            }
            None => {
                let status = match self.state.0 {
                    Role::Leader => DataStatus::ReplicateAcks(0, Some(slot_vote.into())),
                    Role::Follower => DataStatus::Acked,
                };
                self.replicated_data.insert(
                    data_id,
                    Data {
                        data: Some(entry),
                        status,
                    },
                );
            }
        }
    }

    fn get_next_slot_idx(&mut self) -> SlotIdx {
        let idx = self.fastpaxos_next_log_idx;
        self.fastpaxos_next_log_idx += 1;
        idx
    }

    fn get_next_test_slot_idx(&mut self) -> SlotIdx {
        let idx = self.testvoter_next_log_idx;
        self.testvoter_next_log_idx += 1;
        idx
    }

    fn create_slot_vote(&mut self) -> SlotVote {
        match self.mode {
            Mode::SPaxos => SlotVote::Test(self.get_next_test_slot_idx()),
            _ => SlotVote::Real(self.get_next_slot_idx()),
        }
    }

    pub fn handle_replicate(&mut self, r: Replicate<T>) {
        let slot_vote = match self.mode {
            Mode::FastPaxos => {
                let pm = PaxosMsg::Replicate(r.clone());
                self.send_to_all_peers(pm);
                self.create_slot_vote()
            }
            Mode::SPaxos => {
                let pm = PaxosMsg::Replicate(r.clone());
                self.send_to_all_peers(pm);
                self.create_slot_vote()
            }
            Mode::OmniPaxos | Mode::PathPaxos => match self.state {
                (Role::Leader, Phase::Accept) => self.create_slot_vote(),
                (Role::Follower, Phase::Accept) => {
                    self.forward_proposals(vec![r.data]);
                    return;
                }
                _ => {
                    self.buffered_proposals.push(r.data);
                    return;
                }
            },
        };
        let Replicate { data_id, data } = r;
        self.replicate_data(data_id, data, slot_vote);
        let n = self.get_promise();
        let proposal = Proposal {
            n,
            data_id,
            version: 0,
        };
        let ra = ReplicateAck {
            proposal,
            slot_vote,
        };
        if self.pid == n.pid {
            self.handle_replicate_ack(ra);
        } else {
            self.send_msg_to(n.pid, PaxosMsg::ReplicateAck(ra));
        }
    }

    fn perform_slow_path(
        &mut self,
        proposal: Proposal,
        data: Option<T>,
        slot_idx: Option<SlotIdx>,
    ) {
        let slot_idx = slot_idx.unwrap_or(self.get_next_slot_idx());
        self.slot_status
            .insert(slot_idx, SlotStatus::SlowAcks(proposal, 1));
        let ao = AcceptOrder {
            proposal,
            slot_idx,
            data,
        };
        self.send_to_all_promised_followers(PaxosMsg::AcceptOrder(ao));
    }

    pub fn handle_replicate_ack(&mut self, ra: ReplicateAck) {
        let ReplicateAck {
            proposal,
            slot_vote,
        } = ra;
        if proposal.n != self.get_promise() {
            return;
        }
        match slot_vote {
            SlotVote::Test(slot_idx) => {
                // s-paxos
                match self.state {
                    (Role::Leader, Phase::Accept) => {
                        if !self.replicated_data.contains_key(&proposal.data_id) {
                            self.replicated_data.insert(
                                proposal.data_id,
                                Data {
                                    data: None,
                                    status: DataStatus::ReplicateAcks(1, Some(slot_idx)),
                                },
                            );
                            return;
                        }
                        let acks = self
                            .replicated_data
                            .increment_replicate_acks(&proposal.data_id, slot_idx);
                        if acks == self.quorum_size {
                            self.perform_slow_path(proposal, None, None);
                        } else if acks == self.super_quorum_size {
                            match self
                                .replicated_data
                                .remove(&proposal.data_id)
                                .unwrap()
                                .status
                            {
                                DataStatus::ReplicateAcks(_, Some(_)) => {
                                    self.mode_changer.increment_fast_paths()
                                }
                                DataStatus::ReplicateAcks(_, None) => {
                                    self.mode_changer.increment_slow_paths()
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => unimplemented!(
                        "Unexpected state when handling ReplicateAck: {:?}",
                        self.state
                    ),
                }
            }
            SlotVote::Real(slot_idx) => {
                match self.slot_status.get_mut(&slot_idx) {
                    None => {
                        let slot_status = match self.mode {
                            Mode::FastPaxos => {
                                let mut proposals = Vec::with_capacity(self.super_quorum_size);
                                proposals.push(proposal);
                                let votes = Proposals(proposals);
                                SlotStatus::FastVotes(votes)
                            }
                            Mode::OmniPaxos | Mode::PathPaxos => {
                                let data = self
                                    .replicated_data
                                    .get(&proposal.data_id)
                                    .map(|d| d.data.clone().unwrap())
                                    .unwrap();
                                self.perform_slow_path(proposal, Some(data), Some(slot_idx));
                                SlotStatus::SlowAcks(proposal, 1)
                            }
                            Mode::SPaxos => unimplemented!(
                                "SPaxos should not receive ReplicateAck with proposed_slot_idx"
                            ),
                        };
                        self.slot_status.insert(slot_idx, slot_status);
                        return;
                    }
                    Some(SlotStatus::FastVotes(votes)) => {
                        votes.add_proposal(proposal);
                        let pr = votes.check_result(self.quorum_size, self.super_quorum_size);
                        self.handle_votes_result(slot_idx, pr);
                    }
                    Some(SlotStatus::SlowAcks(p, acks)) => {
                        if p == &proposal {
                            *acks += 1;
                            if &acks == &&self.quorum_size {
                                let ds = DecidedSlot {
                                    slot_idx,
                                    data_id: p.data_id,
                                };
                                self.handle_decidedslot(ds);
                                self.send_to_all_promised_followers(PaxosMsg::DecidedSlot(ds));
                                return;
                            }
                        } else if (p.n, p.version) == (proposal.n, proposal.version) {
                            // some voted in fast, some in slow meaning this was during mode switch. Treat as fast votes and it will take the slow path
                            let mut votes = Proposals(vec![p.clone(); *acks]);
                            votes.add_proposal(proposal);
                            self.slot_status
                                .insert(slot_idx, SlotStatus::FastVotes(votes));
                        } else {
                            unimplemented!("Unexpected proposal in SlotStatus::SlowAcks")
                        }
                    }
                    Some(SlotStatus::Decided(_)) | Some(SlotStatus::Completed(_)) => {
                        // ignore
                        return;
                    }
                    _ => {
                        unimplemented!(
                            "Should not receive ReplicateAck during recovery or after voting"
                        )
                    }
                }
            }
        }
    }

    fn handle_votes_result(&mut self, slot_idx: usize, pr: ProposalResult) {
        match pr {
            ProposalResult::FastPath(p) if p.n == self.get_promise() => {
                self.mode_changer.increment_fast_paths();
                let _ = self.replicated_data.set_decided_slot(&p.data_id, slot_idx);
                let ds = DecidedSlot {
                    slot_idx,
                    data_id: p.data_id,
                };
                self.handle_decidedslot(ds);
                self.send_to_all_promised_followers(PaxosMsg::DecidedSlot(ds));
            }
            ProposalResult::SlowPath(data_id, v) => {
                self.mode_changer.increment_slow_paths();
                let proposal = Proposal {
                    n: self.get_promise(),
                    data_id,
                    version: v + 1,
                };
                let ao = AcceptOrder {
                    proposal,
                    slot_idx,
                    data: None,
                };
                self.slot_status
                    .insert(slot_idx, SlotStatus::SlowAcks(proposal, 1));
                self.send_to_all_promised_followers(PaxosMsg::AcceptOrder(ao));
            }
            _ => {}
        }
    }

    pub fn handle_acceptorder(&mut self, ao: AcceptOrder<T>) {
        let AcceptOrder {
            proposal,
            slot_idx,
            data,
            ..
        } = ao;
        if proposal.n != self.get_promise() {
            return;
        }
        match self.slot_status.get(&slot_idx) {
            None => {
                // first time voting
                self.slot_status
                    .insert(slot_idx, SlotStatus::Voted(proposal));
                let ra = ReplicateAck {
                    proposal,
                    slot_vote: SlotVote::Real(slot_idx),
                };
                if !self.replicated_data.contains_key(&proposal.data_id) {
                    self.replicated_data.insert(
                        proposal.data_id,
                        Data {
                            data,
                            status: DataStatus::Acked,
                        },
                    );
                }
                self.send_msg_to(proposal.n.pid, PaxosMsg::ReplicateAck(ra));
            }
            Some(SlotStatus::Voted(p)) if p < &proposal => {
                match self.replicated_data.get_mut(&proposal.data_id) {
                    Some(d) => {
                        if d.data.is_none() {
                            d.data = data;
                        }
                    }
                    None => {
                        self.replicated_data.insert(
                            proposal.data_id,
                            Data {
                                data,
                                status: DataStatus::Acked,
                            },
                        );
                    }
                }
                self.vote_slowpath(proposal, slot_idx);
            }
            _ => {} // ignore
        }
    }

    fn vote_slowpath(&mut self, proposal: Proposal, slot_idx: SlotIdx) {
        self.slot_status
            .insert(slot_idx, SlotStatus::Voted(proposal));
        let ra = ReplicateAck {
            proposal,
            slot_vote: SlotVote::Real(slot_idx),
        };
        self.send_msg_to(proposal.n.pid, PaxosMsg::ReplicateAck(ra));
    }

    pub fn handle_decidedslot(&mut self, ds: DecidedSlot) {
        let DecidedSlot { slot_idx, data_id } = ds;
        let status = match self.replicated_data.get(&data_id) {
            Some(Data {
                data: Some(_),
                status: _,
            }) => SlotStatus::Completed(data_id),
            _ => SlotStatus::Decided(data_id),
        };
        self.slot_status.insert(slot_idx, status);
        self.replicated_data.set_decided_slot(&data_id, slot_idx);
        self.take_and_append_completed_slots();
    }

    pub fn take_and_append_completed_slots(&mut self) {
        let decided_idx = self.get_decided_idx();
        let se = self
            .slot_status
            .get_completed_idx_and_entries(decided_idx, &mut self.replicated_data);
        // #[cfg(feature = "logging")]
        // info!(self.logger, "Node {}: Decided slot {}, se: {:?}, slots: {:?}, rd: {:?}\n", self.pid, slot_idx, se, self.slot_status, self.replicated_data);
        if se.completed_idx > decided_idx {
            self.internal_storage
                .append_entries_without_batching(se.completed_entries)
                .expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_decided_idx(se.completed_idx)
                .expect(WRITE_ERROR_MSG);
        }
    }
}

impl<T: Entry> LeaderState<T> {
    /// Returns the slots that need to be appended to the log
    pub fn get_recovered_slots(&mut self) -> HashMap<usize, DataId> {
        let mut slots = HashMap::with_capacity(self.promises_meta.len());
        for ps in self.promises_meta.iter_mut() {
            match ps {
                PromiseState::PreparePromised(p) => {
                    let pending_slots = std::mem::take(&mut p.pending_slots);
                    for p in pending_slots {
                        if p.decided {
                            slots.insert(p.idx, SlotStatus::Decided(p.proposal.data_id));
                        } else {
                            match slots.get_mut(&p.idx) {
                                Some(SlotStatus::Recovery(ps)) => {
                                    ps.add_proposal(p.proposal);
                                }
                                Some(SlotStatus::Decided(_)) => {}
                                None => {
                                    let mut proposals = Vec::with_capacity(self.max_pid);
                                    proposals.push(p.proposal);
                                    let votes = Proposals(proposals);
                                    slots.insert(p.idx, SlotStatus::Recovery(votes));
                                }
                                _ => {
                                    unimplemented!()
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        slots
            .into_iter()
            .map(|(idx, s)| {
                let data_id: DataId = match s {
                    SlotStatus::Decided(data_id) => data_id,
                    SlotStatus::Recovery(proposals) => proposals.get_recovery_result(),
                    _ => unimplemented!(),
                };
                (idx, data_id)
            })
            .collect()
    }
}
