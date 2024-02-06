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
    pub(crate) fn replicate_data(&mut self, data_id: DataId, entry: T) {
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
                self.replicated_data.insert(
                    data_id,
                    Data {
                        data: Some(entry),
                        status: DataStatus::ReplicateAcks(0),
                    },
                );
            }
        }
    }

    fn send_to_all_peers(&mut self, pm: PaxosMsg<T>) {
        for pid in &self.peers {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: *pid,
                msg: pm.clone(),
            });
        }
    }

    fn get_next_slot_idx(&mut self) -> usize {
        let idx = self.fastpaxos_next_log_idx;
        self.fastpaxos_next_log_idx += 1;
        idx
    }

    pub fn handle_replicate(&mut self, r: Replicate<T>) {
        let proposed_slot_idx = match self.mode {
            Mode::FastPaxos => {
                let pm = PaxosMsg::Replicate(r.clone());
                self.send_to_all_peers(pm);
                Some(self.get_next_slot_idx())
            }
            Mode::OmniPaxos | Mode::PathPaxos => match self.state {
                (Role::Leader, Phase::Accept) => Some(self.get_next_slot_idx()),
                (Role::Follower, Phase::Accept) => {
                    self.forward_proposals(vec![r.data]);
                    return;
                }
                _ => {
                    self.buffered_proposals.push(r.data);
                    return;
                }
            },
            Mode::SPaxos => {
                let pm = PaxosMsg::Replicate(r.clone());
                self.send_to_all_peers(pm);
                None
            }
        };
        let Replicate { data_id, data } = r;
        self.replicate_data(data_id, data);
        let n = self.get_promise();
        let proposal = Proposal {
            n,
            data_id,
            version: 0,
        };
        let ra = ReplicateAck {
            proposal,
            proposed_slot_idx,
        };
        match self.state {
            (Role::Leader, Phase::Accept) => {
                self.handle_replicate_ack(ra);
            }
            (Role::Follower, Phase::Accept) => {
                let msg = PaxosMessage {
                    from: self.pid,
                    to: n.pid,
                    msg: PaxosMsg::ReplicateAck(ra),
                };
                self.outgoing.push(msg);
            }
            _ => {
                // self.buffered_proposals.push(r.data);
            }
        }
    }

    pub fn handle_replicate_ack(&mut self, ra: ReplicateAck) {
        let ReplicateAck {
            proposal,
            proposed_slot_idx,
        } = ra;
        if proposal.n != self.leader_state.n_leader {
            return;
        }
        match proposed_slot_idx {
            None => {
                // s-paxos
                match self.state {
                    (Role::Leader, Phase::Accept) => {
                        if !self.replicated_data.contains_key(&proposal.data_id) {
                            self.replicated_data.insert(
                                proposal.data_id,
                                Data {
                                    data: None,
                                    status: DataStatus::ReplicateAcks(0),
                                },
                            );
                        }
                        let acks = self
                            .replicated_data
                            .increment_replicate_acks(&proposal.data_id);
                        if acks == self.quorum_size {
                            let slot_idx = self.get_next_slot_idx();
                            self.slot_status
                                .insert(slot_idx, SlotStatus::SlowAcks(proposal, 1));
                            let ao = AcceptOrder {
                                proposal,
                                slot_idx,
                                data: None,
                            };
                            self.send_to_all_promised_followers(PaxosMsg::AcceptOrder(ao));
                        }
                    }
                    _ => unimplemented!(
                        "Unexpected state when handling ReplicateAck: {:?}",
                        self.state
                    ),
                }
            }
            Some(slot_idx) => {
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
                                let ao = AcceptOrder {
                                    proposal,
                                    slot_idx,
                                    data: Some(data),
                                };
                                self.slot_status
                                    .insert(slot_idx, SlotStatus::SlowAcks(proposal, 1));
                                self.send_to_all_promised_followers(PaxosMsg::AcceptOrder(ao));
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
            ProposalResult::FastPath(p) if p.n == self.leader_state.n_leader => {
                let _ = self.replicated_data.set_decided_slot(&p.data_id, slot_idx);
                let ds = DecidedSlot {
                    slot_idx,
                    data_id: p.data_id,
                };
                self.handle_decidedslot(ds);
                self.send_to_all_promised_followers(PaxosMsg::DecidedSlot(ds));
            }
            ProposalResult::SlowPath(data_id, v) => {
                let proposal = Proposal {
                    n: self.leader_state.n_leader,
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
            slot_idx: log_idx,
            data,
        } = ao;
        if proposal.n != self.leader_state.n_leader {
            return;
        }
        match self.slot_status.get(&log_idx) {
            None => {
                // first time voting
                self.slot_status
                    .insert(log_idx, SlotStatus::Voted(proposal));
                let ra = ReplicateAck {
                    proposal,
                    proposed_slot_idx: Some(log_idx),
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
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: proposal.n.pid,
                    msg: PaxosMsg::ReplicateAck(ra),
                });
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
                self.vote_slowpath(proposal, log_idx);
            }
            _ => {} // ignore
        }
    }

    fn vote_slowpath(&mut self, proposal: Proposal, log_idx: usize) {
        self.slot_status
            .insert(log_idx, SlotStatus::Voted(proposal));
        let ra = ReplicateAck {
            proposal,
            proposed_slot_idx: Some(log_idx),
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to: proposal.n.pid,
            msg: PaxosMsg::ReplicateAck(ra),
        });
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
