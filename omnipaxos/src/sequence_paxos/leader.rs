use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseData, PromiseMetaData},
};
use crate::storage::{RollbackValue, Snapshot, SnapshotType};

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }
        if self.stopped() {
            self.pending_proposals.clear();
        }
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, None, self.leader_state.max_pid, self.leader_state.quorum);
            self.leader = n;
            self.internal_storage
                .flush_batch()
                .expect("storage error while trying to flush batch");
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_snapshot: None,
                decided_idx,
                accepted_idx,
                suffix: vec![],
                stopsign: self.get_stopsign(),
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            self.internal_storage
                .set_promise(n)
                .expect("storage error while trying to write promise");
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                });
            }
        } else {
            self.state.0 = Role::Follower;
        }
    }

    pub(crate) fn handle_preparereq(&mut self, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader {
            self.leader_state.set_decided_idx(from, None);
            #[cfg(feature = "batch_accept")]
            {
                self.leader_state.set_batch_accept_meta(from, None);
            }
            self.send_prepare(from);
        }
    }

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        if self.leader.pid > 0 && self.leader.pid != self.pid {
            #[cfg(feature = "logging")]
            trace!(
                self.logger,
                "Forwarding proposal to Leader {:?}",
                self.leader
            );
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = PaxosMessage {
                from: self.pid,
                to: self.leader.pid,
                msg: pf,
            };
            self.outgoing.push(msg);
        } else {
            self.pending_proposals.append(&mut entries);
        }
    }

    pub(crate) fn forward_stopsign(&mut self, ss: StopSign) {
        if self.leader.pid > 0 && self.leader.pid != self.pid {
            #[cfg(feature = "logging")]
            trace!(
                self.logger,
                "Forwarding StopSign to Leader {:?}",
                self.leader
            );
            let fs = PaxosMsg::ForwardStopSign(ss);
            let msg = PaxosMessage {
                from: self.pid,
                to: self.leader.pid,
                msg: fs,
            };
            self.outgoing.push(msg);
        } else if self.pending_stopsign.as_mut().is_none() {
            self.pending_stopsign = Some(ss);
        }
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        if !self.stopped() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.pending_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.accept_entries_leader(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if !self.stopped() {
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    if self.pending_stopsign.as_mut().is_none() {
                        self.pending_stopsign = Some(ss);
                    }
                }
                (Role::Leader, Phase::Accept) => {
                    if self.pending_stopsign.is_none() {
                        self.accept_stopsign(ss.clone());
                        for pid in self.leader_state.get_promised_followers() {
                            self.send_accept_stopsign(pid, ss.clone(), false);
                        }
                    }
                }
                _ => self.forward_stopsign(ss),
            }
        }
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        });
    }

    #[cfg(feature = "batch_accept")]
    fn send_accept_and_cache(&mut self, to: NodeId, entries: Vec<T>) {
        let acc = AcceptDecide {
            n: self.leader_state.n_leader,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.internal_storage.get_decided_idx(),
            entries,
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptDecide(acc),
        });
        self.leader_state
            .set_batch_accept_meta(to, Some(self.outgoing.len() - 1));
    }

    pub(crate) fn accept_entry(&mut self, entry: T) {
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(entry)
            .expect("storage error while trying to write an entry");
        if let Some(am) = accepted_metadata {
            self.send_acceptdecide(am);
        }
    }

    fn accept_entries_leader(&mut self, entries: Vec<T>) {
        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(entries)
            .expect("storage error while trying to write entries");
        if let Some(am) = accepted_metadata {
            self.send_acceptdecide(am);
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let my_decided_idx = self.get_decided_idx();
        let PromiseMetaData {
            n: max_promise_n,
            accepted_idx: max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n: promise_n,
            accepted_idx: promise_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let follower_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        let (delta_snapshot, suffix, sync_idx) =
            if (promise_n == max_promise_n) && (promise_accepted_idx < max_accepted_idx) {
                if self.internal_storage.get_compacted_idx() > *promise_accepted_idx
                    && T::Snapshot::use_snapshots()
                {
                    let delta_snapshot = self
                        .internal_storage
                        .create_diff_snapshot(follower_decided_idx, my_decided_idx)
                        .expect("storage error while trying to read diff snapshot");
                    let suffix = self
                        .internal_storage
                        .get_suffix(my_decided_idx)
                        .expect("storage error while trying to read log suffix");
                    (Some(delta_snapshot), suffix, follower_decided_idx)
                } else {
                    let sfx = self
                        .internal_storage
                        .get_suffix(*promise_accepted_idx)
                        .expect("storage error while trying to read log suffix");
                    (None, sfx, *promise_accepted_idx)
                }
            } else if follower_decided_idx < my_decided_idx && T::Snapshot::use_snapshots() {
                let delta_snapshot = self
                    .internal_storage
                    .create_diff_snapshot(follower_decided_idx, my_decided_idx)
                    .expect("storage error while trying to read diff snapshot");
                let suffix = self
                    .internal_storage
                    .get_suffix(my_decided_idx)
                    .expect("storage error while trying to read log suffix");
                (Some(delta_snapshot), suffix, follower_decided_idx)
            } else {
                let suffix = self
                    .internal_storage
                    .get_suffix(follower_decided_idx)
                    .expect("storage error while trying to read log suffix");
                (None, suffix, follower_decided_idx)
            };
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: self.leader_state.n_leader,
            seq_num: self.leader_state.next_seq_num(to),
            decided_snapshot: delta_snapshot,
            suffix,
            sync_idx,
            decided_idx: my_decided_idx,
            stopsign: self.get_stopsign(),
        };
        let msg = PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        };
        self.outgoing.push(msg);
    }

    pub(crate) fn send_acceptdecide(&mut self, am: AcceptedMetaData<T>) {
        let AcceptedMetaData {
            accepted_idx,
            flushed_entries,
        } = am;
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        let decided_idx = self.internal_storage.get_decided_idx();
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                #[cfg(feature = "batch_accept")]
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let PaxosMessage { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => {
                                a.entries.append(flushed_entries.clone().as_mut());
                                a.decided_idx = decided_idx;
                            }
                            _ => self.send_accept_and_cache(pid, flushed_entries.clone()),
                        }
                    }
                    _ => self.send_accept_and_cache(pid, flushed_entries.clone()),
                }
            } else {
                let acc = AcceptDecide {
                    n: self.leader_state.n_leader,
                    seq_num: self.leader_state.next_seq_num(pid),
                    decided_idx,
                    entries: flushed_entries.clone(),
                };
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: pid,
                    msg: PaxosMsg::AcceptDecide(acc),
                });
            }
        }
    }

    fn send_decide(&mut self, to: NodeId, decided_idx: u64) {
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx,
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        });
    }

    fn send_decide_stopsign(&mut self, to: NodeId) {
        let d = DecideStopSign {
            seq_num: self.leader_state.next_seq_num(to),
            n: self.leader_state.n_leader,
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::DecideStopSign(d),
        });
    }

    fn adopt_pending_stopsign(&mut self) {
        if let Some(ss) = self.pending_stopsign.take() {
            self.accept_stopsign(ss);
        }
    }

    fn append_pending_proposals(&mut self) {
        if !self.pending_proposals.is_empty() {
            let new_entries = std::mem::take(&mut self.pending_proposals);
            // append new proposals in my sequence
            let append_res = self
                .internal_storage
                .append_entries_and_get_accepted_idx(new_entries)
                .expect("storage error while trying to write log entries");
            if let Some(accepted_idx) = append_res {
                self.leader_state.set_accepted_idx(self.pid, accepted_idx);
            }
        }
    }

    // Correctness: This function performs multiple operations that cannot be rolled
    // back, so instead it relies on writing in a "safe" order for correctness.
    fn handle_majority_promises(&mut self) {
        self.state = (Role::Leader, Phase::Accept);
        let max_stopsign = self.leader_state.take_max_promise_stopsign();
        let max_promise = self.leader_state.take_max_promise();
        let max_promise_meta = self.leader_state.get_max_promise_meta();
        let decided_idx = self
            .leader_state
            .decided_indexes
            .iter()
            .max()
            .unwrap()
            .unwrap();
        let old_decided_idx = self.internal_storage.get_decided_idx();
        let old_accepted_round = self.internal_storage.get_accepted_round();
        self.internal_storage
            .set_accepted_round(self.leader_state.n_leader)
            .expect("storage error while trying to write accepted round");
        let result = self.internal_storage.set_decided_idx(decided_idx);
        self.internal_storage.rollback_and_panic_if_err(
            &result,
            vec![RollbackValue::AcceptedRound(old_accepted_round)],
            "storage error while trying to write decided index",
        );
        match max_promise {
            Some(PromiseData {
                decided_snapshot,
                suffix,
            }) => {
                match decided_snapshot {
                    Some(s) => {
                        let old_compacted_idx = self.internal_storage.get_compacted_idx();
                        let old_log_res = self.internal_storage.get_suffix(old_compacted_idx);
                        let old_snapshot_res = self.internal_storage.get_snapshot();
                        if old_log_res.is_err() || old_snapshot_res.is_err() {
                            self.internal_storage.rollback_and_panic(
                                vec![
                                    RollbackValue::AcceptedRound(old_accepted_round),
                                    RollbackValue::DecidedIdx(old_decided_idx),
                                ],
                                "storage error while trying to read old log or snapshot",
                            );
                        }
                        let decided_idx = self
                            .leader_state
                            .get_decided_idx(max_promise_meta.pid)
                            .unwrap();
                        let snapshot_result = match s {
                            SnapshotType::Complete(c) => {
                                self.internal_storage.set_snapshot(decided_idx, c)
                            }
                            SnapshotType::Delta(d) => {
                                self.internal_storage.merge_snapshot(decided_idx, d)
                            }
                        };
                        self.internal_storage.rollback_and_panic_if_err(
                            &snapshot_result,
                            vec![
                                RollbackValue::AcceptedRound(old_accepted_round),
                                RollbackValue::DecidedIdx(old_decided_idx),
                            ],
                            "storage error while trying to write snapshot",
                        );
                        let accepted_res = self
                            .internal_storage
                            .append_entries_without_batching(suffix);
                        // manually rollback snapshot and log if append suffix fails
                        self.internal_storage.rollback_and_panic_if_err(
                            &accepted_res,
                            vec![
                                RollbackValue::AcceptedRound(old_accepted_round),
                                RollbackValue::DecidedIdx(old_decided_idx),
                                RollbackValue::Log(old_log_res.unwrap()),
                                RollbackValue::Snapshot(
                                    old_compacted_idx,
                                    old_snapshot_res.unwrap(),
                                ),
                            ],
                            "storage error while trying to write log entries",
                        );
                        if let Some(ss) = max_stopsign {
                            self.accept_stopsign(ss);
                        } else {
                            self.append_pending_proposals();
                            self.adopt_pending_stopsign();
                        }
                    }
                    None => {
                        // no snapshot, only suffix
                        let result = if max_promise_meta.n == old_accepted_round {
                            self.internal_storage
                                .append_entries_without_batching(suffix)
                        } else {
                            self.internal_storage.append_on_decided_prefix(suffix)
                        };
                        self.internal_storage.rollback_and_panic_if_err(
                            &result,
                            vec![
                                RollbackValue::AcceptedRound(old_accepted_round),
                                RollbackValue::DecidedIdx(old_decided_idx),
                            ],
                            "storage error while trying to write log entries",
                        );
                        if let Some(ss) = max_stopsign {
                            self.accept_stopsign(ss);
                        } else {
                            self.append_pending_proposals();
                            self.adopt_pending_stopsign();
                        }
                    }
                }
            }
            None => {
                // I am the most updated
                self.append_pending_proposals();
                self.adopt_pending_stopsign();
            }
        }
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx()
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            let old_decided_idx = self.internal_storage.get_decided_idx();
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > old_decided_idx
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                self.internal_storage
                    .set_decided_idx(decided_idx)
                    .expect("storage error while trying to write decided index");
                // Send Decides to followers or batch with previous AcceptDecide
                for pid in self.leader_state.get_promised_followers() {
                    if cfg!(feature = "batch_accept") {
                        #[cfg(feature = "batch_accept")]
                        match self.leader_state.get_batch_accept_meta(pid) {
                            Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                                let PaxosMessage { msg, .. } =
                                    self.outgoing.get_mut(outgoing_idx).unwrap();
                                match msg {
                                    PaxosMsg::AcceptDecide(a) => {
                                        a.decided_idx = decided_idx;
                                    }
                                    _ => self.send_decide(pid, decided_idx),
                                }
                            }
                            _ => self.send_decide(pid, decided_idx),
                        }
                    } else {
                        self.send_decide(pid, decided_idx);
                    }
                }
            }
        }
    }

    pub(crate) fn handle_accepted_stopsign(
        &mut self,
        acc_stopsign: AcceptedStopSign,
        from: NodeId,
    ) {
        if acc_stopsign.n == self.leader_state.n_leader
            && self.state == (Role::Leader, Phase::Accept)
        {
            self.leader_state.set_accepted_stopsign(from);
            if self.leader_state.is_stopsign_chosen() {
                let mut ss = self
                    .internal_storage
                    .get_stopsign()
                    .expect("storage error while trying to read stopsign")
                    .expect("No stopsign found when deciding!");
                ss.decided = true;
                let old_decided_idx = self.internal_storage.get_decided_idx();
                self.internal_storage
                    .set_decided_idx(self.internal_storage.get_accepted_idx() + 1)
                    .expect("storage error while trying to write decided index");
                let result = self.internal_storage.set_stopsign(ss);
                self.internal_storage.rollback_and_panic_if_err(
                    &result,
                    vec![RollbackValue::DecidedIdx(old_decided_idx)],
                    "storage error while trying to write stopsign",
                );
                for pid in self.leader_state.get_promised_followers() {
                    self.send_decide_stopsign(pid);
                }
            }
        }
    }
}
