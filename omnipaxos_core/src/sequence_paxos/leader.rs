use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::storage::SnapshotType;

use super::*;

impl<T, S, B> SequencePaxos<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
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
            self.leader_state = LeaderState::with(
                n,
                None,
                self.leader_state.max_pid,
                self.leader_state.majority,
            );
            self.leader = n;
            self.internal_storage.set_promise(n);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.internal_storage.get_decided_idx();
            let accepted_idx = self.internal_storage.get_log_len();
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
                n_accepted: self.internal_storage.get_accepted_round(),
                accepted_idx,
            };
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
            let decided_idx = self.internal_storage.get_decided_idx();
            let n_accepted = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_log_len();
            let prep = Prepare {
                n: self.leader_state.n_leader,
                decided_idx,
                n_accepted,
                accepted_idx,
            };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Prepare(prep),
            });
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
                (Role::Leader, Phase::Accept) => self.send_batch_accept(entries),
                (Role::Leader, Phase::FirstAccept) => {
                    self.send_first_accept();
                    self.send_batch_accept(entries);
                }
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
                        self.send_accept_stopsign(ss);
                    }
                }
                (Role::Leader, Phase::FirstAccept) => {
                    self.send_first_accept();
                    self.accept_stopsign(ss.clone());
                    self.send_accept_stopsign(ss);
                }
                _ => self.forward_stopsign(ss),
            }
        }
    }

    pub(crate) fn send_first_accept(&mut self) {
        let f = FirstAccept {
            n: self.leader_state.n_leader,
        };
        for pid in self.leader_state.get_promised_followers() {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: PaxosMsg::FirstAccept(f.clone()),
            });
        }
        self.state.1 = Phase::Accept;
    }

    #[cfg(feature = "batch_accept")]
    fn send_accept_and_cache(&mut self, to: NodeId, entries: Vec<T>) {
        let acc = AcceptDecide {
            n: self.leader_state.n_leader,
            decided_idx: self.leader_state.get_chosen_idx(),
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

    pub(crate) fn send_accept(&mut self, entry: T) {
        let accepted_idx = self.internal_storage.append_entry(entry.clone());
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                #[cfg(feature = "batch_accept")]
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let PaxosMessage { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.push(entry.clone()),
                            _ => self.send_accept_and_cache(pid, vec![entry.clone()]),
                        }
                    }
                    _ => self.send_accept_and_cache(pid, vec![entry.clone()]),
                }
            } else {
                let acc = AcceptDecide {
                    n: self.leader_state.n_leader,
                    decided_idx: self.leader_state.get_chosen_idx(),
                    entries: vec![entry.clone()],
                };
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: pid,
                    msg: PaxosMsg::AcceptDecide(acc),
                });
            }
        }
    }

    fn send_batch_accept(&mut self, entries: Vec<T>) {
        let accepted_idx = self.internal_storage.append_entries(entries.clone());
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                #[cfg(feature = "batch_accept")]
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let PaxosMessage { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.append(entries.clone().as_mut()),
                            _ => self.send_accept_and_cache(pid, entries.clone()),
                        }
                    }
                    _ => self.send_accept_and_cache(pid, entries.clone()),
                }
            } else {
                let acc = AcceptDecide {
                    n: self.leader_state.n_leader,
                    decided_idx: self.leader_state.get_chosen_idx(),
                    entries: entries.clone(),
                };
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: pid,
                    msg: PaxosMsg::AcceptDecide(acc),
                });
            }
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
        let (delta_snapshot, suffix, sync_idx) = if (promise_n == max_promise_n)
            && (promise_accepted_idx < max_accepted_idx)
        {
            let sfx = self.internal_storage.get_suffix(*promise_accepted_idx);
            (None, sfx, *promise_accepted_idx)
        } else {
            let follower_decided_idx = self
                .leader_state
                .get_decided_idx(*pid)
                .expect("Received PromiseMetaData but not found in ld");
            if follower_decided_idx < my_decided_idx && Self::use_snapshots() {
                let diff_entries = self
                    .internal_storage
                    .get_entries(follower_decided_idx, my_decided_idx);
                let delta_snapshot = Some(SnapshotType::Delta(S::create(diff_entries.as_slice())));
                let suffix = self.internal_storage.get_suffix(my_decided_idx);
                (delta_snapshot, suffix, follower_decided_idx)
            } else {
                let suffix = self.internal_storage.get_suffix(follower_decided_idx);
                (None, suffix, follower_decided_idx)
            }
        };
        let acc_sync = AcceptSync {
            n: self.leader_state.n_leader,
            decided_snapshot: delta_snapshot,
            suffix,
            sync_idx,
            decided_idx: my_decided_idx,
            stopsign: self.get_stopsign(),
        };
        let msg = PaxosMessage {
            from: self.pid,
            to: *pid,
            msg: PaxosMsg::AcceptSync(acc_sync),
        };
        self.outgoing.push(msg);
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
            let accepted_idx = self.internal_storage.append_entries(new_entries);
            self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        }
    }

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
        match max_promise {
            Some((decided_snapshot, suffix)) => {
                match decided_snapshot {
                    Some(s) => {
                        let decided_idx = self
                            .leader_state
                            .get_decided_idx(max_promise_meta.pid)
                            .unwrap();
                        match s {
                            SnapshotType::Complete(c) => {
                                self.internal_storage.set_snapshot(decided_idx, c);
                            }
                            SnapshotType::Delta(d) => {
                                self.internal_storage.merge_snapshot(decided_idx, d);
                            }
                            _ => unimplemented!(),
                        }
                        self.internal_storage.append_entries(suffix);
                        if let Some(ss) = max_stopsign {
                            self.accept_stopsign(ss);
                        } else {
                            self.append_pending_proposals();
                            self.adopt_pending_stopsign();
                        }
                    }
                    None => {
                        // no snapshot, only suffix
                        if max_promise_meta.n == self.internal_storage.get_accepted_round() {
                            self.internal_storage.append_entries(suffix);
                        } else {
                            self.internal_storage.append_on_decided_prefix(suffix);
                        }
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
        self.internal_storage
            .set_accepted_round(self.leader_state.n_leader);
        self.internal_storage.set_decided_idx(decided_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T, S>, from: NodeId) {
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

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T, S>, from: NodeId) {
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
            self.leader_state.get_chosen_idx()
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.leader_state.get_chosen_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                self.leader_state.set_chosen_idx(accepted.accepted_idx);
                let d = Decide {
                    n: self.leader_state.n_leader,
                    decided_idx: self.leader_state.get_chosen_idx(),
                };
                for pid in self.leader_state.get_promised_followers() {
                    if cfg!(feature = "batch_accept") {
                        #[cfg(feature = "batch_accept")]
                        match self.leader_state.get_batch_accept_meta(pid) {
                            Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                                let PaxosMessage { msg, .. } =
                                    self.outgoing.get_mut(outgoing_idx).unwrap();
                                match msg {
                                    PaxosMsg::AcceptDecide(a) => {
                                        a.decided_idx = self.leader_state.get_chosen_idx()
                                    }
                                    _ => {
                                        self.outgoing.push(PaxosMessage {
                                            from: self.pid,
                                            to: pid,
                                            msg: PaxosMsg::Decide(d),
                                        });
                                    }
                                }
                            }
                            _ => {
                                self.outgoing.push(PaxosMessage {
                                    from: self.pid,
                                    to: pid,
                                    msg: PaxosMsg::Decide(d),
                                });
                            }
                        }
                    } else {
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: pid,
                            msg: PaxosMsg::Decide(d),
                        });
                    }
                }
                self.handle_decide(d);
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
                let d = DecideStopSign {
                    n: self.leader_state.n_leader,
                };
                self.handle_decide_stopsign(d);
                for pid in self.leader_state.get_promised_followers() {
                    self.outgoing.push(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::DecideStopSign(d),
                    });
                }
            }
        }
    }
}
