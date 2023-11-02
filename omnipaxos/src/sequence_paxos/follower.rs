use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::{
    storage::{Snapshot, SnapshotType, InternalStorageTxn},
    util::{MessageStatus, WRITE_ERROR_MSG},
};

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            // Flush any pending writes
            self.internal_storage
                .flush_batch()
                .expect(WRITE_ERROR_MSG);
            self.internal_storage.set_promise(prep.n).expect(WRITE_ERROR_MSG);
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let decided_idx = self.get_decided_idx();
            let stopsign = self.internal_storage.get_stopsign();
            let (decided_snapshot, suffix) = if na > prep.n_accepted {
                // I'm more up to date and possible split-brain => append onto preparer's decided entries.
                let ld = prep.decided_idx;
                if ld < decided_idx && T::Snapshot::use_snapshots() {
                    let (delta_snapshot, _) = self
                        .internal_storage
                        .create_diff_snapshot(ld)
                        .expect("storage error while trying to read diff snapshot");
                    let suffix = self
                        .internal_storage
                        .get_suffix(decided_idx)
                        .expect("storage error while trying to read log suffix");
                    (delta_snapshot, suffix)
                } else {
                    let suffix = self
                        .internal_storage
                        .get_suffix(ld)
                        .expect("storage error while trying to read log suffix");
                    (None, suffix)
                }
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // I'm more up to date and no split-brain possible => append onto preparer's
                // accepted entries.
                let compacted_idx = self.internal_storage.get_compacted_idx();
                if T::Snapshot::use_snapshots() && compacted_idx > prep.accepted_idx {
                    let (delta_snapshot, _) = self
                        .internal_storage
                        .create_diff_snapshot(prep.decided_idx)
                        .expect("storage error while trying to read diff snapshot");
                    let suffix = self
                        .internal_storage
                        .get_suffix(decided_idx)
                        .expect("storage error while trying to read log suffix");
                    (delta_snapshot, suffix)
                } else {
                    let suffix = self
                        .internal_storage
                        .get_suffix(prep.accepted_idx)
                        .expect("storage error while trying to read log suffix");
                    (None, suffix)
                }
            } else {
                (None, vec![])
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_snapshot,
                suffix,
                decided_idx,
                accepted_idx,
                stopsign,
            };
            self.cached_promise_message = Some(promise.clone());
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            });
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            let txn = InternalStorageTxn::new();
            txn.set_accepted_round(accsync.n);
            txn.set_decided_idx(accsync.decided_idx);
            if let Some(snap) = accsync.decided_snapshot {
                match snap {
                    SnapshotType::Complete(c) => txn.set_snapshot(accsync.sync_idx, c),
                    SnapshotType::Delta(d) => {
                        let decided_snap = self.internal_storage.create_merged_snapshot(accsync.sync_idx, d)
                        .expect("Error reading from storage.");
                        txn.set_snapshot(accsync.sync_idx, decided_snap);
                    }
                };
            }
            txn.append_on_prefix(accsync.sync_idx, accsync.suffix);
            match accsync.stopsign {
                Some(ss) => txn.set_stopsign(Some(ss)),
                None => self.forward_pending_proposals(),
            }
            // Commit write and reply
            let accepted_idx = self
                .internal_storage
                .execute_txn(txn)
                .expect(WRITE_ERROR_MSG);
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx,
            };
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, cached_idx));
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            });
            #[cfg(feature = "unicache")]
            self.internal_storage.set_unicache(accsync.unicache);
        }
    }

    fn forward_pending_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.pending_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    // TODO: handle UNIcache flag better (need to check ballot before decoding)
    pub(crate) fn handle_acceptdecide(
        &mut self,
        n: Ballot,
        seq_num: SequenceNumber,
        decided_idx: usize,
        entries: Vec<T>,
    ) {
        if self.check_valid_ballot(n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(seq_num, n.pid) == MessageStatus::Expected
        {
            let mut new_accepted_idx = self.internal_storage.append_entries_and_get_accepted_idx(entries).expect(WRITE_ERROR_MSG);
            if decided_idx > self.internal_storage.get_decided_idx() {
                if decided_idx > self.internal_storage.get_accepted_idx() {
                    new_accepted_idx = Some(self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG));
                }
                self.internal_storage.set_decided_idx(decided_idx).expect(WRITE_ERROR_MSG);
            }
            if let Some(idx) = new_accepted_idx {
                self.handle_flushed_accepted(n, idx);
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            let new_accepted_idx = self.internal_storage.set_stopsign(Some(acc_ss.ss)).expect(WRITE_ERROR_MSG);
            self.handle_flushed_accepted(acc_ss.n, new_accepted_idx);
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            if dec.decided_idx > self.internal_storage.get_accepted_idx() {
                let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
                self.handle_flushed_accepted(dec.n, new_accepted_idx);
            }
            self.internal_storage.set_decided_idx(dec.decided_idx).expect(WRITE_ERROR_MSG);
        }
    }

    fn handle_flushed_accepted(&mut self, n: Ballot, accepted_idx: usize) {
        match &self.latest_accepted_meta {
            Some((round, outgoing_idx)) if round == &n => {
                let PaxosMessage { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                match msg {
                    PaxosMsg::Accepted(a) => {
                        a.accepted_idx = accepted_idx;
                    }
                    _ => panic!("Cached idx is not an Accepted Message<T>!"),
                }
            }
            _ => {
                let accepted = Accepted { n, accepted_idx };
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((n, cached_idx));
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: n.pid,
                    msg: PaxosMsg::Accepted(accepted),
                });
            }
        };
    }

    /// Also returns whether the message's ballot was promised
    fn check_valid_ballot(&mut self, message_ballot: Ballot) -> bool {
        let my_promise = self.internal_storage.get_promise();
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "NotAccepted. My promise: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: message_ballot.pid,
                    msg: PaxosMsg::NotAccepted(not_acc),
                });
                false
            }
            std::cmp::Ordering::Less => {
                // Should never happen, but to be safe send PrepareReq
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "Received non-prepare message from a leader I've never promised. My: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.reconnected(message_ballot.pid);
                false
            }
        }
    }

    /// Also returns the MessageStatus of the sequence based on the incoming sequence number.
    fn handle_sequence_num(&mut self, seq_num: SequenceNumber, from: NodeId) -> MessageStatus {
        let msg_status = self.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.current_seq_num = seq_num,
            MessageStatus::DroppedPreceding => self.reconnected(from),
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Promise
                match &self.cached_promise_message {
                    Some(promise) => {
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: promise.n.pid,
                            msg: PaxosMsg::Promise(promise.clone()),
                        });
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        self.send_preparereq_to_all_peers();
                    }
                }
            }
            Phase::Recover => {
                // Resend PrepareReq
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => (),
            Phase::None => (),
        }
    }

    fn send_preparereq_to_all_peers(&mut self) {
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        for peer in &self.peers {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: *peer,
                msg: PaxosMsg::PrepareReq(prepreq),
            });
        }
    }
}
