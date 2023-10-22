use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::{
    storage::{RollbackValue, Snapshot, SnapshotType, StorageResult},
    util::MessageStatus,
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
            self.internal_storage
                .flush_batch()
                .expect("storage error while trying to flush batch");
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let decided_idx = self.get_decided_idx();
            let stopsign = self.internal_storage.get_stopsign();
            let (decided_snapshot, suffix) = if na > prep.n_accepted {
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
            self.internal_storage
                .set_promise(prep.n)
                .expect("storage error while trying to write promise");
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

    // Correctness: This function performs multiple storage operations that cannot be rolled
    // back, so instead it relies on writing in a "safe" order for correctness.
    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            let old_decided_idx = self.internal_storage.get_decided_idx();
            let old_accepted_round = self.internal_storage.get_accepted_round();
            self.internal_storage
                .set_accepted_round(accsync.n)
                .expect("storage error while trying to write accepted round");
            let result = self.internal_storage.set_decided_idx(accsync.decided_idx);
            self.internal_storage.rollback_and_panic_if_err(
                &result,
                vec![RollbackValue::AcceptedRound(old_accepted_round)],
                "storage error while trying to write decided index",
            );
            match accsync.decided_snapshot {
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
                    let snapshot_res = match s {
                        SnapshotType::Complete(c) => {
                            self.internal_storage.set_snapshot(accsync.sync_idx, c)
                        }
                        SnapshotType::Delta(d) => {
                            self.internal_storage.merge_snapshot(accsync.sync_idx, d)
                        }
                    };
                    self.internal_storage.rollback_and_panic_if_err(
                        &snapshot_res,
                        vec![
                            RollbackValue::AcceptedRound(old_accepted_round),
                            RollbackValue::DecidedIdx(old_decided_idx),
                        ],
                        "storage error while trying to write snapshot",
                    );
                    let accepted_res = self
                        .internal_storage
                        .append_on_prefix(accsync.sync_idx, accsync.suffix);
                    self.internal_storage.rollback_and_panic_if_err(
                        &accepted_res,
                        vec![
                            RollbackValue::AcceptedRound(old_accepted_round),
                            RollbackValue::DecidedIdx(old_decided_idx),
                            RollbackValue::Log(old_log_res.unwrap()),
                            RollbackValue::Snapshot(old_compacted_idx, old_snapshot_res.unwrap()),
                        ],
                        "storage error while trying to write log entries",
                    );
                }
                None => {
                    // no snapshot, only suffix
                    let accepted_idx = self
                        .internal_storage
                        .append_on_prefix(accsync.sync_idx, accsync.suffix);
                    self.internal_storage.rollback_and_panic_if_err(
                        &accepted_idx,
                        vec![
                            RollbackValue::AcceptedRound(old_accepted_round),
                            RollbackValue::DecidedIdx(old_decided_idx),
                        ],
                        "storage error while trying to write log entries",
                    );
                }
            };
            match accsync.stopsign {
                Some(ss) => self.accept_stopsign(ss),
                None => self.forward_pending_proposals(),
            }
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: self.internal_storage.get_accepted_idx(),
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

    pub(crate) fn handle_acceptdecide(&mut self, acc: AcceptDecide<T>) {
        if self.check_valid_ballot(acc.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc.seq_num, acc.n.pid) == MessageStatus::Expected
        {
            // handle decide
            let old_decided_idx = self.get_decided_idx();
            if acc.decided_idx > old_decided_idx {
                self.internal_storage
                    .set_decided_idx(acc.decided_idx)
                    .expect("storage error while trying to write decided index");
            }
            // handle accept
            let entries = acc.entries;
            let result = self.accept_entries_follower(acc.n, entries);
            self.internal_storage.rollback_and_panic_if_err(
                &result,
                vec![RollbackValue::DecidedIdx(old_decided_idx)],
                "storage error while trying to write log entries.",
            );
        }
    }

    #[cfg(feature = "unicache")]
    pub(crate) fn handle_encoded_acceptdecide(&mut self, e: EncodedAcceptDecide<T>) {
        if self.check_valid_ballot(e.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(e.seq_num, e.n.pid) == MessageStatus::Expected
        {
            // handle decide
            let old_decided_idx = self.get_decided_idx();
            if e.decided_idx > old_decided_idx {
                self.internal_storage
                    .set_decided_idx(e.decided_idx)
                    .expect("storage error while trying to write decided index");
            }
            // handle accept
            let encoded_entries = e.entries;
            let result = self.accept_encoded_entries_follower(e.n, encoded_entries);
            self.internal_storage.rollback_and_panic_if_err(
                &result,
                vec![RollbackValue::DecidedIdx(old_decided_idx)],
                "storage error while trying to write log entries.",
            );
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            self.accept_stopsign(acc_ss.ss);
            let a = Accepted {
                n: acc_ss.n,
                accepted_idx: self.internal_storage.get_accepted_idx(),
            };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: acc_ss.n.pid,
                msg: PaxosMsg::Accepted(a),
            });
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            self.internal_storage
                .set_decided_idx(dec.decided_idx)
                .expect("storage error while trying to write decided index");
        }
    }

    fn accept_entries_follower(&mut self, n: Ballot, entries: Vec<T>) -> StorageResult<()> {
        let accepted_res = self
            .internal_storage
            .append_entries_and_get_accepted_idx(entries)?;
        if let Some(accepted_idx) = accepted_res {
            self.handle_flushed_accepted(n, accepted_idx);
        }
        Ok(())
    }

    #[cfg(feature = "unicache")]
    fn accept_encoded_entries_follower(
        &mut self,
        n: Ballot,
        encoded_entries: Vec<T::EncodeResult>,
    ) -> StorageResult<()> {
        let accepted_res = self
            .internal_storage
            .append_encoded_entries_and_get_accepted_idx(encoded_entries)?;
        if let Some(accepted_idx) = accepted_res {
            self.handle_flushed_accepted(n, accepted_idx);
        }
        Ok(())
    }

    fn handle_flushed_accepted(&mut self, n: Ballot, accepted_idx: u64) {
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
}
