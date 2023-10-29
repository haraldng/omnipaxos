use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::{
    storage::{Snapshot, SnapshotType},
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
                .commit_write_batch()
                .expect(WRITE_ERROR_MSG);
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
            // TODO: don't need to btach anything? since there is only 1 write
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

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            // TODO: Flush writes so that decided_idx is up to date for snapshot merge? Shouldn't
            // be needed (in fact could mess up decided_idx from when we sent Promise)
            self.internal_storage
                .commit_write_batch()
                .expect(WRITE_ERROR_MSG);
            self.internal_storage.batch_set_accepted_round(accsync.n);
            self.internal_storage
                .batch_set_decided_idx(accsync.decided_idx);
            if let Some(snap) = accsync.decided_snapshot {
                match snap {
                    SnapshotType::Complete(c) => self
                        .internal_storage
                        .batch_set_snapshot(accsync.sync_idx, c),
                    SnapshotType::Delta(d) => self
                        .internal_storage
                        .batch_merge_snapshot(accsync.sync_idx, d)
                        .expect("Error reading from storage."),
                };
            }
            self.internal_storage
                .batch_append_on_prefix(accsync.sync_idx, accsync.suffix);
            match accsync.stopsign {
                Some(ss) => self.internal_storage.batch_set_stopsign(Some(ss)),
                None => self.forward_pending_proposals(),
            }
            // Commit write and reply
            self.internal_storage
                .commit_write_batch()
                .expect(WRITE_ERROR_MSG);
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
            let entry_cache_full = self.internal_storage.batch_append_entries(entries);
            if decided_idx > self.internal_storage.get_decided_idx() {
                self.internal_storage.batch_set_decided_idx(decided_idx);
            }
            if entry_cache_full {
                let new_accepted_idx = self
                    .internal_storage
                    .commit_write_batch()
                    .expect(WRITE_ERROR_MSG)
                    .unwrap();
                self.handle_flushed_accepted(n, new_accepted_idx);
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            self.internal_storage.batch_set_stopsign(Some(acc_ss.ss));
            let accepted_idx = self
                .internal_storage
                .commit_write_batch()
                .expect(WRITE_ERROR_MSG)
                .unwrap();
            let a = Accepted {
                n: acc_ss.n,
                accepted_idx,
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
            self.internal_storage.batch_set_decided_idx(dec.decided_idx);
            // TODO: Don't need to do this, but then tests break when they wait on decided entries
            // Flush any pending entries
            let new_accepted_idx = self
                .internal_storage
                .commit_write_batch()
                .expect(WRITE_ERROR_MSG);
            if let Some(idx) = new_accepted_idx {
                self.handle_flushed_accepted(dec.n, idx);
            }
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
}
