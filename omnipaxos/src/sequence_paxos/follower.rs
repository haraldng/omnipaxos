use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::util::{MessageStatus, WRITE_ERROR_MSG};

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
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_promise(prep.n)
                .expect(WRITE_ERROR_MSG);
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let log_sync = if na > prep.n_accepted {
                // I'm more up to date: send leader what he is missing after his decided index.
                Some(self.create_log_sync(prep.decided_idx, prep.decided_idx))
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // I'm more up to date and in same round: send leader what he is missing after his
                // accepted index.
                Some(self.create_log_sync(prep.accepted_idx, prep.decided_idx))
            } else {
                // I'm equally or less up to date
                None
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_idx: self.internal_storage.get_decided_idx(),
                accepted_idx,
                log_sync,
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
            let new_accepted_idx = self
                .internal_storage
                .sync_log(accsync.n, accsync.decided_idx, Some(accsync.log_sync))
                .expect(WRITE_ERROR_MSG);
            if self.internal_storage.get_stopsign().is_none() {
                self.forward_buffered_proposals();
            }
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: new_accepted_idx,
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

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.buffered_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_acceptdecide(&mut self, acc_dec: AcceptDecide<T>) {
        if self.check_valid_ballot(acc_dec.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_dec.seq_num, acc_dec.n.pid) == MessageStatus::Expected
        {
            #[cfg(not(feature = "unicache"))]
            let entries = acc_dec.entries;
            #[cfg(feature = "unicache")]
            let entries = self.internal_storage.decode_entries(acc_dec.entries);
            let mut new_accepted_idx = self
                .internal_storage
                .append_entries_and_get_accepted_idx(entries)
                .expect(WRITE_ERROR_MSG);
            let flushed_after_decide = self.update_decided_idx_and_get_accepted_idx(acc_dec.decided_idx);    
            if flushed_after_decide.is_some() {
                new_accepted_idx = flushed_after_decide;
            }
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(acc_dec.n, idx);
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            // Don't have to handle flushed entries here because we will send Accepted after
            // appending stopsign.
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            let new_accepted_idx = self
                .internal_storage
                .set_stopsign(Some(acc_ss.ss))
                .expect(WRITE_ERROR_MSG);
            self.reply_accepted(acc_ss.n, new_accepted_idx);
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            let new_accepted_idx = self.update_decided_idx_and_get_accepted_idx(dec.decided_idx);
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(dec.n, idx);
            }
        }
    }

    /// To maintain decided index <= accepted index, batched entries may be flushed.
    /// Returns `Some(new_accepted_idx)` if entries are flushed, otherwise `None`.
    fn update_decided_idx_and_get_accepted_idx(&mut self, new_decided_idx: usize) -> Option<usize> {
        if new_decided_idx <= self.internal_storage.get_decided_idx() {
            return None;
        }
        if new_decided_idx > self.internal_storage.get_accepted_idx() {
            let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_decided_idx(new_decided_idx.min(new_accepted_idx))
                .expect(WRITE_ERROR_MSG);
            Some(new_accepted_idx)
        } else {
            self.internal_storage
                .set_decided_idx(new_decided_idx)
                .expect(WRITE_ERROR_MSG);
            None
        }
    }

    fn reply_accepted(&mut self, n: Ballot, accepted_idx: usize) {
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
