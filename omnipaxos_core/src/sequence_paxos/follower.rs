use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::{
    storage::{Snapshot, SnapshotType, StorageResult},
    util::MessageStatus,
};
#[cfg(feature = "logging")]
use slog::warn;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        let old_promise = self.internal_storage.get_promise().expect("storage error");
        if old_promise <= prep.n || old_promise == prep.n && self.state.1 == Phase::Recover {
            self.leader = prep.n;
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self
                .internal_storage
                .get_accepted_round()
                .expect("storage error");
            let accepted_idx = self.internal_storage.get_log_len().expect("storage error");
            let decided_idx = self.get_decided_idx();
            let stopsign = self.get_stopsign();
            self.internal_storage
                .set_promise(prep.n)
                .expect("storage error");
            let (decided_snapshot, suffix) = if na > prep.n_accepted {
                let ld = prep.decided_idx;
                if ld < decided_idx && T::Snapshot::use_snapshots() {
                    let delta_snapshot =
                        self.internal_storage.create_diff_snapshot(ld, decided_idx);
                    let suffix = self.internal_storage.get_suffix(decided_idx);
                    if delta_snapshot.is_err() || suffix.is_err() {
                        self.internal_storage
                            .set_promise(old_promise)
                            .expect("storage error");
                    }
                    (
                        Some(delta_snapshot.expect("storage error")),
                        suffix.expect("storage error"),
                    )
                } else {
                    let suffix = self.internal_storage.get_suffix(ld);
                    if suffix.is_err() {
                        self.internal_storage
                            .set_promise(old_promise)
                            .expect("storage error");
                    }
                    (None, suffix.expect("storage error"))
                }
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // <<<<<<< HEAD
                //                 if self.internal_storage.get_compacted_idx() > prep.accepted_idx
                //                     && T::Snapshot::use_snapshots()
                //                 {
                // =======
                let compacted_idx = self.internal_storage.get_compacted_idx();
                if compacted_idx.is_err() {
                    self.internal_storage
                        .set_promise(old_promise)
                        .expect("storage error");
                }
                if T::Snapshot::use_snapshots()
                    && compacted_idx.expect("storage error") > prep.accepted_idx
                {
                    // >>>>>>> 72af6d7 (add error handling to storage API)
                    let delta_snapshot = self
                        .internal_storage
                        .create_diff_snapshot(prep.decided_idx, decided_idx);
                    let suffix = self.internal_storage.get_suffix(decided_idx);
                    if delta_snapshot.is_err() || suffix.is_err() {
                        self.internal_storage
                            .set_promise(old_promise)
                            .expect("storage error");
                    }
                    (
                        Some(delta_snapshot.expect("storage error")),
                        suffix.expect("storage error"),
                    )
                } else {
                    let suffix = self.internal_storage.get_suffix(prep.accepted_idx);
                    if suffix.is_err() {
                        self.internal_storage
                            .set_promise(old_promise)
                            .expect("storage error");
                    }
                    (None, suffix.expect("storage error"))
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
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            });
        }
    }

    // Correctness: This function performs multiple operations that cannot be rolled
    // back, so instead it relies on writing in a "safe" order for correctness.
    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.internal_storage.get_promise().expect("storage error") == accsync.n
            && self.state == (Role::Follower, Phase::Prepare)
        {
            let accepted = match accsync.decided_snapshot {
                Some(s) => {
                    match s {
                        SnapshotType::Complete(c) => {
                            self.internal_storage
                                .set_snapshot(accsync.decided_idx, c)
                                .expect("storage error");
                        }
                        SnapshotType::Delta(d) => {
                            self.internal_storage
                                .merge_snapshot(accsync.decided_idx, d)
                                .expect("storage error");
                        }
                    }
                    let accepted_idx = self
                        .internal_storage
                        .append_entries(accsync.suffix)
                        .expect("storage error");
                    Accepted {
                        n: accsync.n,
                        accepted_idx,
                    }
                }
                None => {
                    // no snapshot, only suffix
                    let accepted_idx = self
                        .internal_storage
                        .append_on_prefix(accsync.sync_idx, accsync.suffix)
                        .expect("storage error");
                    Accepted {
                        n: accsync.n,
                        accepted_idx,
                    }
                }
            };
            self.internal_storage
                .set_accepted_round(accsync.n)
                .expect("storage error");
            self.internal_storage
                .set_decided_idx(accsync.decided_idx)
                .expect("storage error");
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, cached_idx));
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            });
            match accsync.stopsign {
                Some(ss) => {
                    if let Some(ss_entry) =
                        self.internal_storage.get_stopsign().expect("storage error")
                    {
                        let StopSignEntry {
                            decided: has_decided,
                            stopsign: _my_ss,
                        } = ss_entry;
                        if !has_decided {
                            self.accept_stopsign(ss);
                        }
                    } else {
                        self.accept_stopsign(ss);
                    }
                    let a = AcceptedStopSign { n: accsync.n };
                    self.outgoing.push(PaxosMessage {
                        from: self.pid,
                        to: from,
                        msg: PaxosMsg::AcceptedStopSign(a),
                    });
                }
                None => self.forward_pending_proposals(),
            }
        }
    }

    fn forward_pending_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.pending_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_acceptdecide(&mut self, acc: AcceptDecide<T>) {
        if self.internal_storage.get_promise().expect("storage error") == acc.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            let msg_status = self.current_seq_num.check_msg_status(acc.seq_num);
            let old_decided_idx = self.get_decided_idx();
            let old_accepted_round = self
                .internal_storage
                .get_accepted_round()
                .expect("storage error");
            match msg_status {
                MessageStatus::First => {
                    // psuedo-AcceptSync for reconfigurations
                    self.internal_storage
                        .set_accepted_round(acc.n)
                        .expect("storage error");
                    self.forward_pending_proposals();
                    self.current_seq_num = acc.seq_num;
                }
                MessageStatus::Expected => self.current_seq_num = acc.seq_num,
                MessageStatus::DroppedPreceding => {
                    self.reconnected(acc.n.pid);
                    return;
                }
                MessageStatus::Outdated => return,
            }

            let entries = acc.entries;
            // handle decide
            if acc.decided_idx > old_decided_idx {
                let result = self.internal_storage.set_decided_idx(acc.decided_idx);
                if result.is_err() {
                    self.internal_storage
                        .set_accepted_round(old_accepted_round)
                        .expect("storage error");
                    result.expect("storage error");
                }
            }
            let result = self.accept_entries(acc.n, entries);
            if result.is_err() {
                self.internal_storage
                    .set_accepted_round(old_accepted_round)
                    .expect("storage error");
                self.internal_storage
                    .set_decided_idx(old_decided_idx)
                    .expect("storage error");
                result.expect("storage error");
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.internal_storage.get_promise().expect("storage error") == acc_ss.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            self.accept_stopsign(acc_ss.ss);
            let a = AcceptedStopSign { n: acc_ss.n };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: self.leader.pid,
                msg: PaxosMsg::AcceptedStopSign(a),
            });
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.internal_storage.get_promise().expect("storage error") == dec.n
            && self.state.1 == Phase::Accept
        {
            let msg_status = self.current_seq_num.check_msg_status(dec.seq_num);
            match msg_status {
                MessageStatus::First => {
                    #[cfg(feature = "logging")]
                    warn!(
                        self.logger,
                        "Decide cannot be the first message in a sequence!"
                    );
                    return;
                }
                MessageStatus::Expected => self.current_seq_num = dec.seq_num,
                MessageStatus::DroppedPreceding => {
                    self.reconnected(dec.n.pid);
                    return;
                }
                MessageStatus::Outdated => return,
            }
            self.internal_storage
                .set_decided_idx(dec.decided_idx)
                .expect("storage error");
        }
    }

    pub(crate) fn handle_decide_stopsign(&mut self, dec: DecideStopSign) {
        if self.internal_storage.get_promise().expect("storage error") == dec.n
            && self.state.1 == Phase::Accept
        {
            let mut ss = self
                .internal_storage
                .get_stopsign()
                .expect("storage error")
                .expect("No stopsign found when deciding!");
            ss.decided = true;
            let log_len = self.internal_storage.get_log_len().expect("storage error");
            let old_decided_idx = self.get_decided_idx();
            self.internal_storage
                .set_decided_idx(log_len + 1)
                .expect("storage error");
            let result = self.internal_storage.set_stopsign(ss); // need to set it again now with the modified decided flag
            if result.is_err() {
                self.internal_storage
                    .set_decided_idx(old_decided_idx)
                    .expect("storage error");
            }
            result.expect("storage error");
        }
    }

    fn accept_entries(&mut self, n: Ballot, entries: Vec<T>) -> StorageResult<()> {
        let accepted_idx = self.internal_storage.append_entries(entries)?;
        match &self.latest_accepted_meta {
            Some((round, outgoing_idx)) if round == &n => {
                let PaxosMessage { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                match msg {
                    PaxosMsg::Accepted(a) => {
                        a.accepted_idx = accepted_idx;
                        Ok(())
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
                    to: self.leader.pid,
                    msg: PaxosMsg::Accepted(accepted),
                });
                Ok(())
            }
        }
    }
}
