use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::{
    storage::{RollbackValue, Snapshot, SnapshotType, StorageResult},
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
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            self.leader = prep.n;
            self.internal_storage
                .flush_batch()
                .expect("storage error while trying to flush batch");
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let decided_idx = self.get_decided_idx();
            let stopsign = self.get_stopsign();
            let (decided_snapshot, suffix) = if na > prep.n_accepted {
                let ld = prep.decided_idx;
                if ld < decided_idx && T::Snapshot::use_snapshots() {
                    let delta_snapshot = self
                        .internal_storage
                        .create_diff_snapshot(ld, decided_idx)
                        .expect("storage error while trying to read diff snapshot");
                    let suffix = self
                        .internal_storage
                        .get_suffix(decided_idx)
                        .expect("storage error while trying to read log suffix");
                    (Some(delta_snapshot), suffix)
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
                    let delta_snapshot = self
                        .internal_storage
                        .create_diff_snapshot(prep.decided_idx, decided_idx)
                        .expect("storage error while trying to read diff snapshot");
                    let suffix = self
                        .internal_storage
                        .get_suffix(decided_idx)
                        .expect("storage error while trying to read decided index");
                    (Some(delta_snapshot), suffix)
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
            self.cached_promise = Some(promise.clone());
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
        if self.internal_storage.get_promise() == accsync.n
            && self.state == (Role::Follower, Phase::Prepare)
        {
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
            let accepted = match accsync.decided_snapshot {
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
                            self.internal_storage.set_snapshot(accsync.decided_idx, c)
                        }
                        SnapshotType::Delta(d) => {
                            self.internal_storage.merge_snapshot(accsync.decided_idx, d)
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
                        .append_entries_without_batching(accsync.suffix);
                    self.internal_storage.rollback_and_panic_if_err(
                        &accepted_res,
                        vec![
                            RollbackValue::AcceptedRound(old_accepted_round),
                            RollbackValue::DecidedIdx(old_decided_idx),
                            RollbackValue::Log(old_log_res.unwrap()),
                            RollbackValue::Snapshot(old_compacted_idx, old_snapshot_res.unwrap()),
                        ],
                        "storage error while trying to write snapshot",
                    );
                    Accepted {
                        n: accsync.n,
                        accepted_idx: accepted_res
                            .expect("storage error while trying to write log entries"),
                    }
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
                    Accepted {
                        n: accsync.n,
                        accepted_idx: accepted_idx
                            .expect("storage error while trying to write log entries"),
                    }
                }
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
            match accsync.stopsign {
                Some(ss) => {
                    if let Some(ss_entry) = self
                        .internal_storage
                        .get_stopsign()
                        .expect("storage error while trying to read stopsign")
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
        if self.internal_storage.get_promise() == acc.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            let msg_status = self.current_seq_num.check_msg_status(acc.seq_num);
            let old_decided_idx = self.get_decided_idx();
            let old_accepted_round = match msg_status {
                MessageStatus::First => {
                    // psuedo-AcceptSync for reconfigurations
                    let old_accepted_round = self.internal_storage.get_accepted_round();
                    self.internal_storage
                        .set_accepted_round(acc.n)
                        .expect("storage error while trying to write accepted round");
                    self.forward_pending_proposals();
                    self.current_seq_num = acc.seq_num;
                    Some(old_accepted_round)
                }
                MessageStatus::Expected => {
                    self.current_seq_num = acc.seq_num;
                    None
                }
                MessageStatus::DroppedPreceding => {
                    self.reconnected(acc.n.pid);
                    return;
                }
                MessageStatus::Outdated => return,
            };

            let entries = acc.entries;
            // handle decide
            if acc.decided_idx > old_decided_idx {
                let result = self.internal_storage.set_decided_idx(acc.decided_idx);
                if result.is_err() {
                    if let Some(r) = old_accepted_round {
                        self.internal_storage.rollback_and_panic(
                            vec![RollbackValue::AcceptedRound(r)],
                            "storage error while trying to write decided index.",
                        )
                    } else {
                        panic!(
                            "storage error while trying to write decided index: {}",
                            result.unwrap_err()
                        );
                    }
                }
            }
            let result = self.accept_entries_follower(acc.n, entries);
            if result.is_err() {
                if let Some(r) = old_accepted_round {
                    self.internal_storage.rollback_and_panic(
                        vec![RollbackValue::AcceptedRound(r)],
                        "storage error while trying to write decided index.",
                    );
                } else {
                    self.internal_storage.rollback_and_panic(
                        vec![RollbackValue::DecidedIdx(old_decided_idx)],
                        "storage error while trying to write log entries.",
                    );
                }
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.internal_storage.get_promise() == acc_ss.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            let msg_status = self.current_seq_num.check_msg_status(acc_ss.seq_num);
            match msg_status {
                MessageStatus::First => {
                    // pseudo-AcceptSync for prepare-less reconfigurations
                    self.internal_storage
                        .set_accepted_round(acc_ss.n)
                        .expect("storage error while trying to write accepted round");
                    self.forward_pending_proposals();
                    self.current_seq_num = acc_ss.seq_num;
                }
                MessageStatus::Expected => self.current_seq_num = acc_ss.seq_num,
                MessageStatus::DroppedPreceding => {
                    self.reconnected(acc_ss.n.pid);
                    return;
                }
                MessageStatus::Outdated => return,
            }

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
        if self.internal_storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
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
                .expect("storage error while trying to write decided index");
        }
    }

    pub(crate) fn handle_decide_stopsign(&mut self, dec: DecideStopSign) {
        if self.internal_storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            let msg_status = self.current_seq_num.check_msg_status(dec.seq_num);
            match msg_status {
                MessageStatus::First => {
                    #[cfg(feature = "logging")]
                    warn!(
                        self.logger,
                        "DecideStopSign cannot be the first message in a sequence!"
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

            let mut ss = self
                .internal_storage
                .get_stopsign()
                .expect("storage error while trying to read stopsign")
                .expect("No stopsign found when deciding!");
            ss.decided = true;
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let old_decided_idx = self.get_decided_idx();
            self.internal_storage
                .set_decided_idx(accepted_idx + 1)
                .expect("storage error while trying to write decided index");
            let result = self.internal_storage.set_stopsign(ss); // need to set it again now with the modified decided flag
            self.internal_storage.rollback_and_panic_if_err(
                &result,
                vec![RollbackValue::DecidedIdx(old_decided_idx)],
                "storage error while trying to write decided index",
            );
        }
    }

    fn accept_entries_follower(&mut self, n: Ballot, entries: Vec<T>) -> StorageResult<()> {
        let accepted_res = self
            .internal_storage
            .append_entries_and_get_accepted_idx(entries)?;
        if let Some(accepted_idx) = accepted_res {
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
                        to: self.leader.pid,
                        msg: PaxosMsg::Accepted(accepted),
                    });
                }
            };
        }
        Ok(())
    }
}
