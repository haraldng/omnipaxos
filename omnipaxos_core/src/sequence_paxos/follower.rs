use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::storage::SnapshotType;
#[cfg(feature = "logging")]
use slog::debug;

impl<T, S, B> SequencePaxos<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        if self.internal_storage.get_promise() <= prep.n {
            self.leader = prep.n;
            self.internal_storage.set_promise(prep.n);
            self.state = (Role::Follower, Phase::Prepare);
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_log_len();
            let decided_idx = self.get_decided_idx();
            let (decided_snapshot, suffix) = if na > prep.n_accepted {
                let ld = prep.decided_idx;
                if ld < decided_idx && Self::use_snapshots() {
                    let delta_snapshot =
                        self.internal_storage.create_diff_snapshot(ld, decided_idx);
                    let suffix = self.internal_storage.get_suffix(decided_idx);
                    (Some(delta_snapshot), suffix)
                } else {
                    let suffix = self.internal_storage.get_suffix(ld);
                    (None, suffix)
                }
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                let suffix = self.internal_storage.get_suffix(prep.accepted_idx);
                (None, suffix)
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
                stopsign: self.get_stopsign(),
            };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            });
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T, S>, from: NodeId) {
        if self.internal_storage.get_promise() == accsync.n
            && self.state == (Role::Follower, Phase::Prepare)
        {
            let accepted = match accsync.decided_snapshot {
                Some(s) => {
                    match s {
                        SnapshotType::Complete(c) => {
                            self.internal_storage.set_snapshot(accsync.decided_idx, c);
                        }
                        SnapshotType::Delta(d) => {
                            self.internal_storage.merge_snapshot(accsync.decided_idx, d);
                        }
                        _ => unimplemented!(),
                    }
                    let accepted_idx = self.internal_storage.append_entries(accsync.suffix);
                    Accepted {
                        n: accsync.n,
                        accepted_idx,
                    }
                }
                None => {
                    // no snapshot, only suffix
                    let accepted_idx = self
                        .internal_storage
                        .append_on_prefix(accsync.sync_idx, accsync.suffix);
                    Accepted {
                        n: accsync.n,
                        accepted_idx,
                    }
                }
            };
            self.internal_storage.set_accepted_round(accsync.n);
            self.internal_storage.set_decided_idx(accsync.decided_idx);
            self.state = (Role::Follower, Phase::Accept);
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, cached_idx));
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            });
            match accsync.stopsign {
                Some(ss) => {
                    if let Some(ss_entry) = self.internal_storage.get_stopsign() {
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
            self.internal_storage.set_decided_idx(accsync.decided_idx);
        }
    }

    fn forward_pending_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.pending_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_firstaccept(&mut self, f: FirstAccept) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message First Accept");
        if self.internal_storage.get_promise() == f.n
            && self.state == (Role::Follower, Phase::FirstAccept)
        {
            self.internal_storage.set_accepted_round(f.n);
            self.state.1 = Phase::Accept;
            self.forward_pending_proposals();
        }
    }

    pub(crate) fn handle_acceptdecide(&mut self, acc: AcceptDecide<T>) {
        if self.internal_storage.get_promise() == acc.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            let entries = acc.entries;
            self.accept_entries(acc.n, entries);
            // handle decide
            if acc.decided_idx > self.internal_storage.get_decided_idx() {
                self.internal_storage.set_decided_idx(acc.decided_idx);
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.internal_storage.get_promise() == acc_ss.n
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
        if self.internal_storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            self.internal_storage.set_decided_idx(dec.decided_idx);
        }
    }

    pub(crate) fn handle_decide_stopsign(&mut self, dec: DecideStopSign) {
        if self.internal_storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            let mut ss = self
                .internal_storage
                .get_stopsign()
                .expect("No stopsign found when deciding!");
            ss.decided = true;
            self.internal_storage.set_stopsign(ss); // need to set it again now with the modified decided flag
            self.internal_storage
                .set_decided_idx(self.internal_storage.get_log_len() + 1);
        }
    }

    fn accept_entries(&mut self, n: Ballot, entries: Vec<T>) {
        let accepted_idx = self.internal_storage.append_entries(entries);
        match &self.latest_accepted_meta {
            Some((round, outgoing_idx)) if round == &n => {
                let PaxosMessage { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                match msg {
                    PaxosMsg::Accepted(a) => a.accepted_idx = accepted_idx,
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
        }
    }
}
