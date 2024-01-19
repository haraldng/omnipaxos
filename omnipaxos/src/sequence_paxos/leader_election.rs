use crate::util::{ConfigurationId, LogicalClock};
use super::*;
use crate::messages::leader_election::*;

struct QCChecker {
    hb_round: u32,
    hb_replies: usize,
    status: bool,
    quorum: Quorum
}

impl QCChecker {
    fn new_hb_round(&mut self) {
        self.status = self.quorum.is_prepare_quorum(self.hb_replies);
        self.hb_round += 1;
        self.hb_replies = 1;
    }

    fn is_qc(&self) -> bool {
        self.status
    }
}

struct Vote {
    pid: NodeId,
    qc: bool,
    forwarded: bool,
}

struct CandidateState {
    ballot: Ballot,
    votes: Vec<Vote>,
}

impl CandidateState {
    fn clear(&mut self) {
        self.ballot = Ballot::default();
        self.votes.clear();
    }
}

struct ProgressTimer {
    ballot: Ballot,
    timer: LogicalClock
}

impl ProgressTimer {
    fn new(ballot: Ballot) -> Self {
        Self {
            ballot,
            timer: LogicalClock::with(100)
        }
    }

    fn reset_timer(&mut self) {
        self.timer.reset();
    }

    fn clear(&mut self) {
        self.ballot = Ballot::default();
        self.timer.reset();
    }
}
struct LeaderElection {
    /// The identifier for the configuration that this instance is part of.
    configuration_id: ConfigurationId,
    /// Process identifier used to uniquely identify this instance.
    pid: NodeId,
    /// Vector that holds the pids of all the other servers.
    peers: Vec<NodeId>,
    quorum: Quorum,
    /// timer for when an established leader exists
    rp: ProgressTimer,
    qc: QCChecker,
    /// state for when I'm trying to become leader
    candidate_state: CandidateState,
    /// state when somebody else is trying to become leader
    pre_promise: ProgressTimer,
    /// largest **promised** (not pre-promised) ballot that has been seen.
    max_ballot: Ballot,
}

impl LeaderElection {
    pub fn tick(&mut self) {
        if self.rp.timer.tick_and_check_timeout() {
            let candidate_n = self.max_ballot.n + 1;
            let candidate_ballot = Ballot::with(self.configuration_id, candidate_n, 0, self.pid);
            self.candidate_state = CandidateState { ballot: candidate_ballot, votes: vec![Vote { pid: self.pid, qc: self.qc.is_qc(), forwarded: false }]};
            self.rp = ProgressTimer::new(candidate_ballot);
            todo!("Send RequestVote to everybody")
        }
        if self.pre_promise.ballot != Ballot::default() {
            if self.pre_promise.timer.tick_and_check_timeout() {
                let req = VoteRequest { ballot: self.pre_promise.ballot };
                todo!("Broadcast voterequest");
            }
        }
    }

    pub fn update_promise(&mut self, promise: Ballot) {
        self.rp = ProgressTimer::new(promise);
        if self.pre_promise.ballot == promise {
            self.pre_promise.clear();
        }
        if promise > self.max_ballot {
            self.max_ballot = promise;
        }
    }

    fn handle_vote_request(&mut self, req: VoteRequest) {
        let rp = if req.ballot > self.pre_promise.ballot && req.ballot > self.rp.ballot && self.rp.timer.check_timeout() {
            self.pre_promise = ProgressTimer::new(req.ballot);
            None
        } else {
            Some(self.rp.ballot)
        };
        let rep = VoteReply {
            ballot: req.ballot,
            sender: self.pid,
            recent_progress: rp,
            qc: self.qc.is_qc(),
        };
        todo!("Reply or broadcast vote")
    }

    fn handle_vote_reply(&mut self, rep: VoteReply, from: NodeId) {
        if rep.ballot == self.candidate_state.ballot {
            // reply for me
            match rep.recent_progress {
                Some(max_ballot) => {
                    // they've made progress recently, so my take over failed
                    self.max_ballot = std::cmp::max(max_ballot, self.max_ballot);
                    let c = CancelElection { b: self.candidate_state.ballot };
                    self.candidate_state.clear();
                    todo!("broadcast cancel");
                },
                None => {
                    // got the vote
                    let v = Vote {
                        pid: rep.sender,
                        qc: rep.qc,
                        forwarded: rep.sender != from,
                    };
                    self.candidate_state.votes.push(v);
                    if self.quorum.is_prepare_quorum(self.candidate_state.votes.len()) {
                        todo!("Output leader so that we send prepare")
                    }
                }
            }
        }
    }

    fn handle_cancel_election(&mut self, c: CancelElection) {
        if c.b == self.pre_promise.ballot {
            self.pre_promise.clear();
        }
    }

    fn handle_hb_request(&self, hb_req: HBRequest) {
        let hb_reply = HBReply { round: hb_req.round, max_ballot: self.max_ballot };
        todo!("reply")
    }

    fn handle_hb_reply(&mut self, hb_reply: HBReply) {
        if self.qc.hb_round == hb_reply.round {
            self.qc.hb_replies += 1;
            self.max_ballot = std::cmp::max(self.max_ballot, hb_reply.max_ballot);
        }
    }

    fn handle_hb_timeout(&mut self) {
        self.qc.new_hb_round();
    }
}