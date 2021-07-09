use crate::ble::messages::{BleMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest};
use crate::leader_election::{Leader, Round};

/// Used to define an epoch
#[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq)]
pub struct Ballot {
    /// Ballot number
    pub n: u32,
    /// The pid of the process
    pub pid: u64,
}

impl Ballot {
    /// Create a new Ballot
    pub fn with(n: u32, pid: u64) -> Ballot {
        Ballot { n, pid }
    }
}

impl Round for Ballot {}

/// A Ballot Leader Election component. Used in conjunction with Omni-Paxos handles the election of a leader for a group of omni-paxos replicas,
/// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub struct BallotLeaderComp {
    pid: u64,
    pub(crate) peers: Vec<u64>,
    hb_round: u32,
    ballots: Vec<(Ballot, bool)>,
    current_ballot: Ballot, // (round, pid)
    candidate: bool,
    leader: Option<Ballot>,
    hb_current_delay: u64,
    hb_delay: u64,
    delta: u64,
    /// The majority of replicas inside a cluster
    pub majority: usize,
    stopped: bool,
    quick_timeout: bool,
    initial_election_factor: u64,
    heartbeat_elapsed: u64,
    outgoing: Vec<BleMessage>,
}

impl BallotLeaderComp {
    /// Construct a new BallotLeaderComponent
    pub fn with(
        peers: Vec<u64>,
        pid: u64,
        hb_delay: u64,
        delta: u64,
        quick_timeout: bool,
        initial_leader: Option<Leader<Ballot>>,
        initial_election_factor: u64,
    ) -> BallotLeaderComp {
        let n = &peers.len() + 1;
        let (leader, initial_ballot) = match initial_leader {
            Some(l) => {
                let leader_ballot = Ballot::with(l.round.n, l.pid);
                let initial_ballot = if l.pid == pid {
                    leader_ballot
                } else {
                    Ballot::with(0, pid)
                };
                (Some(leader_ballot), initial_ballot)
            }
            None => {
                let initial_ballot = Ballot::with(0, pid);
                (None, initial_ballot)
            }
        };
        BallotLeaderComp {
            pid,
            majority: n / 2 + 1, // +1 because peers is exclusive ourselves
            peers,
            hb_round: 0,
            ballots: Vec::with_capacity(n),
            current_ballot: initial_ballot,
            candidate: true,
            leader,
            hb_current_delay: hb_delay,
            hb_delay,
            delta,
            stopped: false,
            quick_timeout,
            initial_election_factor,
            heartbeat_elapsed: 0,
            outgoing: vec![],
        }
    }

    /// Sets initial state after creation. Should only be used before being started.
    pub fn set_initial_leader(&mut self, l: Leader<Ballot>) {
        assert!(self.leader.is_none());
        let leader_ballot = Ballot::with(l.round.n, l.pid);
        self.leader = Some(leader_ballot);
        if l.pid == self.pid {
            self.current_ballot = leader_ballot;
            self.candidate = true;
        } else {
            self.current_ballot = Ballot::with(0, self.pid);
            self.candidate = false;
        };
        self.quick_timeout = false;
    }

    fn check_leader(&mut self) -> Option<Leader<Ballot>> {
        let ballots = std::mem::take(&mut self.ballots);
        let top_ballot = ballots
            .into_iter()
            .filter_map(
                |(ballot, candidate)| {
                    if candidate {
                        Some(ballot)
                    } else {
                        None
                    }
                },
            )
            .max()
            .unwrap_or_default();

        let mut result: Option<Leader<Ballot>> = None;

        if top_ballot < self.leader.unwrap_or_default() {
            // did not get HB from leader
            self.current_ballot.n = self.leader.unwrap_or_default().n + 1;
            self.leader = None;
            self.candidate = true;

            result = None
        } else if self.leader != Some(top_ballot) {
            // got a new leader with greater ballot
            self.quick_timeout = false;
            self.leader = Some(top_ballot);
            let top_pid = top_ballot.pid;
            if self.pid == top_pid {
                self.candidate = true;
            } else {
                self.candidate = false;
            }

            result = Some(Leader::with(top_pid, top_ballot))
        }

        result
    }

    // tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout.
    // Returns true to indicate that there will probably be some readiness need to be handled.
    fn tick(&mut self) -> bool {
        self.heartbeat_elapsed += 1;

        let mut has_ready = false;

        if self.heartbeat_elapsed >= self.hb_current_delay {
            self.heartbeat_elapsed = 0;
            self.hb_timeout();
            has_ready = true;
        }

        has_ready
    }

    fn new_hb_round(&mut self) {
        self.hb_current_delay = if self.quick_timeout {
            // use short timeout if still no first leader
            self.hb_delay / self.initial_election_factor
        } else {
            self.hb_delay
        };

        self.hb_round += 1;
        for peer in &self.peers {
            let hb_request = HeartbeatRequest::with(self.hb_round);

            self.outgoing.push(BleMessage::with(
                self.pid,
                *peer,
                HeartbeatMsg::Request(hb_request),
            ));
        }
    }

    fn hb_timeout(&mut self) -> Option<Leader<Ballot>> {
        let result: Option<Leader<Ballot>> = if self.ballots.len() + 1 >= self.majority {
            self.ballots.push((self.current_ballot, self.candidate));
            self.check_leader()
        } else {
            self.ballots.clear();
            self.candidate = false;
            None
        };
        self.new_hb_round();

        result
    }

    /// Handle an incoming message.
    pub fn handle(&mut self, m: BleMessage) {
        match m.msg {
            HeartbeatMsg::Request(req) => self.handle_request(m.from, req),
            HeartbeatMsg::Reply(rep) => self.handle_reply(rep),
        }
    }

    fn handle_request(&mut self, from: u64, req: HeartbeatRequest) {
        let hb_reply = HeartbeatReply::with(req.round, self.current_ballot, self.candidate);

        self.outgoing.push(BleMessage::with(
            self.pid,
            from,
            HeartbeatMsg::Reply(hb_reply),
        ));
    }

    fn handle_reply(&mut self, rep: HeartbeatReply) {
        if rep.round == self.hb_round {
            self.ballots.push((rep.ballot, rep.candidate));
        } else {
            self.hb_delay += self.delta;
        }
    }
}
