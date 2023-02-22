/// Ballot Leader Election algorithm for electing new leaders
use crate::util::defaults::*;
#[cfg(feature = "hocon_config")]
#[allow(unused_imports)]
use crate::utils::hocon_kv::LOG_FILE_PATH;

#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    messages::ballot_leader_election::{
        BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest,
    },
    omni_paxos::OmniPaxosConfig,
    util::NodeId,
};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};

/// Used to define an epoch
#[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq)]
pub struct Ballot {
    /// Ballot number
    pub n: u32,
    /// Custom priority parameter
    pub priority: u64,
    /// The pid of the process
    pub pid: NodeId,
}

impl Ballot {
    /// Creates a new Ballot
    /// # Arguments
    /// * `n` - Ballot number.
    /// * `priority` - Custom priority parameter.
    /// * `pid` -  Used as tiebreaker for total ordering of ballots.
    pub fn with(n: u32, priority: u64, pid: NodeId) -> Ballot {
        Ballot { n, priority, pid }
    }
}

/// A Ballot Leader Election component. Used in conjunction with Omni-Paxos handles the election of a leader for a group of omni-paxos replicas,
/// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub struct BallotLeaderElection {
    /// Process identifier used to uniquely identify this instance.
    pid: NodeId,
    /// Vector that holds all the other replicas.
    peers: Vec<u64>,
    /// The current round of the heartbeat cycle.
    hb_round: u32,
    /// Vector which holds all the received ballots.
    ballots: Vec<(Ballot, bool)>,
    /// Holds the current ballot of this instance.
    current_ballot: Ballot, // (round, pid)
    /// States if the instance is a candidate to become a leader.
    quorum_connected: bool,
    /// Current elected leader.
    leader: Option<Ballot>,
    /// The majority of replicas inside a cluster. It is measured in ticks.
    majority: usize,
    /// Vector which holds all the outgoing messages of the BLE instance.
    outgoing: Vec<BLEMessage>,
    /// Logger used to output the status of the component.
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl BallotLeaderElection {
    /// Construct a new BallotLeaderElection node
    pub(crate) fn with(config: BLEConfig) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let n = &peers.len() + 1;
        let initial_ballot = match &config.initial_leader {
            Some(leader_ballot) if leader_ballot.pid == pid => *leader_ballot,
            _ => Ballot::with(0, config.priority, pid),
        };
        let mut ble = BallotLeaderElection {
            pid,
            majority: n / 2 + 1, // +1 because peers is exclusive ourselves
            peers,
            hb_round: 0,
            ballots: Vec::with_capacity(n),
            current_ballot: initial_ballot,
            quorum_connected: true,
            leader: config.initial_leader,
            outgoing: Vec::with_capacity(config.buffer_size),
            #[cfg(feature = "logging")]
            logger: {
                let path = config.logger_file_path;
                config.logger.unwrap_or_else(|| {
                    let s = path.unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                })
            },
        };
        #[cfg(feature = "logging")]
        {
            info!(
                ble.logger,
                "Ballot Leader Election component pid: {} created!", pid
            );
        }
        ble.new_hb_round();
        ble
    }

    /// Update the custom priority used in the Ballot for this server.
    pub(crate) fn set_priority(&mut self, p: u64) {
        self.current_ballot.priority = p;
    }

    /// Returns outgoing messages
    pub(crate) fn get_outgoing_msgs(&mut self) -> Vec<BLEMessage> {
        std::mem::take(&mut self.outgoing)
    }

    /// Handle an incoming message.
    /// # Arguments
    /// * `m` - the message to be handled.
    pub(crate) fn handle(&mut self, m: BLEMessage) {
        match m.msg {
            HeartbeatMsg::Request(req) => self.handle_request(m.from, req),
            HeartbeatMsg::Reply(rep) => self.handle_reply(rep),
        }
    }

    /*
    /// Sets initial state after creation. *Must only be used before being started*.
    /// # Arguments
    /// * `leader_ballot` - Initial leader.
    pub(crate) fn set_initial_leader(&mut self, leader_ballot: Ballot) {
        assert!(self.leader.is_none());
        if leader_ballot.pid == self.pid {
            self.current_ballot = leader_ballot;
        }
        self.leader = Some(leader_ballot);
    }*/

    fn check_leader(&mut self) -> Option<Ballot> {
        self.quorum_connected = true;
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

        if top_ballot < self.leader.unwrap_or_default() {
            // did not get HB from leader
            self.current_ballot.n = self.leader.unwrap_or_default().n + 1;
            self.leader = None;
            None
        } else if self.leader != Some(top_ballot) {
            // got a new leader with greater ballot
            self.leader = Some(top_ballot);
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "BLE {}, New Leader elected: {:?}", self.pid, top_ballot
            );
            Some(top_ballot)
        } else {
            None
        }
    }

    /// Initiates a new heartbeat round.
    pub(crate) fn new_hb_round(&mut self) {
        self.hb_round += 1;
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Initiate new heartbeat round: {}",
            self.hb_round
        );

        for peer in &self.peers {
            let hb_request = HeartbeatRequest {
                round: self.hb_round,
            };

            self.outgoing.push(BLEMessage {
                from: self.pid,
                to: *peer,
                msg: HeartbeatMsg::Request(hb_request),
            });
        }
    }

    pub(crate) fn hb_timeout(&mut self) -> Option<Ballot> {
        let result: Option<Ballot> = if self.ballots.len() + 1 >= self.majority {
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "Received a majority of heartbeats, round: {}, {:?}", self.hb_round, self.ballots
            );
            self.ballots
                .push((self.current_ballot, self.quorum_connected));
            self.check_leader()
        } else {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Did not receive a majority of heartbeats, round: {}, {:?}",
                self.hb_round,
                self.ballots
            );
            self.ballots.clear();
            self.quorum_connected = false;
            None
        };
        self.new_hb_round();
        result
    }

    fn handle_request(&mut self, from: u64, req: HeartbeatRequest) {
        let hb_reply = HeartbeatReply {
            round: req.round,
            ballot: self.current_ballot,
            quorum_connected: self.quorum_connected,
        };

        self.outgoing.push(BLEMessage {
            from: self.pid,
            to: from,
            msg: HeartbeatMsg::Reply(hb_reply),
        });
    }

    fn handle_reply(&mut self, rep: HeartbeatReply) {
        if rep.round == self.hb_round {
            self.ballots.push((rep.ballot, rep.quorum_connected));
        } else {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Got late response, round {}, ballot {:?}",
                self.hb_round,
                rep.ballot
            );
        }
    }
}

/// Configuration for `BallotLeaderElection`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other replicas in the configuration.
/// * `priority`: Set custom priority for this node to be elected as the leader.
/// * `hb_delay`: Timeout for waiting on heartbeat messages. It is measured in number of ticks.
/// * `initial_leader`: The initial leader of the cluster.
/// * `initial_timeout`: Optional initial timeout that can be used to elect a leader faster initially.
/// * `logger`: Custom logger for logging events of Ballot Leader Election.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `buffer_size`: The buffer size for outgoing messages.
#[derive(Clone, Debug)]
pub struct BLEConfig {
    pid: NodeId,
    peers: Vec<u64>,
    priority: u64,
    initial_leader: Option<Ballot>,
    buffer_size: usize,
    #[cfg(feature = "logging")]
    logger: Option<Logger>,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
}

impl From<OmniPaxosConfig> for BLEConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        Self {
            pid: config.pid,
            peers: config.peers,
            priority: config.leader_priority,
            initial_leader: config.initial_leader,
            buffer_size: BLE_BUFFER_SIZE,
            #[cfg(feature = "logging")]
            logger: None,
            #[cfg(feature = "logging")]
            logger_file_path: config.logger_file_path,
        }
    }
}
