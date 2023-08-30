use std::cmp::Ordering;

/// Ballot Leader Election algorithm for electing new leaders
use crate::util::{defaults::*, ConfigurationId, FlexibleQuorum, Quorum};

#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    messages::ballot_leader_election::{
        BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest,
    },
    util::NodeId,
    OmniPaxosConfig,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
#[cfg(feature = "ui")]
use omnipaxos_ui::UI;

/// Used to define a Sequence Paxos epoch
#[derive(Clone, Copy, Eq, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Ballot {
    /// The identifier for the configuration that the replica with this ballot is part of.
    pub config_id: ConfigurationId,
    /// Ballot number
    pub n: u32,
    /// Custom priority parameter
    pub priority: u32,
    /// The pid of the process
    pub pid: NodeId,
}

impl Ballot {
    /// Creates a new Ballot
    /// # Arguments
    /// * `config_id` - The identifier for the configuration that the replica with this ballot is part of.
    /// * `n` - Ballot number.
    /// * `pid` -  Used as tiebreaker for total ordering of ballots.
    pub fn with(config_id: ConfigurationId, n: u32, priority: u32, pid: NodeId) -> Ballot {
        Ballot {
            config_id,
            n,
            priority,
            pid,
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.n, self.priority, self.pid).cmp(&(other.n, other.priority, other.pid))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The connectivity of an OmniPaxos node
pub(crate) type Connectivity = u8;

/// A Ballot Leader Election component. Used in conjunction with OmniPaxos to handle the election of a leader for a cluster of OmniPaxos servers,
/// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub(crate) struct BallotLeaderElection {
    /// The identifier for the configuration that this instance is part of.
    configuration_id: ConfigurationId,
    /// Process identifier used to uniquely identify this instance.
    pid: NodeId,
    /// Vector that holds the pids of all the other servers.
    peers: Vec<NodeId>,
    /// The current round of the heartbeat cycle.
    hb_round: u32,
    /// Vector which temporarily holds all the received heartbeats from one heartbeat round, including the current node.
    ballots: Vec<(Ballot, Connectivity)>,
    /// Vector that holds all the received heartbeats from the previous heartbeat round, including the current node.
    /// Represents nodes that are currently alive from the view of the current node.
    #[cfg(feature = "ui")]
    prev_round_ballots: Vec<(Ballot, Connectivity)>,
    /// Holds the current ballot of this instance.
    current_ballot: Ballot,
    /// The number of replicas inside the cluster that this instance is
    /// connected to (based on heartbeats received) including itself.
    connectivity: Connectivity,
    /// Current elected leader.
    leader: Option<Ballot>,
    /// The number of replicas inside the cluster whose heartbeats are needed to become and remain the leader.
    quorum: Quorum,
    /// Vector which holds all the outgoing messages of the BLE instance.
    outgoing: Vec<BLEMessage>,
    /// Logger used to output the status of the component.
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl BallotLeaderElection {
    /// Construct a new BallotLeaderElection node
    pub(crate) fn with(config: BLEConfig, initial_leader: Option<Ballot>) -> Self {
        let config_id = config.configuration_id;
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let initial_ballot = Ballot::with(config_id, 0, config.priority, pid);
        let mut ble = BallotLeaderElection {
            configuration_id: config_id,
            pid,
            peers,
            hb_round: 0,
            ballots: Vec::with_capacity(num_nodes),
            #[cfg(feature = "ui")]
            prev_round_ballots: Vec::with_capacity(num_nodes),
            current_ballot: initial_ballot,
            connectivity: num_nodes as Connectivity,
            leader: initial_leader,
            quorum,
            outgoing: Vec::with_capacity(config.buffer_size),
            #[cfg(feature = "logging")]
            logger: {

                #[cfg(not(feature = "ui"))]
                {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                }
                #[cfg(feature = "ui")]
                {
                    let logger = UI::logger();
                    info!(logger, "UI_logger created with slog");
                    logger
                }
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

    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub(crate) fn set_priority(&mut self, p: u32) {
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

    fn check_leader(&mut self) -> Option<Ballot> {
        let ballots = std::mem::take(&mut self.ballots);
        let top_accept_ballot = ballots
            .iter()
            .filter_map(|&(ballot, connectivity)| {
                if self.quorum.is_accept_quorum(connectivity as usize) {
                    Some(ballot)
                } else {
                    None
                }
            })
            .max()
            .unwrap_or_default();
        let leader_ballot = self.leader.unwrap_or_default();
        if top_accept_ballot == leader_ballot {
            // leader is still alive and has accept quorum
            None
        } else {
            // leader is dead || changed priority || doesn't have an accept quorum
            let top_prepare_ballot = ballots
                .iter()
                .filter_map(|&(ballot, connectivity)| {
                    if self.quorum.is_prepare_quorum(connectivity as usize) {
                        Some(ballot)
                    } else {
                        None
                    }
                })
                .max()
                .unwrap_or_default();
            if top_prepare_ballot > leader_ballot {
                // new leader with prepare quorum
                let new_leader = top_prepare_ballot;
                self.leader = Some(new_leader);
                #[cfg(feature = "logging")]
                debug!(
                    self.logger,
                    "BLE {}, New Leader elected: {:?}", self.pid, new_leader
                );
                Some(new_leader)
            } else {
                // nobody has taken over leadership, let's try to ourselves
                self.current_ballot.n = leader_ballot.n + 1;
                self.leader = None;
                None
            }
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
        // Add our own ballot to the list of received ballots of current hb round
        self.ballots.push((self.current_ballot, self.connectivity));
        #[cfg(feature = "ui")]
        {
            self.prev_round_ballots = self.ballots.clone();
        }
        let my_connectivity = self.ballots.len();
        self.connectivity = my_connectivity as Connectivity;
        let result: Option<Ballot> = if self.quorum.is_prepare_quorum(my_connectivity) {
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "Received a majority of heartbeats, round: {}, {:?}", self.hb_round, self.ballots
            );
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
            None
        };
        self.new_hb_round();
        result
    }

    fn handle_request(&mut self, from: NodeId, req: HeartbeatRequest) {
        let hb_reply = HeartbeatReply {
            round: req.round,
            ballot: self.current_ballot,
            connectivity: self.connectivity,
        };

        self.outgoing.push(BLEMessage {
            from: self.pid,
            to: from,
            msg: HeartbeatMsg::Reply(hb_reply),
        });
    }

    fn handle_reply(&mut self, rep: HeartbeatReply) {
        if rep.round == self.hb_round && rep.ballot.config_id == self.configuration_id {
            self.ballots.push((rep.ballot, rep.connectivity));
        } else {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Got late response, round {}, ballot {:?}", self.hb_round, rep.ballot
            );
        }
    }

    #[cfg(feature = "ui")]
    pub(crate) fn get_current_ballot(&self) -> Ballot {
        self.current_ballot
    }

    #[cfg(feature = "ui")]
    pub(crate) fn get_ballots(&self) -> &Vec<(Ballot, Connectivity)> {
        &self.prev_round_ballots
    }
}

/// Configuration for `BallotLeaderElection`.
/// # Fields
/// * `configuration_id`: The identifier for the configuration that this node is part of.
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `priority`: Set custom priority for this node to be elected as the leader.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct BLEConfig {
    configuration_id: ConfigurationId,
    pid: NodeId,
    peers: Vec<NodeId>,
    priority: u32,
    flexible_quorum: Option<FlexibleQuorum>,
    buffer_size: usize,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
}

impl From<OmniPaxosConfig> for BLEConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();

        Self {
            configuration_id: config.cluster_config.configuration_id,
            pid,
            peers,
            priority: config.server_config.leader_priority,
            flexible_quorum: config.cluster_config.flexible_quorum,
            buffer_size: BLE_BUFFER_SIZE,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
        }
    }
}
