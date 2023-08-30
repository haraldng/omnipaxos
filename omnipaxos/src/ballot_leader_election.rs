use std::cmp::Ordering;

/// Ballot Leader Election algorithm for electing new leaders
use crate::{
    sequence_paxos::{Phase, Role},
    util::{defaults::*, ConfigurationId, FlexibleQuorum, Quorum},
};

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
    /// The heartbeat replies this instance received during the current round
    heartbeat_replies: Vec<HeartbeatReply>,
    /// The current ballot of this instance.
    current_ballot: Ballot,
    /// We are connected to the highest seen ballot and the owner of this ballot can make progress
    /// as the leader of the cluster
    happy: bool,
    /// Current elected leader.
    leader: Ballot,
    /// The number of replicas inside the cluster whose heartbeats are needed to become and remain the leader.
    quorum: Quorum,
    /// Keeps track of whether we have already signaled to Omnipaxos that we have become the leader
    /// of the cluster
    already_signaled_leadership: bool,
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
            heartbeat_replies: Vec::with_capacity(num_nodes),
            current_ballot: initial_ballot,
            happy: true,
            // TODO: use initial_leader and remove default ballot
            leader: initial_ballot,
            quorum,
            outgoing: Vec::with_capacity(config.buffer_size),
            #[cfg(feature = "logging")]
            logger: {
                let s = config
                    .logger_file_path
                    .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                create_logger(s.as_str())
            },
            // TODO: what if we were leader and established when we crashed?
            already_signaled_leadership: false,
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
                ballot: self.current_ballot,
                happy: self.happy,
            };
            self.outgoing.push(BLEMessage {
                from: self.pid,
                to: *peer,
                msg: HeartbeatMsg::Request(hb_request),
            });
        }
    }

    pub(crate) fn hb_timeout(&mut self, seq_paxos_state: &(Role, Phase)) -> Option<Ballot> {
        let mut tell_paxos = false;

        // Check replies for new leader
        let max_reply = self
            .heartbeat_replies
            .iter()
            .max_by(|r1, r2| r1.ballot.cmp(&r2.ballot));
        if let Some(reply) = max_reply {
            if reply.ballot > self.leader {
                self.leader = reply.ballot;
                self.happy = reply.happy;
                self.already_signaled_leadership = false;
            }
        }

        // Update happiness
        self.happy = if self.current_ballot == self.leader {
            let followers = self
                .heartbeat_replies
                .iter()
                .filter(|hb_reply| hb_reply.leader == self.current_ballot)
                .count()
                + 1;
            let happy = match seq_paxos_state {
                (Role::Leader, Phase::Accept) => self.quorum.is_accept_quorum(followers),
                _ => self.quorum.is_prepare_quorum(followers),
            };
            if happy && !self.already_signaled_leadership {
                self.already_signaled_leadership = true;
                tell_paxos = true;
            }
            happy
        } else {
            let leaders_reply = self
                .heartbeat_replies
                .iter()
                .find(|reply| reply.ballot.pid == self.leader.pid);
            match leaders_reply {
                Some(reply) if reply.ballot >= self.leader => reply.happy,
                // Must have learned of leader ballot from HeartbeatRequest so HeartbeatReply is stale
                Some(_) => self.happy,
                None => false,
            }
        };

        // Increase rule
        if !self.happy {
            let all_neighbors_unhappy = self.heartbeat_replies.iter().all(|r| !r.happy);
            let im_quorum_connected = self
                .quorum
                .is_prepare_quorum(self.heartbeat_replies.len() + 1);
            if all_neighbors_unhappy && im_quorum_connected {
                self.current_ballot.n = self.leader.n + 1;
                self.leader = self.current_ballot;
                self.happy = true;
                self.already_signaled_leadership = false;
            }
        }

        self.heartbeat_replies.clear();
        self.new_hb_round();
        if tell_paxos {
            Some(self.leader)
        } else {
            None
        }
    }

    fn handle_request(&mut self, from: NodeId, req: HeartbeatRequest) {
        if req.ballot > self.leader {
            self.leader = req.ballot;
            self.happy = req.happy;
            self.already_signaled_leadership = false;
        }
        let hb_reply = HeartbeatReply {
            round: req.round,
            ballot: self.current_ballot,
            leader: self.leader,
            happy: self.happy,
        };
        self.outgoing.push(BLEMessage {
            from: self.pid,
            to: from,
            msg: HeartbeatMsg::Reply(hb_reply),
        });
    }

    fn handle_reply(&mut self, rep: HeartbeatReply) {
        if rep.round == self.hb_round && rep.ballot.config_id == self.configuration_id {
            self.heartbeat_replies.push(rep);
        } else {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Got late response, round {}, ballot {:?}", self.hb_round, rep.ballot
            );
        }
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
