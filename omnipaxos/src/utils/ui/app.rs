use crate::ballot_leader_election::{Ballot, Connectivity};
use crate::util::{ConfigurationId, NodeId};
use crate::OmniPaxosConfig;

#[derive(Debug, Clone)]
pub(crate) struct  Node {
    pub(crate) pid: NodeId,
    pub(crate) configuration_id: u32,
    pub(crate) ballot_number: u32,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            pid: 0,
            configuration_id: 0,
            ballot_number: 0,
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid &&
        self.configuration_id == other.configuration_id &&
        self.ballot_number == other.ballot_number
    }
}

impl From<Ballot> for Node {
    fn from(ballot: Ballot) -> Self {
        Self {
            pid: ballot.pid,
            configuration_id: ballot.config_id,
            ballot_number: ballot.n,
            ..Default::default()
        }
    }
}

/// Ui application, containing the ui states
pub(crate) struct App {
    // The current node.
    pub(crate) current_node: Node,
    // Leader of the current node.
    pub(crate) current_leader: Option<NodeId>,
    // Max index of the decided log entry.
    pub(crate) decided_idx: u64,
    // Ids of all the nodes in the cluster specified in the configuration, does not include the current node.
    pub(crate) peers: Vec<NodeId>,
    // All the active nodes in the cluster that current node is connected to.
    pub(crate) active_peers: Vec<Node>,
}

impl App {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        Self {
            current_node: Node {
                pid: config.pid,
                configuration_id: config.configuration_id,
                ballot_number: 0,
            },
            current_leader: None,
            decided_idx: 0,
            active_peers: Vec::with_capacity(config.peers.len()),
            peers: config.peers,
        }
    }
}

pub(crate) struct UIAppConfig {
    pid: u64,
    peers: Vec<u64>,
    configuration_id: ConfigurationId,
}

impl From<OmniPaxosConfig> for UIAppConfig {
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
        }
    }
}
