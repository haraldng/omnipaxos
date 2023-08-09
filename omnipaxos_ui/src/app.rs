use crate::util::*;

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub pid: NodeId,
    pub configuration_id: ConfigurationId,
    pub ballot_number: u32,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
            && self.configuration_id == other.configuration_id
            && self.ballot_number == other.ballot_number
    }
}

/// Ui application, containing the ui states
pub struct App {
    // The current node.
    pub current_node: Node,
    // Leader of the current node.
    pub current_leader: Option<NodeId>,
    // Max index of the decided log entry.
    pub decided_idx: u64,
    // Ids of all the nodes in the cluster specified in the configuration, does not include the current node.
    pub peers: Vec<NodeId>,
    // All the active nodes in the cluster that current node is connected to.
    pub active_peers: Vec<Node>,
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

pub struct UIAppConfig {
    pub pid: u64,
    pub peers: Vec<u64>,
    pub configuration_id: ConfigurationId,
}
