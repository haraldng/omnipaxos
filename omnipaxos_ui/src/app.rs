use crate::util::defaults::*;
use std::time::Instant;
use omnipaxos::util::{ConfigurationId, NodeId};

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub pid: NodeId,
    pub configuration_id: ConfigurationId,
    pub ballot_number: u32,
    pub connectivity: u8,
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
    /// The current node.
    pub current_node: Node,
    /// Leader of the current node.
    pub current_leader: Option<NodeId>,
    /// Max index of the decided log entry.
    pub decided_idx: u64,
    /// Ids of all the nodes in the cluster specified in the configuration, does not include the current node.
    pub peers: Vec<NodeId>,
    /// All the active nodes in the cluster that current node is connected to.
    pub active_peers: Vec<Node>,
    /// The last time the ui states were updated.
    last_update_time: Instant,
    /// The throughput data of the current node, (sub, throughput).
    pub(crate) throughput_data: Vec<(String, u64)>,
    /// Number of decided log entries per second, calculated from throughput_data.
    pub(crate) dps: f64,
}

impl App {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        Self {
            current_node: Node {
                pid: config.pid,
                configuration_id: config.configuration_id,
                ballot_number: 0,
                connectivity: 0,
            },
            current_leader: None,
            decided_idx: 0,
            active_peers: Vec::with_capacity(config.peers.len()),
            peers: config.peers,
            last_update_time: Instant::now(),
            throughput_data: Vec::with_capacity(THROUGHPUT_DATA_SIZE),
            dps: 0.0,
        }
    }

    pub fn set_decided_idx(&mut self, decided_idx: u64) {
        let period = self.last_update_time.elapsed().as_secs_f64();
        let throughput = decided_idx - self.decided_idx;
        self.throughput_data
            .insert(0, (self.current_node.ballot_number.to_string(), throughput));
        // temp: only calculate dps from the last tick.
        self.dps = (throughput as f64) / period;
        self.last_update_time = Instant::now();
        self.decided_idx = decided_idx;
    }
}

pub struct UIAppConfig {
    pub pid: u64,
    pub peers: Vec<u64>,
    pub configuration_id: ConfigurationId,
}
