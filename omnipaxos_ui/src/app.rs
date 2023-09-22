use crate::util::{defaults::*, ColorGenerator};
use omnipaxos::util::{ConfigurationId, NodeId};
use ratatui::style::Color;
use std::time::Instant;

/// Basic information of a node.
#[derive(Debug, Clone, Default)]
pub(crate) struct Node {
    pub(crate) pid: NodeId,
    pub(crate) configuration_id: ConfigurationId,
    pub(crate) ballot_number: u32,
    pub(crate) connectivity: u8,
    pub(crate) color: Color,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
            && self.configuration_id == other.configuration_id
            && self.ballot_number == other.ballot_number
    }
}

/// Ui application, containing the ui states
pub(crate) struct App {
    /// The current node.
    pub(crate) current_node: Node,
    /// Leader of the current node.
    pub(crate) current_leader: Option<NodeId>,
    /// Color of the leader.
    pub(crate) leader_color: Color,
    /// Role of the current node.
    pub(crate) current_role: Role,
    /// Max index of the decided log entry.
    pub(crate) decided_idx: u64,
    /// Ids of all the nodes in the cluster specified in the configuration, includes the current node.
    pub(crate) nodes: Vec<Node>,
    /// All the active nodes in the cluster that current node is connected to.
    pub(crate) active_peers: Vec<Node>,
    /// The last time the ui states were updated.
    last_update_time: Instant,
    /// The throughput data of the current node, (current ballot, throughput).
    pub(crate) throughput_data: Vec<(String, u64)>,
    /// Number of decided log entries per second, calculated from throughput_data.
    pub(crate) dps: f64,
    /// The progress of all the followers, calculated by accepted_idx / leader’s accepted index.
    /// Calculated only when the current node is the leader. Idx is the pid of the node.
    pub(crate) followers_progress: Vec<f64>,
    /// The accepted_idx of all the followers. Idx is the pid of the node.
    pub(crate) followers_accepted_idx: Vec<u64>,
    // Color generator for the current node, the first color is for the current node.
    // pub(crate) color_generator: ColorGenerator,
}

impl App {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        let max_peer_pid = config.peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &config.pid) as usize;
        let mut color_generator = ColorGenerator::new(COLORS.to_vec());
        let current_node = Node {
            pid: config.pid,
            configuration_id: config.configuration_id,
            ballot_number: 0,
            connectivity: 0,
            color: *color_generator.next_color(),
        };
        let mut nodes: Vec<Node> = config
            .peers
            .clone()
            .into_iter()
            .map(|pid| Node {
                pid,
                color: *color_generator.next_color(),
                ..Default::default()
            })
            .collect();
        nodes.push(current_node.clone());
        nodes.sort_by(|a, b| a.pid.cmp(&b.pid));
        Self {
            current_node,
            current_leader: None,
            leader_color: Default::default(),
            current_role: Role::Follower,
            decided_idx: 0,
            active_peers: Vec::with_capacity(config.peers.len()),
            nodes,
            last_update_time: Instant::now(),
            throughput_data: Vec::with_capacity(THROUGHPUT_DATA_SIZE),
            dps: 0.0,
            followers_progress: vec![0.0; max_pid + 1],
            followers_accepted_idx: vec![0; max_pid + 1],
        }
    }

    pub(crate) fn set_decided_idx(&mut self, decided_idx: u64) {
        let period = self.last_update_time.elapsed().as_secs_f64();
        let throughput = decided_idx - self.decided_idx;
        self.throughput_data
            .insert(0, (throughput.to_string(), throughput));
        // temp: only calculate dps from the last tick.
        self.dps = (throughput as f64) / period;
        self.last_update_time = Instant::now();
        self.decided_idx = decided_idx;
    }
}

pub struct UIAppConfig {
    pub(crate) pid: u64,
    pub(crate) peers: Vec<u64>,
    pub(crate) configuration_id: ConfigurationId,
}
