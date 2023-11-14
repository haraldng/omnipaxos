use crate::{util::defaults::*, UIAppConfig};
use omnipaxos::util::NodeId;
use ratatui::style::Color;
use std::time::Instant;

/// Basic information of a node.
#[derive(Debug, Clone, Default)]
pub(crate) struct Node {
    pub(crate) pid: NodeId,
    pub(crate) ballot_number: u32,
    pub(crate) leader: NodeId,
    pub(crate) color: Color,
    pub(crate) connected: bool,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid && self.ballot_number == other.ballot_number
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
    pub(crate) decided_idx: usize,
    /// Ids of all the nodes in the cluster specified in the configuration, includes the current node.
    pub(crate) nodes: Vec<Node>,
    /// All the active nodes in the cluster that current node is connected to.
    pub(crate) active_peers: Vec<Node>,
    /// The last time the ui states were updated.
    last_update_time: Instant,
    /// The throughput data of the current node, (current ballot, throughput).
    pub(crate) throughput_data: Vec<(String, usize)>,
    /// Number of decided log entries per second, calculated from throughput_data.
    pub(crate) dps: f64,
    /// The progress of all the followers, calculated by accepted_idx / leader’s accepted index.
    /// Calculated only when the current node is the leader. Idx is the pid of the node.
    pub(crate) followers_progress: Vec<f64>,
    /// The accepted_idx of all the followers. Idx is the pid of the node.
    pub(crate) followers_accepted_idx: Vec<usize>,
}

impl App {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        let max_peer_pid = config.peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &config.pid) as usize;
        let mut current_node = Node {
            pid: config.pid,
            ballot_number: 0,
            leader: 0,
            ..Default::default()
        };
        let mut peers = config.peers.clone();
        peers.push(config.pid);
        peers.sort();
        let nodes: Vec<Node> = peers
            .into_iter()
            .enumerate()
            .map(|(idx, pid)| {
                let node = Node {
                    pid,
                    color: COLORS[idx % COLORS.len()],
                    ..Default::default()
                };
                if pid == config.pid {
                    current_node.color = node.color;
                }
                node
            })
            .collect();
        let active_peers = nodes
            .iter()
            .filter(|x| x.pid != config.pid)
            .cloned()
            .collect();
        Self {
            current_node,
            current_leader: None,
            leader_color: Default::default(),
            current_role: Role::Follower,
            decided_idx: 0,
            active_peers,
            nodes,
            last_update_time: Instant::now(),
            throughput_data: Vec::with_capacity(THROUGHPUT_DATA_SIZE),
            dps: 0.0,
            followers_progress: vec![0.0; max_pid + 1],
            followers_accepted_idx: vec![0; max_pid + 1],
        }
    }

    pub(crate) fn set_decided_idx(&mut self, decided_idx: usize) {
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
