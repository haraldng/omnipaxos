use crate::ballot_leader_election::{Ballot, Connectivity};
use crate::util::ConfigurationId;
use crate::OmniPaxosConfig;

/// Ui application, containing the ui states
pub(crate) struct App {
    // Id of the current node.
    pub(crate) pid: u64,
    // Ballot of the current node.
    pub(crate) ballot: Ballot,
    // Ids of all the nodes in the cluster.
    pub(crate) peers: Vec<u64>,
    // Id of the configuration of current node.
    pub(crate) configuration_id: ConfigurationId,
    // Leader of the current node.
    pub(crate) current_leader: Option<Ballot>,
    // Max index of the decided log entry.
    pub(crate) decided_idx: u64,
    // Ballots of the nodes in the cluster that are active, including the current node.
    pub(crate) ballots: Vec<(Ballot, Connectivity)>,
}

impl App {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        Self {
            pid: config.pid,
            peers: config.peers,
            configuration_id: config.configuration_id,
            current_leader: None,
            decided_idx: 0,
            ballot: Default::default(),
            ballots: vec![],
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
