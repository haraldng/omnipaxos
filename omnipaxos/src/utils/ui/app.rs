use crate::ballot_leader_election::Ballot;
use crate::OmniPaxosConfig;
use crate::util::ConfigurationId;

// Ui application, containing the ui states
pub(crate) struct App {
    pub(crate) pid: u64,
    pub(crate) peers: Vec<u64>,
    pub(crate) configuration_id: ConfigurationId,
    pub(crate) current_leader: Option<Ballot>,
}

impl App {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        Self {
            pid: config.pid,
            peers: config.peers,
            configuration_id: config.configuration_id,
            current_leader: None,
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

