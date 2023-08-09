use crate::ballot_leader_election::Ballot;
use crate::OmniPaxosConfig;
use omnipaxos_ui::app::{Node, UIAppConfig};

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
