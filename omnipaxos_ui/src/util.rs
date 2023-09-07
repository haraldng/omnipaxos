use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::OmniPaxosConfig;
use crate::app::{Node, UIAppConfig};

pub(crate) mod defaults{
    pub(crate) const UI_TITLE: &str = "OmniPaxos";
    pub(crate) const UI_THROUGHPUT_TITLE: &str = "Throughput: ";
    pub(crate) const UI_TABLE_TITLE: &str = "Active peers";
    // Width of each rectangle that represents a node
    pub(crate) const UI_CANVAS_NODE_WIDTH: f64 = 15.0;
    // Radius of the circle that represents the cluster
    pub(crate) const UI_CANVAS_RADIUS: f64 = 50.0;
    // Max number of throughput data to be stored, and displayed on the bar chart
    pub(crate) const THROUGHPUT_DATA_SIZE: usize = 200;
    pub(crate) const UI_BARCHART_WIDTH: u16 = 3;
    pub(crate) const UI_BARCHART_GAP: u16 = 1;
    pub(crate) const UI_TABLE_TITLE_HEIGHT: u16 = 1;
    pub(crate) const UI_TABLE_CONTENT_HEIGHT: u16 = 1;
    pub(crate) const UI_TABLE_ROW_MARGIN: u16 = 1;

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