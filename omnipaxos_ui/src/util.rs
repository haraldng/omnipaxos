use crate::app::{Node, UIAppConfig};
use omnipaxos::{ballot_leader_election::Ballot, OmniPaxosConfig};

pub(crate) mod defaults {
    use ratatui::prelude::Color;

    pub(crate) const UI_TITLE: &str = "OmniPaxos";
    pub(crate) const UI_THROUGHPUT_TITLE: &str = "Throughput";
    pub(crate) const UI_TABLE_TITLE: &str = "Peers";
    pub(crate) const UI_NODE_INFO_TITLE: &str = "Current node information";
    pub(crate) const UI_CLUSTER_INFO_TITLE: &str = "Cluster information";
    pub(crate) const UI_LOGGING_TITLE: &str = "System log";
    pub(crate) const THROUGHPUT_DATA_SIZE: usize = 200;
    pub(crate) const UI_BARCHART_WIDTH: u16 = 3;
    pub(crate) const UI_BARCHART_GAP: u16 = 1;
    pub(crate) const UI_TABLE_CONTENT_HEIGHT: u16 = 1;
    pub(crate) const UI_TABLE_ROW_MARGIN: u16 = 1;
    pub const ORANGE: Color = Color::Indexed(208);
    pub const PINK: Color = Color::Indexed(211);

    pub(crate) const COLORS: [Color; 8] = [
        Color::Green,
        Color::Blue,
        Color::Red,
        ORANGE,
        Color::Cyan,
        Color::Magenta,
        Color::Yellow,
        PINK,
    ];
}

impl From<Ballot> for Node {
    fn from(ballot: Ballot) -> Self {
        Self {
            pid: ballot.pid,
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

        Self { pid, peers }
    }
}
