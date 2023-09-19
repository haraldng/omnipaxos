use crate::app::{Node, UIAppConfig};
use omnipaxos::{ballot_leader_election::Ballot, OmniPaxosConfig};
use ratatui::prelude::Color;

pub(crate) mod defaults {
    use ratatui::prelude::Color;

    pub(crate) const UI_TITLE: &str = "OmniPaxos";
    pub(crate) const UI_THROUGHPUT_TITLE: &str = "Throughput";
    pub(crate) const UI_TABLE_TITLE: &str = "Active peers";
    pub(crate) const UI_NODE_INFO_TITLE: &str = "Current node information";
    pub(crate) const UI_CLUSTER_INFO_TITLE: &str = "Cluster information";
    pub(crate) const UI_LOGGING_TITLE: &str = "System log";
    pub(crate) const THROUGHPUT_DATA_SIZE: usize = 200;
    pub(crate) const UI_BARCHART_WIDTH: u16 = 3;
    pub(crate) const UI_BARCHART_GAP: u16 = 1;
    pub(crate) const UI_TABLE_CONTENT_HEIGHT: u16 = 1;
    pub(crate) const UI_TABLE_ROW_MARGIN: u16 = 1;
    pub(crate) const COLORS: [Color; 4] = [Color::Green, Color::Blue, Color::Magenta, Color::Cyan];
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

pub(crate) struct ColorGenerator {
    colors: Vec<Color>,
    index: usize,
}

impl ColorGenerator {
    pub fn new(colors: Vec<Color>) -> Self {
        ColorGenerator { colors, index: 0 }
    }

    pub fn next_color(&mut self) -> &Color {
        if self.colors.is_empty() {
            panic!("No colors provided!");
        }
        let color = &self.colors[self.index];
        self.index = (self.index + 1) % self.colors.len();
        color
    }

    pub fn current_color(&self) -> &Color {
        if self.colors.is_empty() {
            panic!("No colors provided!");
        }

        &self.colors[self.index]
    }
}
