pub(crate) const UI_TITLE: &str = "OmniPaxos";
pub(crate) const UI_THROUGHPUT_TITLE: &str = "Throughput: ";
pub(crate) const UI_TABLE_TITLE: &str = "Active peers";
// Width of each rectangle that represents a node
pub(crate) const UI_CANVAS_NODE_WIDTH: f64 = 15.0;
// Radius of the circle that represents the cluster
pub(crate) const UI_CANVAS_RADIUS: f64 = 50.0;
// Max number of throughput data to be stored, and displayed on the bar chart
pub(crate) const THROUGHPUT_DATA_SIZE: usize = 200;
pub const UI_BARCHART_WIDTH: u16 = 3;
pub const UI_BARCHART_GAP: u16 = 1;

pub(crate) type NodeId = u64;
pub(crate) type ConfigurationId = u32;
