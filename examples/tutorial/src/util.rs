use std::time::Duration;

pub const NODE1_CONFIG_PATH: &str = "examples/tutorial/src/configs/node1.toml";
pub const STORAGE_BASE_PATH: &str = "examples/tutorial/storage/";

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(100);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);

pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);
