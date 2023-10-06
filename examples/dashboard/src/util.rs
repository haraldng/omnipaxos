use std::time::Duration;

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);
pub const UI_TICK_PERIOD: Duration = Duration::from_millis(200);
pub const BATCH_SIZE: u64 = 100;
pub const BATCH_PERIOD: Duration = Duration::from_millis(50);
