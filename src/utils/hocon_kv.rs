/// The identifier for the configuration that this Omni-Paxos replica is part of.
pub const CONFIG_ID: &str = "config_id";
/// The identifier of this replica.
pub const PID: &str = "pid";
/// A fixed delay between heartbeats. It is measured in ticks.
pub const HB_DELAY: &str = "hb_delay";
/// A fixed delay that is added to the hb_delay. It is measured in ticks.
pub const INCREMENT_DELAY: &str = "increment_delay";
/// A factor used in the beginning for a shorter hb_delay.
pub const INITIAL_DELAY_FACTOR: &str = "initial_delay_factor";
/// Path where the default logger logs events.
pub const LOG_FILE_PATH: &str = "log_file_path";
