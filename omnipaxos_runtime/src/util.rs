use crate::util::defaults::TICK_INTERVAL;
use std::time::Duration;

pub(crate) struct Stop;

pub(crate) fn duration_to_num_ticks(d: Duration) -> u64 {
    d.as_millis() as u64 / TICK_INTERVAL.as_millis() as u64
}

pub(crate) mod defaults {
    use std::time::Duration;
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const HB_TIMEOUT: u64 = 2000;
    /// tick() is called every `TICK_INTERVAL` in async omnipaxos_runtime
    pub(crate) const TICK_INTERVAL: Duration = Duration::from_millis(10);
}
