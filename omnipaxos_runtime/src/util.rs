use std::time::Duration;
use omnipaxos_core::{
    storage::{Entry, Snapshot, StopSign},
    util::{LogEntry, SnapshottedEntry, TrimmedEntry},
};
use crate::util::defaults::TICK_INTERVAL;

/// Used for reading in the async omnipaxos_runtime. Note that every read does a clone.
#[derive(Debug, Clone)]
pub enum ReadEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedEntry),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T, S>),
    /// This Sequence Paxos instance has been stopped for reconfiguration.
    StopSign(StopSign),
}

impl<'a, T, S> From<LogEntry<'a, T, S>> for ReadEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    fn from(e: LogEntry<'a, T, S>) -> Self {
        match e {
            LogEntry::Decided(t) => Self::Decided(t.clone()),
            LogEntry::Undecided(t) => Self::Undecided(t.clone()),
            LogEntry::Trimmed(t) => Self::Trimmed(t),
            LogEntry::Snapshotted(s) => Self::Snapshotted(s),
            LogEntry::StopSign(ss) => Self::StopSign(ss),
        }
    }
}

pub(crate) struct Stop;

pub(crate) fn duration_to_num_ticks(d: Duration) -> u64 {
    d.as_millis() as u64 / TICK_INTERVAL.as_millis() as u64
}

pub(crate) mod defaults {
    use std::time::Duration;
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const HB_TIMEOUT: u64 = 500;
    /// tick() is called every `TICK_INTERVAL` in async omnipaxos_runtime
    pub(crate) const TICK_INTERVAL: Duration = Duration::from_millis(10);
}