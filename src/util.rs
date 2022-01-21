use crate::{
    leader_election::ballot_leader_election::Ballot,
    storage::{Snapshot, SnapshotType, StopSign},
};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
/// Promise without the suffix
pub(crate) struct PromiseMetaData {
    pub n: Ballot,
    pub la: u64,
    pub pid: u64,
    pub stopsign: Option<StopSign>,
}

impl PromiseMetaData {
    pub fn with(n: Ballot, la: u64, pid: u64, stopsign: Option<StopSign>) -> Self {
        Self {
            n,
            la,
            pid,
            stopsign,
        }
    }
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n == other.n && self.la == other.la && self.pid == other.pid {
            Ordering::Equal
        } else if self.n > other.n || (self.n == other.n && self.la > other.la) {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n && self.la == other.la && self.pid == other.pid
    }
}

#[derive(Debug, Clone)]
pub enum SyncItem<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    Entries(Vec<T>),
    Snapshot(SnapshotType<T, S>),
    None,
}
