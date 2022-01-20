use crate::{
    leader_election::ballot_leader_election::Ballot,
    storage::{Entry, Snapshot, SnapshotType},
};

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
/// Promise without the suffix
pub(crate) struct PromiseMetaData {
    pub n: Ballot,
    pub la: u64,
    pub pid: u64,
}

impl PromiseMetaData {
    pub fn with(n: Ballot, la: u64, pid: u64) -> Self {
        Self { n, la, pid }
    }
}

pub enum PromiseType<T, S>
where
    T: Clone,
    S: Snapshot<T>,
{
    Entries(Vec<Entry<T>>),
    Snapshot(SnapshotType<T, S>),
    None,
}

// impl Ord for PromiseMetaData
// where
//     R: Ballotound,
// {
//     fn cmp(&self, other: &Self) -> Ordering {
//         if self.n == other.n && self.la == other.la && self.pid == other.pid {
//             Ordering::Equal
//         } else if self.n > other.n && self.la > other.la {
//             Ordering::Greater
//         } else {
//             Ordering::Less
//         }
//     }
// }

// impl PartialOrd for PromiseMetaData
// {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         let ordering = if self.n == other.n && self.la == other.la && self.pid == other.pid {
//             Ordering::Equal
//         } else if self.n > other.n || (self.n == other.n && self.la > other.la) {
//             Ordering::Greater
//         } else {
//             Ordering::Less
//         };
//         Some(ordering)
//     }
// }

// impl PartialEq for PromiseMetaData
// {
//     fn eq(&self, other: &Self) -> bool {
//         self.n == other.n && self.la == other.la && self.pid == other.pid
//     }
// }
