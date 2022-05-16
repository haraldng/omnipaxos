use crate::leader_election::Round;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
/// Promise without the suffix
pub(crate) struct PromiseMetaData<R>
where
    R: Round,
{
    pub n: R,
    pub la: u64,
    pub pid: u64,
}

impl<R> PromiseMetaData<R>
where
    R: Round,
{
    pub fn with(n: R, la: u64, pid: u64) -> Self {
        Self { n, la, pid }
    }
}

impl<R> Ord for PromiseMetaData<R>
where
    R: Round,
{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.n == other.n && self.la == other.la && self.pid == other.pid {
            Ordering::Equal
        } else if self.n > other.n && self.la > other.la {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl<R> PartialOrd for PromiseMetaData<R>
where
    R: Round,
{
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

impl<R> PartialEq for PromiseMetaData<R>
where
    R: Round,
{
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n && self.la == other.la && self.pid == other.pid
    }
}

impl<R> Eq for PromiseMetaData<R> where R: Round {}
