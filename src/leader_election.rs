use std::fmt::Debug;

/// A round in Leader-based Sequence Paxos must be total ordered.
pub trait Round: Clone + Debug + Ord + Default + Send + 'static {}

#[derive(Copy, Clone, Debug)]
pub struct Leader<R>
where
    R: Round,
{
    pub pid: u64,
    pub round: R,
}

impl<R> Leader<R>
where
    R: Round,
{
    pub fn with(pid: u64, round: R) -> Self {
        Leader { pid, round }
    }
}
