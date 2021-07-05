use std::fmt::Debug;

/// Rounds in Omni-Paxos must be totally ordered.
pub trait Round: Clone + Debug + Ord + Default + Send + 'static {}

/// Leader event that indicates a leader has been elected. Should be created when the user-defined BLE algorithm
/// outputs a leader event. Should be then handled in Omni-Paxos by calling [`crate::paxos::Paxos::handle_leader()`].
#[derive(Copy, Clone, Debug)]
pub struct Leader<R>
where
    R: Round,
{
    /// The pid of the elected leader.
    pub pid: u64,
    /// The round in which `pid` is elected in.
    pub round: R,
}

impl<R> Leader<R>
where
    R: Round,
{
    /// Constructor for [`Leader`].
    pub fn with(pid: u64, round: R) -> Self {
        Leader { pid, round }
    }
}
