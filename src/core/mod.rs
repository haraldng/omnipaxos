#![deny(missing_docs)]
/// Trait and struct related to the leader election in Omni-Paxos.
pub mod leader_election;
/// The different messages Omni-Paxos replicas can communicate to each other with.
pub mod messages;
/// The core algorithm of Omni-Paxos.
pub mod paxos;
/// Traits and structs related to the backend storage of an Omni-Paxos replica.
pub mod storage;
/// A module containing helper functions and structs.
pub mod util;
