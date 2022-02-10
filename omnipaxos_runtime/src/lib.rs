//! A tokio runtime for omnipaxos.

#![deny(missing_docs)]
mod ballot_leader_election;
/// The user-facing omnipaxos runtime
pub mod omnipaxos;
mod sequence_paxos;
mod util;
