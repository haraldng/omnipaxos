//! A tokio runtime for omnipaxos.

#![deny(missing_docs)]
/// The user-facing omnipaxos_runtime
pub mod omnipaxos;
mod ballot_leader_election;
mod sequence_paxos;
mod util;
