//! OmniPaxos is a library for implementing distributed replicated logs with strong consistency guarantees
//! that provides seamless reconfiguration while also being completely resilient to partial network partitions.
//! This library provides the distributed log abstraction as a black-box for the user, where the user only has to
//! provide its desired network and storage implementations.
//!
//! # Crate feature flags
//! The following crate feature flags are available. They are configured in your Cargo.toml.
//! * `batch_accept` - Batch multiple log entries into a single message to reduce overhead.
//! * `continued_leader_reconfiguration` - Let the cluster pick the current leader as the initial leader in the new configuration (if possible) to shorten down-time during reconfiguration.
//! * `logging` - System-wide logging with the sled crate
//! * `toml_config` - Create an OmniPaxos instance from a TOML configuration file

#![deny(missing_docs)]
/// Trait and struct related to the leader election in Omni-Paxos.
pub mod ballot_leader_election;
/// OmniPaxos error definitions
#[cfg(feature = "toml_config")]
pub mod errors;
/// The different messages Omni-Paxos replicas can communicate to each other with.
pub mod messages;
/// The user-facing Omni-Paxos struct.
pub mod omni_paxos;
pub(crate) mod sequence_paxos;
/// The core replication algorithm of Omni-Paxos.
// pub mod sequence_paxos;
/// Traits and structs related to the backend storage of an Omni-Paxos replica.
pub mod storage;
/// A module containing helper functions and structs.
pub mod util;
pub(crate) mod utils;
