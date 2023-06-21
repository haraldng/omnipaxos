//! OmniPaxos is a library for implementing distributed replicated logs with strong consistency guarantees
//! that provides seamless reconfiguration while also being completely resilient to partial network partitions.
//! This library provides the distributed log abstraction as a black-box for the user, where the user only has to
//! provide its desired network and storage implementations.
//!
//! # Crate feature flags
//! The following crate feature flags are available. They are configured in your Cargo.toml.
//! * `batch_accept` - Batch multiple log entries into a single message to reduce overhead.
//! * `logging` - System-wide logging with the slog crate
//! * `toml_config` - Create an OmniPaxos instance from a TOML configuration file
//! * `serde` - Serialization and deserialization of messages and internal structs with serde. Disable this if you want to implement your own custom ser/deserialization or want to store data that is not serde-supported.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![deny(missing_docs)]
/// Trait and struct related to the leader election in Omni-Paxos.
pub mod ballot_leader_election;
/// OmniPaxos error definitions
pub mod errors;
/// The different messages OmniPaxos servers can communicate to each other with.
pub mod messages;
/// The user-facing OmniPaxos struct.
mod omni_paxos;
/// Re-export the contents of the omni_paxos module
pub use omni_paxos::*;

pub(crate) mod sequence_paxos;
/// The core replication algorithm of OmniPaxos.
// pub mod sequence_paxos;
/// Traits and structs related to the backend storage of an OmniPaxos server.
pub mod storage;
/// A module containing helper functions and structs.
pub mod util;
pub(crate) mod utils;

#[cfg(feature = "macros")]
#[allow(unused_imports)]
#[macro_use]
extern crate omnipaxos_macros;

#[cfg(feature = "macros")]
/// Macros in the omnipaxos crate
pub mod macros {
    #[doc(hidden)]
    pub use omnipaxos_macros::*;
}
