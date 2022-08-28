//! A library of storage implementations for SequencePaxos

#![deny(missing_docs)]
/// an in-memory storage implementation with fast read and writes
pub mod memory_storage;
/// an on-disk storage implementation with persistence for the replica state and the log.
pub mod persistent_storage;
