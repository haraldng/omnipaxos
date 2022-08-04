//! An library of storage implementations for sequencePaxos, provides
//! in-memory storage implementation with fast read and writes, and 
//! an persistent storage implementation with persistence for the 
//! replica state and the log through disk storage.

//#![deny(missing_docs)]
pub mod memory;
