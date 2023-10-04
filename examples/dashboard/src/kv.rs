use omnipaxos::macros::Entry;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
// if we do not want to use snapshots, we can simply derive the Entry trait for KeyValue.
pub struct LogEntry(pub u64);
