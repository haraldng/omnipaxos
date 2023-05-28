use omnipaxos::storage::{Snapshot, Entry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

/// Same as in network actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVCommand {
    Put(KeyValue),
    Delete(String),
}

impl Entry for KVCommand {
    type Snapshot = KVSnapshot;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, String>,
}

impl Snapshot<KVCommand> for KVSnapshot {
    fn create(entries: &[KVCommand]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            match e {
                KVCommand::Put(KeyValue { key, value }) => {
                    snapshotted.insert(key.clone(), value.clone());
                },
                KVCommand::Delete(key) => {
                    snapshotted.remove(key);
                },
            }
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}
