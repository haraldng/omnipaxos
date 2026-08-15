use omnipaxos::{macros::Entry, storage::Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Entry, Clone, Debug, Serialize, Deserialize)]
#[snapshot(KVSnapshot)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            snapshotted.insert(e.key.clone(), e.value);
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
