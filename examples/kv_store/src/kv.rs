use omnipaxos::{macros::Entry, storage::Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Entry, Clone, Debug, Serialize, Deserialize)]
#[snapshot(KVSnapshot)]
// if we do not want to use snapshots, we can simply derive the Entry trait for KeyValue.
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
            let KeyValue { key, value } = e;
            snapshotted.insert(key.clone(), *value);
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
