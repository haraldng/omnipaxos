#[cfg(feature = "derive_entry")]
use omnipaxos::macros::Entry;
#[cfg(not(feature = "derive_entry"))]
use omnipaxos::storage::Entry;
#[cfg(not(feature = "derive_entry"))]
use omnipaxos::storage::Snapshot;
use serde::{Deserialize, Serialize};
#[cfg(not(feature = "derive_entry"))]
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "derive_entry", derive(Entry))]
// if we do not want to use snapshots, we can simply derive the Entry trait for KeyValue.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[cfg(not(feature = "derive_entry"))]
impl Entry for KeyValue {
    // we can also use snapshots by implementing the Entry trait and manually setting the Snapshot type.
    type Snapshot = KVSnapshot;
}

#[cfg(not(feature = "derive_entry"))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}
#[cfg(not(feature = "derive_entry"))]
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
