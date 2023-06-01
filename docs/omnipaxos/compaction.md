As time passes, the replicated log in `OmniPaxos` will grow large. To avoid letting the log growing infinitely large, we support two ways of compaction that can be initiated by users:

## Trim
Trimming the log removes all entries up to a certain index. Since the entries are deleted from the log, a trim operation can only be done if **ALL** nodes in the cluster have decided up to that index. Example:

```rust
use omnipaxos::sequence_paxos::CompactionErr;

// we will try trimming the first 100 entries of the log.
let trim_idx = Some(100);  // using `None` will use the highest trimmable index
match omni_paxos.trim(trim_idx) {
    Ok(_) => {
        // later, we can see that the trim succeeded with `omni_paxos.get_compacted_idx()`
    }
    Err(e) => {
        match e {
            CompactionErr::NotAllDecided(idx) => {
                // Our provided trim index was not decided by all servers yet. All servers have currently only decided up to `idx`.
                // If desired, users can retry with omni_paxos.trim(Some(idx)) which will then succeed.
            }
            ...
        }
    }
}
``` 

> **Note:** Make sure your application really does not need the data that will be trimmed anymore. Once it is succeeded, the trimmed entries are lost and cannot be read or recovered.

## Snapshot
Trimming compacts the log and discards any data preceding the trim index. For safety, it therefore requires all servers to have decided the trim index. If you don't want to discard any data and the entries in the log are such that they can be compacted into a snapshot, `OmniPaxos` supports snapshotting decided entries of the log. For instance, in our kv-store example, we don't need to keep every log entry that changes the kv-pairs. Instead, if we want to snapshot the log, it is sufficient to keep the latest value for every key. We implement our snapshot as a struct called `KVSnapshot` which is just a wrapper for a `HashMap` that will hold the latest value for every key in the log. To make it work with `OmniPaxos`, we need to implement the trait `Snapshot` for `KVSnapshot`:

```rust
use std::collections::HashMap;
use omnipaxos::storage::Snapshot;

#[derive(Clone, Debug)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>
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
``` 

The ``create()`` function tells `OmniPaxos` how to create a snapshot given a slice of entries of our `KeyValue` type. In our case, we simply want to insert the kv-pair into the hashmap. The `merge()` function defines how we can merge two snapshots. In our case, we will just insert/update the kv-pairs from the other snapshot. The `use_snapshots()` function tells `OmniPaxos` if snapshots should be used in the protocol. 

With ``KVSnapshot``, we would have instead implemented our [`KeyValue`](../index.md) that we defined earlier like this:
```rust
use omnipaxos::storage::Entry;

#[derive(Clone, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

impl Entry for KeyValue {
    type Snapshot = KVSnapshot;
}
```

> **Note:** If you do not wish to use snapshots, then simply derive the blanket implementation for `Entry` using the macro we showed [here](../index.md)

We can now create snapshots and read snapshots from `OmniPaxos`. Furthermore, snapshotting allows us to either just do the snapshot locally or request all nodes in the cluster to do it with the boolean parameter `local_only`.
```rust
// we will try snapshotting the first 100 entries of the log.
let snapshot_idx = Some(100);  // using `None` will use the highest snapshottable index
let local_only = false; // snapshots will be taken by all nodes.
match omni_paxos.snapshot(snapshot_idx, local_only) {
    Ok(_) => {
        // later, we can see that the snapshot succeeded with `omni_paxos.get_compacted_idx()`
    }
    Err(e) => {
        match e {
            CompactionErr::UndecidedIndex(idx) => {
                // Our provided snapshot index is not decided yet. The currently decided index is `idx`.
            }
            ...
        }
    }
}

// reading a snapshotted entry
if let Some(e) = omni_paxos.read(20) {
    match e {
        LogEntry::Snapshotted(s) => {
            // entry at idx 20 is snapshotted since we snapshotted idx 100
            let snapshotted_idx = s.trimmed_idx;
            let snapshot: KVSnapshot = s.snapshot;
            // ...can query the latest value for a key in snapshot
        }
        ...
}
```

