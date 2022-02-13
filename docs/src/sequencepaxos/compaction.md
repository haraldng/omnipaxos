# Compaction
As time passes, the replicated log in `SequencePaxos` will grow large. To avoid letting the log growing infinitely large, we support two ways of compaction that can be initiated by users:

## Trim
Trimming the log removes all entries up to a certain index. Since the entries are deleted from the log, a trim operation can only be done if **ALL** nodes in the cluster have decided up to that index. Example:

```rust,edition2018,no_run,noplaypen
use omnipaxos_core::sequence_paxos::CompactionErr;

// we will try trimming the first 100 entries of the log.
let trim_idx = Some(100);  // using `None` will use the highest trimmable index
match seq_paxos.trim(trim_idx) {
    Ok(_) => {
        // later, we can see that the trim succeeded with `seq_paxos.get_compacted_idx()`
    }
    Err(e) => {
        match e {
            CompactionErr::NotAllDecided(idx) => {
                // Our provided trim index was not decided by all servers yet. All servers have currently only decided up to `idx`.
                // If desired, users can retry with seq_paxos.trim(Some(idx)) which will then succeed.
            }
            ...
        }
    }
}
``` 

> **Note:** Make sure your application really does not need the data that will be trimmed anymore. Once it is succeeded, the trimmed entries are lost and cannot be read or recovered.

## Snapshot
The disadvantage of trimming is that the data is lost after the trim and it requires all servers to have decided the trim index. If the entries in the log are such that they can be compacted into a snapshot, `SequencePaxos` supports snapshotting decided entries of the log. For instance, in our kv-store example application, we don't need to keep every log entry that changes the kv-pairs. Instead, if we want to snapshot the log, it is sufficient to keep the latest value for every key. We implement our snapshot as a struct called `KVSnapshot` which is just a wrapper for a `HashMap` that will hold the latest value for every key in the log. To make it work with `SequencePaxos`, we need to implement the trait `Snapshot` for `KVSnapshot`:

```rust,edition2018,no_run,noplaypen
use omnipaxos_core::storage::Snapshot;

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

The ``create_entries()`` function tells `SequencePaxos` how to create a snapshot given a slice of entries of our `KeyValue` type. In our case, we simply want to insert the kv-pair into the hashmap. The `merge()` function defines how we can merge two snapshots. In our case, we will just insert/update the kv-pairs from the other snapshot. The `use_snapshots()` function simply tells `SequencePaxos` if snapshots should be used in the protocol. This is needed for users that does not want to use snapshots.

With ``KVSnapshot``, we would have instead created our `SequencePaxos` node as follows:
```rust,edition2018,no_run,noplaypen
// ...same as shown before in the `SequencePaxos` chapter.
let storage = MemoryStorage::<KeyValue, KVSnapshot)>::default();    // use KVSnapshot as type argument instead of ()
let mut sp = SequencePaxos::with(sp_config, storage);
```
We can now create snapshots and read snapshots from `SequencePaxos`. Furthermore, snapshotting allows us to either just do the snapshot locally or request all nodes in the cluster to do it with the boolean parameter `local_only`.
```rust,edition2018,no_run,noplaypen
// we will try snapshotting the first 100 entries of the log.
let snapshot_idx = Some(100);  // using `None` will use the highest snapshottable index
let local_only = false; // snapshots will be taken by all nodes.
match seq_paxos.trim(snapshot_idx, local_only) {
    Ok(_) => {
        // later, we can see that the snapshot succeeded with `seq_paxos.get_compacted_idx()`
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
if let Some(e) = seq_paxos.read(20) {
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

> **Note:** If your `Entry` type is not snapshottable, simply use `()` as type argument for `Snapshot`.