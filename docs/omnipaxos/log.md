We now show how to read and write the replicated log. The replicated log is *append-only*. To append an entry we call the following:

```rust
let write_entry = KeyValue { key: String::from("a"), value: 123 };
omni_paxos.append(write_entry).expect("Failed to append");
```

This will cause our `write_entry` to be proposed to get decided in the replicated log. Appends can be pipelined without waiting for preceding entries to be decided. Furthermore, `append()` can be called on any node. If the calling node is not the leader, the entry will be forwarded. 

## Reading the Log
Reads are also handled by calling various functions on `OmniPaxos`. To read the entry at a specific index `idx` of the log we call `omni_paxos.read_entry(idx)`. We can also read a specific range of log entries with `omni_paxos.read_entries()`. 

```rust
/*** Read a single entry ***/
let idx = 5;
let read_entry = omni_paxos.read(idx);

/*** Read a range ***/
let read_entries = omni_paxos.read_entries(2..5);
``` 

The read functions return `Option<LogEntry>` and `Option<Vec<LogEntry>>` respectively, where `None` is returned if the index or range is out of bounds. `LogEntry` is an enum with the following variants:
- `Decided(T)`: The entry is decided and guaranteed to not be reverted. It is thus safe to apply a decided entry to the application state. For instance, in our case, it is safe to update our key-value store when we read a `Decided(KeyValue)` entry.
- `Undecided(T)`: The entry is NOT decided and might be removed from the log at a later time. However, it could be useful in applications that allow speculative execution for example.
- `Trimmed(TrimmedIndex)`: We tried to read an index where the entry has already been trimmed. 
- `Snapshotted(SnapshottedEntry<T>)`: The index we read has already been compacted into a snapshot. We can access the snapshot from the field `snapshot` in `SnapshottedEntry`. In our case our this will correspond to `KVSnapshot` that we defined [here](../compaction).
- `StopSign(StopSign)`: This Sequence Paxos instance has been stopped for reconfiguration. This implies that this log will not be appended anymore and one should use the new Sequence Paxos instead for writing.

It is also possible to only read decided entries or snapshot from a specific index using `read_decided_suffix(idx)`.



