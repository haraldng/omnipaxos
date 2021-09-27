# Garbage Collection

In some cases, the decided sequence can become very large and a portion of the sequence needs to be deleted. To solve this Omnipaxos offers a manual garbage collection.

```rust,edition2018,no_run,noplaypen
// `index` - Deletes all entries up to index, if the index is None then the minimum index accepted by ALL servers will be used as the index.
crate::omnipaxos::OmniPaxos::garbage_collect(index)
```

The function can be called from any replica. If the replica which initiates the garbage collection is not the leader then it will redirect the proposal for garbage collection to the leader which in turn will initiate the garbage collection for all the replicas in the system.

> **Note:** At the moment there is no way of knowing if the garbage collection has been completed successfully. The only method would be to get the garbage_collected_idx and see if it is equal to the given index.
