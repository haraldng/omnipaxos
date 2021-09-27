# Storage

OmniPaxos comes with an abstraction layer for storage. This allows the user to store the decided sequences and the state of the replicas in memory or on the storage.

By default, OmniPaxos comes with an in-memory storage option which can be passed as an argument to the OmniPaxos constructor.

```rust,edition2018,no_run,noplaypen
Storage::with(
    MemorySequence::<Ballot>::new(),
    MemoryState::<Ballot>::new(),
)
```

## Custom Storage

Sometimes the default storage provided is not sufficient for a particular application. The user might have more storage than RAM memory or they would like to do additional processing before storing.

To implement a storage component for OmniPaxos the traits `PaxosState<R>` and `Sequence<R>` need to be implemented.

```rust,edition2018,no_run,noplaypen
#[derive(Debug)]
pub struct MemorySequence<R>
where
    R: Round,
{
    sequence: Vec<Entry<R>>,
}

impl<R> Sequence<R> for MemorySequence<R>
where
    R: Round,
{ ... }

--------------------------------------------

#[derive(Debug)]
pub struct MemoryState<R>
where
    R: Round,
{
    n_prom: R,
    acc_round: R,
    ld: u64,
    gc_idx: u64,
    max_promise_sfx: Vec<Entry<R>>,
}

impl<R> PaxosState<R> for MemoryState<R>
where
    R: Round,
{ ... }
```
