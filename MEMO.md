# PhysicalClock Memo

## Quick Usage (MyClock)

`OmniPaxosConfig::build` and `ClusterConfig::build_for_server` take a `PhysicalClock`
instance by reference.

## Access From SequencePaxos (internal)

`SequencePaxos` stores a reference to the same clock, so methods can call it directly:

```rust
// inside SequencePaxos impl
let now_ns: i64 = self.clock.get_time();
let eps_ns: i64 = self.clock.get_uncertainty();
```
    
# Memory Storage Memo
Add
- synlog (synced log) : Vec<T>
- unsynlog (unsynced log) : HashMap<usize, T>


