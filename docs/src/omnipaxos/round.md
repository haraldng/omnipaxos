# Round

Leader event that indicates a leader has been elected. Should be created when the user-defined leader election algorithm outputs a leader event. Should be then handled in Omni-Paxos by calling `crate::omnipaxos::OmniPaxos::handle_leader()`.

> **Note:** Rounds in Omni-Paxos are totally ordered.

```rust,edition2018,no_run,noplaypen
let leader = leader_election.get_leader();

if let Some(l) = leader {
    paxos.handle_leader(l);
}
```
