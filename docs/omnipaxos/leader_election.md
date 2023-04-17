# Ballot Leader Election
A unique feature of Omni-Paxos is the guaranteed progress with one quorum-connected server using Ballot Leader Election (BLE). In this section, we will see how the leader election is used in OmniPaxos.

To detect any leader failures and elect a new leader, you must call the function ``election_timeout()`` periodically (e.g., every 100ms).

```
// Call this periodically
omni_paxos.election_timeout();
```

If a leader has failed, it will be detected in one election timeout and a new leader will be elected in the next timeout (if possible).

> **Note:** The `leader_priority` field in `OmniPaxosConfig` allows user to give desired servers a higher priority to get elected upon a leader change.
