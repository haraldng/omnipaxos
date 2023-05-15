A unique feature of Omni-Paxos is the guaranteed progress with one quorum-connected server using Ballot Leader Election (BLE). In this section, we will see how the leader election is used in OmniPaxos.

To detect any leader failures and elect a new leader, you must call the function ``tick()`` periodically. The number of ticks required before an election timeout is triggered can be configured with the ``election_tick_timeout`` field of ``OmniPaxosConfig``. For example, if ``tick()`` is called every 10ms and the ``election_tick_timeout`` is 10, then an election will be triggered every 100ms.

```
// Call this periodically
omni_paxos.tick();
```

If a leader has failed, it will be detected in one election timeout and a new leader will be elected in the next timeout (if possible).

> **Note:** The `leader_priority` field in `OmniPaxosConfig` allows user to give desired servers a higher priority to get elected upon a leader change.

In some cases, a network error will require the resending of messsages between leader and follower. The `tick()` function also drives this behavior. The number of ticks required before checking if a message needs to be resent can be configured with the ``resend_message_tick_timeout`` field of ``OmniPaxosConfig``.


