# Key-Value store

This example shows how to build a replicated key-value store using OmniPaxos. Each server is executed by one tokio thread and they communicate using tokio's mpsc channels. 

- [kv.rs](/src/kv.rs) defines the `KeyValue` and `KVSnapshot` structs that will be stored in the log of OmniPaxos.
- [server.rs](/src/server.rs) implements the logic for an OmniPaxos server (i.e., a replica in our KV-store) and showcases how to send/receive messages and trigger the necessary timers.
- The [main](/src/main.rs) program spawns the OmniPaxos servers and shows how to append and read entries from the replicated log via different servers. We also show that a new leader will be elected if one of the servers fail.