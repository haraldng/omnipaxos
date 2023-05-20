OmniPaxos is a replicated log library implemented in Rust. OmniPaxos aims to hide the complexities of consensus to provide users a replicated log that is as simple to use as a local log. 

Similar to Raft, OmniPaxos can be used to build strongly consistent services such as replicated state machines. Additionally, the leader election of OmniPaxos offers better resilience to partial connectivity and more flexible and efficient reconfiguration compared to Raft.

The library consist of two workspaces: `omnipaxos` and `omnipaxos_storage`. The `omnipaxos` implements the algorithms of OmniPaxos as plain Rust structs and you need to implement the actual networking yourself (we describe how to send and handle messages [here](../../omnipaxos/communication)). You can provide your own implementation for storing the log and state of OmniPaxos, but we also provide both in-memory and persistent storage implementations that work out of the box in `omnipaxos_storage`.

In addition to the tutorial style presentation in this book, examples of usages of OmniPaxos can be found in the [examples](https://github.com/haraldng/omnipaxos/tree/master/examples) and in the [tests](https://github.com/haraldng/omnipaxos/tree/master/omnipaxos/tests).