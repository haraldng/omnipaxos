# Foreword

OmniPaxos is an in-development sequence consensus library implemented in the Rust programming language.

Similar to Raft, the Omni-Paxos algorithm can be used to build abstractions such as a distributed log or state-machine replication. However, Omni-Paxos uses a modular design that makes it resilient to partial connectivity and provides an efficient reconfiguration that allows the new server to catch up with the log in parallel.

An OmniPaxos replica is implemented as a Rust ```struct```. This should allow for convenient usage in general or on top of an actor framework such as [Kompact](https://github.com/kompics/kompact).

In addition to the tutorial style presentation in this book, examples of usages of OmniPaxos can be found in the [tests](https://github.com/haraldng/omnipaxos/tree/master/tests).
