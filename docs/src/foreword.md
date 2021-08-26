# Foreword

In this document we will describe how to use an in-development sequence consensus library called `OmniPaxos`. It is based on the [Leader-based Sequence Paxos](https://arxiv.org/pdf/2008.13456.pdf) algorithm and implemented in the Rust programming language. 

Similarly to Raft, the Leader-based Sequence Paxos algorithm can be used to build abstractions such as a distributed log or state-machine replication. However, Leader-based Sequence Paxos uses a modular design that allows for increased flexibility in leader election and an efficient reconfiguration that allows new server to catch up the sequence in parallel.

An OmniPaxos replica is implemented as a Rust ```struct```. This should allow for convenient usage in general or on top of an actor framework such as [Kompact](https://github.com/kompics/kompact).

In addition to the tutorial style presentation in this book, many examples of OmniPaxos uses can be found in the [tests](https://github.com/haraldng/omnipaxos/tree/master/tests).