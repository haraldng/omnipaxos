# OmniPaxos

In this section we will introduce the features of OmniPaxos in detail.

In particular, we will talk about [communication](communication.md), [round](round.md), as well as how to [configure](configuration.md) it, before describing some of the advanced options for Omnipaxos, such as [garbage collection](garbage-collection.md), [logging](logging.md), and [storage](storage.md).

As the library is based on the [Leader-based Sequence Paxos](https://arxiv.org/pdf/2008.13456.pdf) algorithm we will give an onverview of the properties it has to adhere to.

The Leader-Based Sequence Consensus algorithm is used to achieve replication inside of a partition. It ensures that a leader, which is elected utilising a different component, proposes an operation and it will be committed only when a consensus is achieved by the majority. At the same time a sequence is stored inside each replica. By utilising a leader we assure that only one operation will be decided at a point in time which also ensures the linearizability of the system.
