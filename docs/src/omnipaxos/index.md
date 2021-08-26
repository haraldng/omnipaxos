# OmniPaxos

In this section we will introduce the features of OmniPaxos in detail.

In particular, we will talk about [communication](communication.md), as well as how to [configure](configuration.md) it, before describing some of the advanced options for Omnipaxos, such as [garbage collection](garbage-collection.md), [logging](logging.md), and [storage](storage.md).

As the library is based on the [Leader-based Sequence Paxos](https://arxiv.org/pdf/2008.13456.pdf) algorithm we will give an onverview of the properties it has to adhere to.

The Leader-Based Sequence Consensus algorithm is used to achieve replication inside of a partition. It ensures that a leader, which is elected utilising a different component, proposes an operation and it will be committed only when a consensus is achieved by the majority. At the same time a sequence is stored inside each replica. By utilising a leader we assure that only one operation will be decided at a point in time which also ensures the linearizability of the system.

By implementing the Leader-Based Sequence Consensus, the system has to abide the following properties:

- **Validity**: If a process decides v then v is a sequence of proposed commands (without duplicates).
- **Uniform Agreement**: If a process p decides u and process q decides v then one is a prefix of the other.
- **Integrity**: If process p decides u and later decides v then u is a strict prefix of v.
- **Termination**: If command C is proposed by a correct process then eventually every correct process decides a sequence containing C.
