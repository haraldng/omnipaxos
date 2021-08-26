# Introduction

In this section of the tutorial we will discuss the model assumptions and concepts that are underlying the implementation of OmniPaxos. We will look at how the [`OmniPaxos`](../omnipaxos/index.md) and [`Ballot Leader Election`](../ble/index.md) modules communicate between them, while also providing examples for the user utilising an actor framework such as [Kompact](https://github.com/kompics/kompact).

At a high level, OmniPaxos is simply an abstractization layer for the [Leader-based Sequence Paxos](https://arxiv.org/pdf/2008.13456.pdf). It provides all the necessary functions to run a distributed system. It is capable of deciding on a series of events, electing new leaders, reconfiguration and basic garbage collection.

The replicas that are created communicate by exchanging discrete pieces of information (*messages*). The network layer is provided by the user alongside the sending and receiving of messages.
