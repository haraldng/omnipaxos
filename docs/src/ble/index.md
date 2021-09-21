# Ballot Leader Election

In this section we will introduce the features of **Ballot Leader Election** in detail.

In particular, we will talk about how the BLE's internal [tick timer](timer.md) works, how to [configure](configuration.md) it.

> **Note:** The [logger](../omnipaxos/logging.md) for the **Ballot Leader Election** is the same as the one in **OmniPaxos**.

A leader is elected by the component which runs the Ballot Leader Election with Gossiping algorithm. It ensures that only one leader is chosen by every process and if the leader crashes a new leader is elected.
