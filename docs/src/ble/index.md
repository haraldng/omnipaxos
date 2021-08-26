# Ballot Leader Election

In this section we will introduce the features of Ballot Leader Election in detail.

In particular, we will talk about how the BLE's internal [tick timer](timer.md) works, how to [configure](configuration.md) it.

> **Note:** The [logger](../omnipaxos/logging.md) for the **Ballot Leader Election** is the same as the one in **OmniPaxos**.

A leader is elected by the component which runs the Ballot Leader Election with Gossiping algorithm. It ensures that only one leader is chosen by every process and if the leader crashes a new leader is elected.

By implementing the Ballot Leader Election with Gossiping, the system has to abide the following properties:

- **BLE1 Completness**: Eventually every correct process trusts some correct process if a majority are correct.
- **BLE2 Eventual Agreement**:  Eventually no two correct correct processes trust different correct processes.
- **BLE3 Monotonic unique ballots**: If a process L with ballot n is elected as leader by $p_{i}$, all previously elected leaders by $p_{i}$ have ballot numbers less than n, and (L, n) is a unique number. 