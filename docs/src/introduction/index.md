# Introduction

In this section of the tutorial, we will discuss the model assumptions and concepts that are underlying the implementation of OmniPaxos. We will look at how the [`OmniPaxos`](../omnipaxos/index.md) and [`Ballot Leader Election`](../ble/index.md) modules communicate between them, while also providing examples for the user utilising an actor framework such as [Kompact](https://github.com/kompics/kompact).

The replicas that are created communicate by exchanging discrete pieces of information (*messages*). The network layer is provided by the user alongside the sending and receiving of messages.

## Components

The OmniPaxos library can be divided into two big components
[**OmniPaxos**](../omnipaxos/index.md) and [**Ballot Leader Election**](../ble/index.md) (*BLE*). By default, OmniPaxos comes without a leader election algorithm as it can be provided by the user. The library includes the BLE algorithm as an already implemented alternative.