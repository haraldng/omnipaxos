# Introduction

The OmniPaxos library is mainly driven by the [**OmniPaxos**](../omnipaxos/index.md) struct.  It is a plain Rust struct and the user therefore needs to provide a network implementation themselves to actually send and receive messages. In this tutorial we will show how a user should interact with this struct in order to implement a strongly consistent, replicated log. This tutorial will focus on how to use the library and showcase its features. 
<!-- For the properties and advantages of OmniPaxos in comparison to other similar protocols, we refer to the Omni-Paxos paper. -->