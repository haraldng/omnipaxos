# Foreword

OmniPaxos is an in-development replicated log library implemented in Rust. OmniPaxos aims to hide the complexities of consensus to provide users a replicated log that is as simple to use as a local log. 

Similar to Raft, OmniPaxos can be used to build strongly consistent services such as replicated state machines. Additionally, the leader election of OmniPaxos offers better resilience to partial connectivity and more flexible and efficient reconfiguration compared to Raft.

The library consist of two parts: `omnipaxos_core` and `omnipaxos_runtime`. The `omnipaxos_core` implements the algorithms of OmniPaxos as plain Rust structs and thus requires users to handle the interaction between the different structs themselves as we describe [here]. If you just want a replicated log out of the box, we suggest using `omnipaxos_runtime` instead, which hides all the interactions from the user by using [Tokio](https://tokio.rs/).

In addition to the tutorial style presentation in this book, examples of usages of OmniPaxos can be found in the [tests](https://github.com/haraldng/omnipaxos/tree/master/tests).
