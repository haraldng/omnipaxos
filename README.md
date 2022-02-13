OmniPaxos
============

OmniPaxos is an in-development replicated log library implemented in Rust. OmniPaxos aims to hide the complexities of consensus to provide users a replicated log that is as simple to use as a local log.

Similar to Raft, OmniPaxos can be used to build strongly consistent services such as replicated state machines. Additionally, the leader election of OmniPaxos offers better resilience to partial connectivity and more flexible and efficient reconfiguration compared to Raft.

An OmniPaxos node is implemented as plain Rust structs. This allows it to be used with any desired storage and network implementations. This should allow for convenient usage in general or on top of an actor framework such as [Kompact](https://github.com/kompics/kompact). We also provide an alternative with an async runtime in [Tokio](https://tokio.rs/).

For more detailed explanations and examples, check out the [tutorial](https://haraldng.github.io/omnipaxos/foreword.html).

## License
This project is licensed under the [Apache-2.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in OmniPaxos by you shall be licensed as Apache-2.0, without any additional terms or conditions.
