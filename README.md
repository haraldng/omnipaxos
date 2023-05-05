OmniPaxos
============

![ci](https://github.com/haraldng/omnipaxos/workflows/ci/badge.svg)
[![Cargo](https://img.shields.io/badge/crates.io-v0.1.0-orange)](https://crates.io/crates/omnipaxos)
[![Documentation](https://docs.rs/omnipaxos/badge.svg)](https://docs.rs/omnipaxos)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/haraldng/omnipaxos)

OmniPaxos is an in-development replicated log library implemented in Rust. OmniPaxos aims to hide the complexities of consensus to provide users a replicated log that is as simple to use as a local log.

Similar to Raft, OmniPaxos can be used to build strongly consistent services such as replicated state machines. Additionally, the leader election of OmniPaxos offers better resilience to partial connectivity and more flexible and efficient reconfiguration compared to Raft.

An OmniPaxos node is implemented as plain Rust structs. This allows it to be used with any desired storage, network, and runtime implementations.

For more detailed explanations and tutorials showcasing our features, check out https://omnipaxos.com.

## License
This project is licensed under the [Apache-2.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in OmniPaxos by you shall be licensed as Apache-2.0, without any additional terms or conditions.
