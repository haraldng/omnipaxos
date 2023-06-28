OmniPaxos also offers support for [flexible quorums](https://arxiv.org/pdf/1608.06696v1.pdf). Normally a node needs to consult a majority of nodes to become a leader and then, as a leader, consult a majority of nodes to append to the log. Flexible quorums allow for a trade off between the number of nodes that need to be consulted in each of these two scenarios.

OmniPaxos can be configured to use a flexible quorum as follows:
```rust
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig, util::FlexibleQuroum};

let flex_quorum = FlexibleQuorum {
    read_quorum_size: 5,
    write_quorum_size: 3,
};

let cluster_config = ClusterConfig {
    configuration_id: 1,
    nodes: vec![1, 2, 3, 4, 5, 6, 7],
    flexible_quorum: Some(flex_quorum),
};
let server_config = ServerConfig {
    pid: 1,
    ..Default::default()
};
let config = OmniPaxosConfig {
    cluster_config,
    server_config,
};
```
In OmniPaxos, becoming a leader involves reading from the log and so `read_quorum_size` is the number of nodes to consult in order to become a leader. The `write_quorum_size` is the number of nodes to consult in order to append to the log.

In order to guarantee safety, the `read_quorum_size` and `write_quorum_size` must overlap (i.e. `read_quorum_size` + `write_quorum_size` > # of nodes in cluster). In practice this means that reducing the `write_quorum_size` requires increasing the `read_quorum_size`. However, appending to the log is a much more common occurrence than electing a leader, so reducing the `write_quorum_size` can be beneficial.
