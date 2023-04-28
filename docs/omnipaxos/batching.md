OmniPaxos supports batch processing to reduce the number of IO operations onto storage. To enable it we need to add config when initiate the OmniPaxos instance.

```rust
let omnipaxos_config = OmniPaxosConfig {
    batch_size: 100, // `batch_size = 1` by default
    configuration_id,
    pid: my_pid,
    peers: my_peers,
    ..Default::default()
}
// build omnipaxos instance
```

> Best Practices: always make sure that all the nodes on your cluster have same batch size to reduce potential problems.

