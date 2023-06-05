To change the servers in the cluster, we must first stop the current OmniPaxos cluster. This is done via the `reconfigure()` function, which takes a `ClusterConfig` that contains the settings for the next OmniPaxos cluster and some optional metadata.

```rust
// Node 3 seems to have crashed... let's replace it with a new node 4.
let new_configuration = ClusterConfig {
    configuration_id: 2,
    nodes: vec![1, 2, 4],
    ..Default::default()
};
let metadata = None;
omni_paxos.reconfigure(new_configuration, metadata).expect("Failed to propose reconfiguration");
```

Calling ``reconfigure()`` will propose a `StopSign` entry to be appended. If it gets decided, the log is sealed and prevented from being further appended. From the `StopSign` entry, all nodes will be able to see the new configuration. When you, the user, read from a node and find a `LogEntry::StopSign` in the log, you should start a new `OmniPaxos` instance at this node if it is part of the new configuration.

```rust
let current_config = ... // the OmniPaxos config for this node
let idx: u64 = ...  // some index we last read from
let decided_entries: Option<Vec<LogEntry<KeyValue>>> = omnipaxos.  read_decided_suffix(idx);

if let Some(de) = decided_entries {
    for d in de {
        match d {
            LogEntry::StopSign(stopsign) => {
                let new_configuration = stopsign.next_config;
                if new_configuration.nodes.contains(&my_pid) {
                // current configuration has been safely stopped. Start new instance
                    let new_storage = MemoryStorage::default();
                    let mut new_omnipaxos = new_configuration.build_for_server(current_config.server_config).unwrap();
 
                    ... // use new_omnipaxos
                }
            }
            _ => {
                todo!()
            }
        }
    }
}
```

> **Note:** New nodes will not see the `StopSign` since they were not part of the old configuration. The user themselves must notify and start these new nodes. Furthermore,the user must ensure these new nodes have the application state or log up to the stopsign before starting their `OmniPaxos` instance.
