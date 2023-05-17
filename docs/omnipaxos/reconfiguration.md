To change the nodes in the cluster, we must first stop the current Omni-Paxos instance. This is done via the `reconfigure()` function, which has a `ReconfigurationRequest` that contains the nodes for the next Omni-Paxos instance and some optional metadata.

```rust,edition2018,no_run,noplaypen
// Node 3 seems to have crashed... let's replace it with a new node 4.
let new_configuration = vec![1, 2, 4];
let metadata = None;
let rc = ReconfigurationRequest::with(new_configuration, metadata);
omni_paxos.reconfigure(rc).expect("Failed to propose reconfiguration");
```

Calling ``reconfigure()`` will propose a `StopSign` entry to be appended. If it gets decided, the log is sealed and prevented from being further appended. From the `StopSign` entry, all nodes will be able to see the new configuration. When you, the user, read from a node and finds a `LogEntry::StopSign` in the log, you should start a new `OmniPaxos` instance at this node if it is also part of the new configuration.

```rust,edition2018,no_run,noplaypen
let idx: u64 = ...  // some index we last read from
    let decided_entries: Option<Vec<LogEntry<KeyValue>>> = seq_paxos.read_decided_suffix(idx);
    if let Some(de) = decided_entries {
        for d in de {
            match d {
                LogEntry::StopSign(stopsign) => {
                    let new_configuration = stopsign.nodes;
                    if new_configuration.contains(&my_pid) {
                    // we are in new configuration, start new instance
                        let mut new_op_conf = OmniPaxosConfig::default();
                        new_sp_conf.set_configuration_id(stopsign.config_id);
                        let new_storage = MemoryStorage::default();
                        let mut new_op = new_op_conf.build(new_storage);
                        
                        ... // use new_sp
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