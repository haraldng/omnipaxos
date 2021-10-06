# OmniPaxos Configuration

To ease up the configuration of the OmniPaxos replica, a hocon configuration can be provided with the following keys set:

```json
{
    // The identifier for the configuration that this Omni-Paxos replica is part of.
    config_id: 1,
    // The identifier of this replica.
    pid: 1,
    // Path where the default logger logs events.
    log_file_path: "/paxos/logs"
}
```

```rust,edition2018,no_run,noplaypen
// config/my_configuration.txt
---------------------------------------
paxos_config: {
    config_id: 1,
    pid: 1,
    log_file_path: "/paxos/logs"
}
---------------------------------------

let raw_cfg = HoconLoader::new()
    .load_file("config/my_configuration.txt")?
    .hocon()?;

let cfg: &Hocon = &raw_cfg["paxos_config"];
```
