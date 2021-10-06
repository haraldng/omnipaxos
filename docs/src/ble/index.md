# Ballot Leader Election

In this section, we will introduce the features of **Ballot Leader Election** in detail.

In particular, we will talk about how the BLE's internal [tick timer](#tick-timer) works, how to [configure](#configuration) it.

> **Note:** The [logger](../omnipaxos/logging.md) for the **Ballot Leader Election** is the same as the one in **OmniPaxos**.

The user is able to use an already implemented leader election algorithm provided by the library. A BLE component can be initialised by calling one of the two constructors. The second constructor uses a hocon configuration which will be discussed in further detail in [configuration](#configuration).

```rust,edition2018,no_run,noplaypen
BallotLeaderElection::with(
    peers: Vec<u64>,
    pid: u64,
    hb_delay: u64,
    increment_delay: u64,
    initial_leader: Option<Leader<Ballot>>,
    initial_delay_factor: Option<u64>,
    logger: Option<Logger>,
    log_file_path: Option<&str>,
)

BallotLeaderElection::with_hocon(
    cfg: &Hocon,
    peers: Vec<u64>,
    initial_leader: Option<Leader<Ballot>>,
    logger: Option<Logger>,
)
```

## Tick Timer

The **Ballot Leader Election** has an internal logical clock that is driven by a tick() function.

In the example below, we have a timer that repeats the provided function once every 100ms and the heartbeat delay would be 5 ticks, then the heartbeat timeout would happen once every 500ms.

```rust,edition2018,no_run,noplaypen
// Create a timer that triggers once every 100ms
// Call method tick() from ble and check if there is a leader event
// Get the outgoing messages and send them through the network to the receivers
timer = create_timer(100ms, {
    let leader = ble.tick();
    if let Some(l) = leader {
        paxos.handle_leader(l);
    }
    
    msgs = ble.get_outgoing_msgs();
    for out_msg in msgs {
        let receiver = out_msg.to;
        // send out_msg to receiver
    }
})
```

## Configuration

To ease up the configuration of the Ballot Leader Election module, a hocon configuration can be provided with the following keys set:

```json
{
    // The identifier for the configuration that this Omni-Paxos replica is part of.
    config_id: 1,
    // The identifier of this replica.
    pid: 1,
    // A fixed delay between heartbeats. It is measured in ticks.
    hb_delay: 5,
    // A fixed delay that is added to the hb_delay. It is measured in ticks.
    increment_delay: 2,
    // A factor used in the beginning for a shorter hb_delay.
    initial_delay_factor: 2,
    // Path where the default logger logs events.
    log_file_path: "/ble/logs"
}
```

```rust,edition2018,no_run,noplaypen
// config/my_configuration.txt
---------------------------------------
ble_config: {
    config_id: 1,
    pid: 1,
    hb_delay: 5,
    increment_delay: 2,
    initial_delay_factor: 2,
    log_file_path: "/ble/logs"
}
---------------------------------------

let raw_cfg = HoconLoader::new()
    .load_file("config/my_configuration.txt")?
    .hocon()?;

let cfg: &Hocon = &raw_cfg["ble_config"];
```
