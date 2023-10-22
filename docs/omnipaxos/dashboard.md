OmniPaxos provides an in-terminal dashboard that can be connected to one of the OmniPaxos nodes. The dashboard displays the performance, the connectivity, and who is the current leader in the cluster. The dashboard can be used by importing the `omnipaxos_ui` dependency:

```rust
[dependencies]
omnipaxos = { version = "LATEST_VERSION", features = ["macros"] }
omnipaxos_ui = "LATEST_VERSION"
```

## Usage
We need to setup the dashboard using the same `OmniPaxosConfig` that was used to setup OmniPaxos and then call the public function `start()` to start showing the UI.

```rust
// op_config: OmniPaxosConfig
let mut omni_paxos_ui = OmniPaxosUI::with(op_config.into());
omni_paxos_ui.start();
```

The dashboard gets updated via the `tick()` function that needs to be called periodically with the states retrieved from the OmniPaxos node. The time period between `tick()`s can be customized depending on how often we want the UI to be re-rendered with updated state. The more frequent, the more updated the dashboard will be, but that might also incur more overhead. From our experience, calling `tick()` every 200ms is a good starting point.

```rust
// Call this periodically
// omni_paxos: OmniPaxos<Entry, Storage>
omni_paxos_ui.tick(omni_paxos.get_ui_states());
```

The dashboard has different views depending on if it is connected to the leader or follower server. The leader's dashboard has more information than the follower's, such as the follower's lag in the replication log, as shown below:

![omnipaxos](../images/dashboard.jpg)