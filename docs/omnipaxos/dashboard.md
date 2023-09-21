OmniPaxos supplies an in-terminal dashboard that works out-of-the-box which displays the information of peers that the OmniPaxos node currently connects to. The dashboard can be used for monitoring the workings status of the OmniPaxos peers or for network debugging. The dashboard can be used by importing the `omnipaxos_ui` dependency:

```rust
[dependencies]
omnipaxos = { version = "LATEST_VERSION", features = ["macros"] }
omnipaxos_ui = "LATEST_VERSION"
```

## How To Use

We need to setup the dashboard using the same configuration that used to setup OmniPaxos then call the public function `start()` to start showing the UI.

```rust
// op_config: OmniPaxosConfig
let mut omni_paxos_ui = OmniPaxosUI::with(op_config.into());
omni_paxos_ui.start();
```

To get user inputs and flush the UI, a `tick()` function need to be called periodically with the states retrieved from OmniPaxos instance. Then the UI will update based on the newest information of the server. The time period between `tick()`s can be customized depends on how often dose the user want the UI to be flushed. For example, if it is called every `200ms`, the OmniPaxos status will be retrieved and the UI will be updated `5` times per seconds. 

```rust
// Call this periodically
// omni_paxos: OmniPaxos<Entry, Storage>
omni_paxos_ui.tick(omni_paxos.get_ui_states());
```

The dashboard has two different views for leader server and follower servers. The leader's dashboard has more information then follower's, such as followers progress to catch up, as shown bellow:

![omnipaxos](../images/dashboard.jpg)