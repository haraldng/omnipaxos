OmniPaxos provide several features that can be used to enhance both usability and performance:

- `batch_accept`: Batch multiple log entries into a single message to reduce overhead during replication.
- `continued_leader_reconfiguration` - When [reconfiguring](../reconfiguration.md), let the current leader become the initial leader in the new configuration (if possible). This can help shorten down-time during reconfiguration.
- `logging` - System-wide logging with the slog crate.
- `toml_config` - Create an OmniPaxos instance from a TOML configuration file.
- `serde` - Serialization and deserialization of messages and internal structs with serde. This makes it convenient to use with any desired network implementation without having to implement your own serializer and deserializer.

Configure the features in your `Cargo.toml` file. By default, `batch_accept` and `continued_leader_reconfiguration` are enabled. 