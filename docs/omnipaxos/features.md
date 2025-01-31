OmniPaxos provide several features that can be used to enhance both usability and performance:

- `persistent_storage` - RocksDB-backed storage implementation for OmniPaxos state. Allows for recovery even if all nodes in a cluster fail.
- `logging` - System-wide logging with the slog crate.
- `toml_config` - Create an OmniPaxos instance from a TOML configuration file.
- `serde` - Serialization and deserialization of messages and internal structs with serde. This makes it convenient to use with any desired network implementation without having to implement your own serializer and deserializer.
- `macros` - Macros for convenience, e.g., deriving blanket implementations for OmniPaxos traits.

Configure the features in your `Cargo.toml` file.
