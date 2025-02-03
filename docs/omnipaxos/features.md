OmniPaxos provide several features that can be used to enhance both usability and performance:

## `omnipaxos` feature flags
- `logging` - System-wide logging with the slog crate.
- `toml_config` - Create an OmniPaxos instance from a TOML configuration file.
- `serde` - Serialization and deserialization of messages and internal structs with serde. This makes it convenient to use with any desired network implementation without having to implement your own serializer and deserializer.
- `macros` - Macros for convenience, e.g., deriving blanket implementations for OmniPaxos traits.

## `omnipaxos_storage` feature flags
- `persistent_storage` - a persistent storage implementation using RocksDB.

Configure the features in your `Cargo.toml` file.
