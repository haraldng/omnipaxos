# Logging

OmniPaxos uses the [slog](https://crates.io/crates/slog) crate to provide system-wide logging facilities. Logging works out of the box with a default asynchronous console and file logger implementation.

The actual logging levels are controlled via build features. The default features correspond to `max_level_trace` and `release_max_level_info`, that is in debug builds all levels are shown, while in the release profile only `info` and more severe message are shown.

## Custom Logger

Sometimes the default logging configuration is not sufficient for a particular application. For example, you might need a larger queue size in the `Async` drain, or you may want to write to a file instead of the terminal.

The user can provide a custom implementation based on [**slog**](https://crates.io/crates/slog).

> **Note:** The custom logger can be created for the [**Ballot Leader Election**](../ble/index.md) module as well.
