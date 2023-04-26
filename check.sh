#!/usr/bin/env bash
set -eux

cargo test --features "logging, toml_config" --workspace --all-targets
cargo +nightly fmt --all -- --check
cargo check --workspace --all-targets
#cargo clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo clippy -- -D warnings -W clippy::all
cargo test --workspace --doc
