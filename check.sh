#!/usr/bin/env bash
set -eux

cd omnipaxos ; ./test_all_features.sh -c ; cd ..
cargo test --features "logging, toml_config, macros" --workspace --all-targets
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo test --features "logging, toml_config" --workspace --doc
