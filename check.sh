#!/usr/bin/env bash
set -eux

cargo check --workspace --all-targets
cd omnipaxos ; ./test_all_features.sh -c ; cd .. # cargo check all possible combinations of features in omnipaxos
cargo test --features "logging, toml_config" --workspace --all-targets
cargo test --features "logging, toml_config, unicache" -p omnipaxos --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo test --features "logging, toml_config" --workspace --doc
