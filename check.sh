#!/usr/bin/env bash
set -eux

cargo test --features "logging, toml_config" --workspace --all-targets
cargo fmt --all -- --check
cargo check --workspace --all-targets
#cargo clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo clippy -- -D warnings -W clippy::all
cargo test --features "logging, toml_config" --workspace --doc

# Check doc structure defination
chmod +x scripts/doc_structure_checker
scripts/doc_structure_checker