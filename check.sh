#!/usr/bin/env bash
set -eux

cargo test --workspace --all-targets
cargo check --workspace --all-targets
cargo fmt --all -- --check
#cargo clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo clippy -- -D warnings -W clippy::all
cargo test --workspace --doc