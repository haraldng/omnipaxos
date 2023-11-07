#!/usr/bin/env bash
set -eux

cargo check --workspace --all-targets
cd omnipaxos ; ./test_all_features.sh -c ; cd .. # cargo check all possible combinations of features in omnipaxos
cargo test --features "logging, toml_config" --workspace --all-targets
cargo test --features "logging, toml_config, unicache" -p omnipaxos --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings -W clippy::all
cargo test --features "logging, toml_config" --workspace --doc

# --------------------------------------------------------------
# Check cargo badge version corresponds to the one defined in Cargo.toml
# --------------------------------------------------------------

set +x
CARGO_TOML_PATH="omnipaxos/Cargo.toml"
# Extract the version from Cargo.toml
CURRENT_VERSION=$(grep -m 1 "version" "$CARGO_TOML_PATH" | sed -E 's/.*"([^"]+)"/\1/')
# Extract the version from the README badge
README_BADGE_VERSION=$(perl -n -e '/crates.io-v([\d.]+)/ && print $1' README.md)

if [ -z "$CURRENT_VERSION" ]; then
  echo "Error: Unable to extract version from $CARGO_TOML_PATH"
  exit 1
fi

if [ -z "$README_BADGE_VERSION" ]; then
  echo "Error: Unable to extract version from README badge"
  exit 1
fi

if [ "$CURRENT_VERSION" != "$README_BADGE_VERSION" ]; then
  echo "Error: Version in Cargo.toml ($CURRENT_VERSION) does not match the version in README badge ($README_BADGE_VERSION)."
  exit 1
fi
