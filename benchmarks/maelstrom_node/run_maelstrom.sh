#!/usr/bin/env bash
# Builds the OmniPaxos Maelstrom node and runs Maelstrom's lin-kv workload against it with network
# partitions. Exits with Maelstrom's status: 0 only if the history is linearizable.
#
# Requires Java 11+ and gnuplot on PATH. Extra arguments are passed to `maelstrom test` and override
# the defaults below, e.g. `./run_maelstrom.sh --test-count 5`.
set -euo pipefail

MAELSTROM_VERSION="${MAELSTROM_VERSION:-0.2.4}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
mkdir -p "$target_dir"
target_dir="$(cd "$target_dir" && pwd)"
maelstrom_dir="$target_dir/maelstrom-$MAELSTROM_VERSION"

if ! command -v java >/dev/null; then
  echo "error: Maelstrom needs Java 11+, but no java is on PATH" >&2
  exit 1
fi
java_major="$(java -version 2>&1 | awk -F '"' '/version/ { split($2, v, "."); print (v[1] == 1) ? v[2] : v[1]; exit }')"
if [ "${java_major:-0}" -lt 11 ]; then
  echo "error: Maelstrom needs Java 11+, but java on PATH is version $java_major" >&2
  exit 1
fi
# Without gnuplot, Maelstrom's plot rendering fails and the whole run is reported as unknown (exit 2)
# even when the history is linearizable.
if ! command -v gnuplot >/dev/null; then
  echo "error: Maelstrom needs gnuplot (brew install gnuplot / apt install gnuplot)" >&2
  exit 1
fi

cargo build --release -p omnipaxos_maelstrom_node --manifest-path "$repo_root/Cargo.toml"

if [ ! -x "$maelstrom_dir/maelstrom" ]; then
  mkdir -p "$maelstrom_dir"
  curl -fsSL "https://github.com/jepsen-io/maelstrom/releases/download/v$MAELSTROM_VERSION/maelstrom.tar.bz2" \
    | tar -xj -C "$maelstrom_dir" --strip-components 1
fi

cd "$maelstrom_dir"
./maelstrom test -w lin-kv \
  --bin "$target_dir/release/omnipaxos_maelstrom_node" \
  --node-count 3 --time-limit 60 --rate 100 --concurrency 10n \
  --nemesis partition --nemesis-interval 3 \
  "$@"
