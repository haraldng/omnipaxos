# OmniPaxos vs Maelstrom (`lin-kv`)

This crate builds a thin OmniPaxos-backed node for [Maelstrom](https://github.com/jepsen-io/maelstrom),
Kyle Kingsbury's lightweight Jepsen-based workbench for testing distributed algorithms. It answers
a **correctness/robustness** question — does OmniPaxos maintain linearizability under injected
network faults? — not a performance one.

## How it works

Maelstrom drives this binary as one OS process per cluster node, communicating over stdin/stdout as
line-delimited JSON (see [doc/protocol.md](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md)
and [doc/workloads.md](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md) in the
Maelstrom repo). Maelstrom itself acts as both the client and the network: it generates `read`/
`write`/`cas` operations, injects network partitions between nodes, and checks the resulting
operation history for linearizability violations via its Knossos checker.

- **Peer-to-peer OmniPaxos messages** are wrapped in a custom envelope
  (`{"type": "omnipaxos_msg", "msg": <the Message<KvEntry>, JSON-encoded>}`) and routed by Maelstrom
  like any other message — `omnipaxos`'s own `serde` feature makes this a direct `serde_json`
  round-trip, no separate wire format needed.
- **Every operation — including reads — goes through the OmniPaxos log.** A read is proposed and
  applied in log order just like a write, so it's linearized relative to every other operation. This
  is a deliberate simplification favoring correctness over performance (reads pay full consensus
  latency), which is fine here since this crate's whole purpose is a correctness credential, not a
  throughput number.
- **Storage is `MemoryStorage`** (in-memory). That's sufficient here because Maelstrom never
  crashes or restarts a node mid-test: its process runner doesn't support Jepsen's `kill`/`pause`
  faults, so network partitions are the only fault it injects.

## Running it

Requires a JDK (Maelstrom needs Java 11+ — check `java -version`; on macOS with only an old JDK
installed, `brew install openjdk` and point `JAVA_HOME`/`PATH` at it for the commands below), plus
`gnuplot` (`brew install gnuplot` / `apt install gnuplot`). Maelstrom only uses `gnuplot` for its
performance plots, but if it's missing the plot step errors and the whole run is reported as
`:valid? :unknown` (exit status 2) even when the history is linearizable.

```sh
./benchmarks/maelstrom_node/run_maelstrom.sh
```

The script builds the node in release mode, downloads a pinned Maelstrom release into
`target/maelstrom-<version>/` (once), and runs a 60-second `lin-kv` test against 3 nodes with 30
concurrent clients at 100 ops/s and a partition change every ~3 seconds — the settings from
Maelstrom's own Raft tutorial. It exits with Maelstrom's status: 0 only if the history is
linearizable. Extra arguments are passed through to `maelstrom test` and override the defaults,
e.g. to repeat the test five times:

```sh
./benchmarks/maelstrom_node/run_maelstrom.sh --test-count 5
```

Lighter settings aren't a meaningful check: a deliberately broken variant of this node that serves
reads from local state (skipping the log) passed a 20-second run at 10 ops/s, but was caught at
the default settings above, with a read returning a value overwritten by an already-acknowledged
write.

Look for `:valid? true` and "Everything looks good!" at the end of the output. Full results
(including operation history and any plots) are written under
`target/maelstrom-<version>/store/lin-kv/<timestamp>/`.

The same script runs nightly (and on manual dispatch) in the `Maelstrom` GitHub Actions workflow
with `--test-count 5`, which uploads the results directory as an artifact.

**Note**: `--node-count 1` will never work against this binary — OmniPaxos requires more than one
node by design (`ClusterConfig::validate` rejects a single-node cluster), so always use
`--node-count 2` or more, matching Maelstrom's own multi-node Raft tutorial invocation.

## Known limitations

- Only network partitions are tested. Crash-recovery would need durable storage in this node and a
  harness that actually restarts nodes (e.g. a full Jepsen setup), which Maelstrom doesn't provide.
- CAS values are compared via JSON structural equality (`serde_json::Value`'s `PartialEq`), matching
  `lin-kv`'s "arbitrary JSON value" semantics for keys/values.
- This is a correctness check, not a benchmark — don't cite results from this crate as a performance
  comparison against etcd/TiKV.
