# OmniPaxos vs Maelstrom (`lin-kv`)

This crate builds a thin OmniPaxos-backed node for [Maelstrom](https://github.com/jepsen-io/maelstrom),
Kyle Kingsbury's lightweight Jepsen-based workbench for testing distributed algorithms. It answers
a **correctness/robustness** question — does OmniPaxos maintain linearizability under injected
network faults? — not a performance one. See `plans/help-me-plan-the-mellow-abelson.md` for why
that distinction matters and why Maelstrom specifically was chosen as a first step (with reusing
etcd's own Jepsen suite noted as a heavier follow-on).

## How it works

Maelstrom drives this binary as one OS process per cluster node, communicating over stdin/stdout as
line-delimited JSON (see [doc/protocol.md](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md)
and [doc/workloads.md](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md) in the
Maelstrom repo). Maelstrom itself acts as both the client and the network: it generates `read`/
`write`/`cas` operations, injects faults (partitions, clock skew, etc.) between nodes, and checks
the resulting operation history for linearizability violations via its Knossos checker.

- **Peer-to-peer OmniPaxos messages** are wrapped in a custom envelope
  (`{"type": "omnipaxos_msg", "msg": <the Message<KvEntry>, JSON-encoded>}`) and routed by Maelstrom
  like any other message — `omnipaxos`'s own `serde` feature makes this a direct `serde_json`
  round-trip, no separate wire format needed.
- **Every operation — including reads — goes through the OmniPaxos log.** A read is proposed and
  applied in log order just like a write, so it's linearized relative to every other operation. This
  is a deliberate simplification favoring correctness over performance (reads pay full consensus
  latency), which is fine here since this crate's whole purpose is a correctness credential, not a
  throughput number.
- **Storage is `MemoryStorage`** (in-memory) for v1 — a "kill and restart, state preserved" nemesis
  is out of scope; a "kill, no restart" (or restart with empty state) nemesis is fine.

## Running it

Requires a JDK (Maelstrom needs Java 11+ — check `java -version`; on macOS with only an old JDK
installed, `brew install openjdk` and point `JAVA_HOME`/`PATH` at it for the commands below), plus
`gnuplot` for Maelstrom's performance plots (`brew install gnuplot` / `apt install gnuplot`) — not
required for the correctness check itself, only for the rendered `.png` plots in the results dir.

```sh
cargo build --release -p omnipaxos_maelstrom_node

# Download a Maelstrom release from https://github.com/jepsen-io/maelstrom/releases
# and run from inside the extracted directory:
./maelstrom test -w lin-kv \
  --bin /path/to/target/release/omnipaxos_maelstrom_node \
  --time-limit 20 --rate 10 --node-count 3 --concurrency 2n \
  --nemesis partition --nemesis-interval 5
```

Look for `:valid? true` and "Everything looks good!" at the end of the output. Full results
(including operation history and any plots) are written under `store/lin-kv/<timestamp>/`.

**Note**: `--node-count 1` will never work against this binary — OmniPaxos requires more than one
node by design (`ClusterConfig::validate` rejects a single-node cluster), so always use
`--node-count 2` or more, matching Maelstrom's own multi-node Raft tutorial invocation.

## Known limitations

- In-memory storage only (see above) — a crash-with-persistence nemesis isn't meaningful yet.
- CAS values are compared via JSON structural equality (`serde_json::Value`'s `PartialEq`), matching
  `lin-kv`'s "arbitrary JSON value" semantics for keys/values.
- This is a correctness check, not a benchmark — don't cite results from this crate as a performance
  comparison against etcd/TiKV; see Track A (`go-ycsb`) in the project plan for that.
