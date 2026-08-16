//! A thin OmniPaxos-backed node for Maelstrom's `lin-kv` workload
//! (https://github.com/jepsen-io/maelstrom), for Jepsen-style linearizability testing under
//! injected faults (partitions, clock skew, crashes).
//!
//! Maelstrom drives this binary as a subprocess per cluster node, communicating over stdin/stdout
//! using line-delimited JSON (see doc/protocol.md and doc/workloads.md in the Maelstrom repo).
//! Maelstrom itself acts as both the client and the network — it injects faults between nodes and
//! checks the resulting history for linearizability violations via its Knossos/Elle checkers.
//!
//! To keep every operation linearizable, both reads AND writes/cas go through the OmniPaxos log
//! (a read is proposed as a log entry just like a write, so it's ordered relative to every other
//! operation) — this is a deliberate simplification favoring correctness over performance, which
//! matches this crate's purpose: Jepsen/Maelstrom results are a correctness credential, not a
//! performance number.
//!
//! Storage is in-memory (`MemoryStorage`) for v1 — a "crash and restart with state preserved"
//! nemesis is out of scope; a "kill" (no restart, or restart with empty state) nemesis is fine.

use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use omnipaxos::{
    messages::Message,
    storage::{Entry, NoSnapshot},
    util::{FlexibleQuorum, NodeId},
    ClusterConfig, OmniPaxosConfig, ServerConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const TICK_INTERVAL: Duration = Duration::from_millis(10);

/// Identifies which client (and which cluster node received it) an operation reply belongs to.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingRequest {
    client_src: String,
    msg_id: Option<u64>,
    /// The node that originally received this client request — only that node replies once the
    /// entry decides, even though every node applies every decided entry identically.
    origin_node: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvOp {
    Read { key: Value },
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

/// The concrete log entry type: a client operation plus enough metadata to reply once decided.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct KvEntry {
    request: PendingRequest,
    op: KvOp,
}

impl Entry for KvEntry {
    type Snapshot = NoSnapshot;
}

type Handle = omnipaxos::OmniPaxos<KvEntry, MemoryStorage<KvEntry>>;

struct NodeState {
    op: Handle,
    self_id: NodeId,
    self_id_str: String,
    kv: HashMap<String, Value>,
    /// Next log index to apply to `kv` / reply for.
    applied_idx: usize,
}

fn maelstrom_id_to_node_id(id: &str) -> NodeId {
    id.trim_start_matches('n')
        .parse::<u64>()
        .expect("Maelstrom node id must be \"n<number>\"")
        + 1
}

fn node_id_to_maelstrom_id(id: NodeId) -> String {
    format!("n{}", id - 1)
}

/// Serializes `body` and writes one Maelstrom envelope line to stdout. Stdout must contain only
/// protocol JSON lines — use `eprintln!` for any debug output instead.
fn send(stdout: &Mutex<io::Stdout>, self_id_str: &str, dest: &str, body: Value) {
    let envelope = json!({ "src": self_id_str, "dest": dest, "body": body });
    let mut stdout = stdout.lock().unwrap();
    writeln!(stdout, "{}", envelope).expect("failed to write to stdout");
    stdout.flush().expect("failed to flush stdout");
}

fn reply(
    stdout: &Mutex<io::Stdout>,
    self_id_str: &str,
    dest: &str,
    in_reply_to: u64,
    mut body: Value,
) {
    body["in_reply_to"] = json!(in_reply_to);
    let envelope = json!({ "src": self_id_str, "dest": dest, "body": body });
    let mut stdout = stdout.lock().unwrap();
    writeln!(stdout, "{}", envelope).expect("failed to write to stdout");
    stdout.flush().expect("failed to flush stdout");
}

/// Applies every newly-decided entry (in log order) to the local `kv` map, and — for entries this
/// node originally received the client request for — sends the appropriate `lin-kv` reply.
fn apply_decided(state: &mut NodeState, stdout: &Mutex<io::Stdout>) {
    let decided_idx = state.op.get_decided_idx();
    while state.applied_idx < decided_idx {
        let idx = state.applied_idx;
        let entry = match state.op.read(idx) {
            Some(omnipaxos::util::LogEntry::Decided(entry)) => entry,
            // Trimmed/Snapshotted/StopSign/Undecided shouldn't occur at an index below the decided
            // index in this crate (no trim/snapshot/reconfigure is ever triggered) — skip
            // defensively rather than panic if one somehow does.
            _ => {
                state.applied_idx += 1;
                continue;
            }
        };
        let KvEntry { request, op } = entry;
        let reply_body = match op {
            KvOp::Read { key } => match state.kv.get(&value_as_map_key(&key)) {
                Some(value) => json!({ "type": "read_ok", "value": value }),
                None => json!({ "type": "error", "code": 20, "text": "key does not exist" }),
            },
            KvOp::Write { key, value } => {
                state.kv.insert(value_as_map_key(&key), value);
                json!({ "type": "write_ok" })
            }
            KvOp::Cas { key, from, to } => {
                let map_key = value_as_map_key(&key);
                match state.kv.get(&map_key) {
                    None => json!({ "type": "error", "code": 20, "text": "key does not exist" }),
                    Some(current) if *current == from => {
                        state.kv.insert(map_key, to);
                        json!({ "type": "cas_ok" })
                    }
                    Some(current) => json!({
                        "type": "error",
                        "code": 22,
                        "text": format!("expected {from} but had {current}")
                    }),
                }
            }
        };
        if request.origin_node == state.self_id {
            if let Some(msg_id) = request.msg_id {
                reply(
                    stdout,
                    &state.self_id_str,
                    &request.client_src,
                    msg_id,
                    reply_body,
                );
            }
        }
        state.applied_idx += 1;
    }
}

/// Maelstrom key values are arbitrary JSON; stringify for use as a `HashMap` key while keeping the
/// original `Value` as the stored value (so non-string keys still round-trip correctly on read).
fn value_as_map_key(v: &Value) -> String {
    v.to_string()
}

fn drain_and_send_outgoing(state: &mut NodeState, stdout: &Mutex<io::Stdout>) {
    let mut messages: Vec<Message<KvEntry>> = Vec::new();
    state.op.take_outgoing_messages(&mut messages);
    for message in messages {
        let dest = node_id_to_maelstrom_id(message.get_receiver());
        let payload =
            serde_json::to_value(&message).expect("Message<KvEntry> is always JSON-serializable");
        send(
            stdout,
            &state.self_id_str,
            &dest,
            json!({ "type": "omnipaxos_msg", "msg": payload }),
        );
    }
}

fn build_node(self_id_str: &str, peer_ids_str: &[String]) -> NodeState {
    let self_id = maelstrom_id_to_node_id(self_id_str);
    let nodes: Vec<NodeId> = peer_ids_str
        .iter()
        .map(|s| maelstrom_id_to_node_id(s))
        .collect();
    let config = OmniPaxosConfig {
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes,
            flexible_quorum: None::<FlexibleQuorum>,
        },
        server_config: ServerConfig {
            pid: self_id,
            ..Default::default()
        },
    };
    let op = config
        .build(MemoryStorage::<KvEntry>::default())
        .expect("cluster config built from Maelstrom's init message must be valid");
    NodeState {
        op,
        self_id,
        self_id_str: self_id_str.to_string(),
        kv: HashMap::new(),
        applied_idx: 0,
    }
}

fn main() {
    let stdout = Arc::new(Mutex::new(io::stdout()));
    let state: Arc<Mutex<Option<NodeState>>> = Arc::new(Mutex::new(None));

    {
        let state = Arc::clone(&state);
        let stdout = Arc::clone(&stdout);
        std::thread::spawn(move || loop {
            std::thread::sleep(TICK_INTERVAL);
            let mut guard = state.lock().unwrap();
            if let Some(node_state) = guard.as_mut() {
                node_state.op.tick();
                drain_and_send_outgoing(node_state, &stdout);
                apply_decided(node_state, &stdout);
            }
        });
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.expect("failed to read line from stdin");
        if line.trim().is_empty() {
            continue;
        }
        let envelope: Value =
            serde_json::from_str(&line).expect("failed to parse Maelstrom envelope as JSON");
        let src = envelope["src"].as_str().unwrap_or_default().to_string();
        let body = &envelope["body"];
        let msg_type = body["type"].as_str().unwrap_or_default();
        let msg_id = body["msg_id"].as_u64();

        match msg_type {
            "init" => {
                let self_id_str = body["node_id"]
                    .as_str()
                    .expect("init message missing node_id")
                    .to_string();
                let node_ids: Vec<String> = body["node_ids"]
                    .as_array()
                    .expect("init message missing node_ids")
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect();
                let node_state = build_node(&self_id_str, &node_ids);
                *state.lock().unwrap() = Some(node_state);
                reply(
                    &stdout,
                    &self_id_str,
                    &src,
                    msg_id.expect("init message missing msg_id"),
                    json!({ "type": "init_ok" }),
                );
            }
            "omnipaxos_msg" => {
                let mut guard = state.lock().unwrap();
                let node_state = guard.as_mut().expect("received a peer message before init");
                let message: Message<KvEntry> = serde_json::from_value(body["msg"].clone())
                    .expect("failed to decode embedded OmniPaxos Message");
                node_state.op.handle_incoming(message);
                drain_and_send_outgoing(node_state, &stdout);
                apply_decided(node_state, &stdout);
            }
            "read" | "write" | "cas" => {
                let mut guard = state.lock().unwrap();
                let node_state = guard
                    .as_mut()
                    .expect("received a client request before init");
                let request = PendingRequest {
                    client_src: src,
                    msg_id,
                    origin_node: node_state.self_id,
                };
                let op = match msg_type {
                    "read" => KvOp::Read {
                        key: body["key"].clone(),
                    },
                    "write" => KvOp::Write {
                        key: body["key"].clone(),
                        value: body["value"].clone(),
                    },
                    "cas" => KvOp::Cas {
                        key: body["key"].clone(),
                        from: body["from"].clone(),
                        to: body["to"].clone(),
                    },
                    _ => unreachable!(),
                };
                if let Err(propose_err) = node_state.op.append(KvEntry { request, op }) {
                    eprintln!("append rejected: {:?}", propose_err);
                }
                drain_and_send_outgoing(node_state, &stdout);
                apply_decided(node_state, &stdout);
            }
            other => {
                eprintln!("ignoring unrecognized message type: {other}");
            }
        }
    }
}
