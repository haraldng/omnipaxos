pub mod utils;

use omnipaxos::{
    messages::{
        sequence_paxos::{PaxosMessage, PaxosMsg},
        Message,
    },
    util::{LogEntry, NodeId},
    ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use utils::Value;

type Node = OmniPaxos<Value, MemoryStorage<Value>>;

fn build(pid: NodeId) -> Node {
    OmniPaxosConfig {
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            flexible_quorum: None,
        },
        server_config: ServerConfig {
            pid,
            ..Default::default()
        },
    }
    .build(MemoryStorage::default())
    .expect("valid config")
}

fn kind(m: &PaxosMsg<Value>) -> &'static str {
    match m {
        PaxosMsg::PrepareReq(_) => "PrepareReq",
        PaxosMsg::Prepare(_) => "Prepare",
        PaxosMsg::Promise(_) => "Promise",
        PaxosMsg::AcceptSync(_) => "AcceptSync",
        PaxosMsg::AcceptDecide(_) => "AcceptDecide",
        PaxosMsg::Accepted(_) => "Accepted",
        PaxosMsg::NotAccepted(_) => "NotAccepted",
        PaxosMsg::Decide(_) => "Decide",
        PaxosMsg::ProposalForward(_) => "ProposalForward",
        PaxosMsg::Compaction(_) => "Compaction",
        PaxosMsg::AcceptStopSign(_) => "AcceptStopSign",
        PaxosMsg::ForwardStopSign(_) => "ForwardStopSign",
    }
}

/// Holds every message until the test delivers it. BLE is never ticked, so
/// ballots only come from `try_become_leader`.
struct Net {
    nodes: Vec<Node>,
    held: Vec<PaxosMessage<Value>>,
}

impl Net {
    fn node(&mut self, pid: NodeId) -> &mut Node {
        &mut self.nodes[(pid - 1) as usize]
    }

    fn collect(&mut self) {
        for n in self.nodes.iter_mut() {
            let mut buf = Vec::new();
            n.take_outgoing_messages(&mut buf);
            for m in buf {
                if let Message::SequencePaxos(p) = m {
                    self.held.push(p);
                }
            }
        }
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, k: &str) {
        self.collect();
        let pos = self
            .held
            .iter()
            .position(|p| p.from == from && p.to == to && kind(&p.msg) == k)
            .unwrap_or_else(|| panic!("no {k} from {from} to {to} in {:?}", self.held));
        let p = self.held.remove(pos);
        self.node(to).handle_incoming(Message::SequencePaxos(p));
    }

    fn deliver_all(&mut self) {
        self.collect();
        while !self.held.is_empty() {
            let p = self.held.remove(0);
            self.node(p.to).handle_incoming(Message::SequencePaxos(p));
            self.collect();
        }
    }
}

/// A new leader that completes Prepare with a quorum that has not yet learned
/// a decision, and then receives a late Promise from a node that has, must not
/// make that node's decided index go backwards (issue #172).
#[test]
fn late_promise_does_not_decrease_followers_decided_idx() {
    let mut net = Net {
        nodes: vec![build(1), build(2), build(3)],
        held: Vec::new(),
    };

    net.node(1).try_become_leader();
    net.deliver(1, 2, "Prepare");
    // Node 3 takes a higher ballot; its Prepares stay held for now.
    net.node(3).try_become_leader();

    // Node 1 becomes leader with node 2 and decides index 1. The Decide to
    // node 2 stays held, so node 2 only knows decided_idx 0.
    net.deliver(2, 1, "Promise");
    net.node(1).append(Value::with_id(1)).expect("append");
    net.deliver(1, 2, "AcceptSync");
    net.deliver(1, 2, "AcceptDecide");
    net.deliver(2, 1, "Accepted");
    net.deliver(2, 1, "Accepted");
    let decided_before = net.node(1).get_decided_idx();
    assert_eq!(decided_before, 1);

    // Node 3 reaches a majority with node 2's Promise (decided_idx 0), then
    // receives node 1's Promise (decided_idx 1) late and replies with AcceptSync.
    net.deliver(3, 1, "Prepare");
    net.deliver(3, 2, "Prepare");
    net.deliver(2, 3, "Promise");
    net.deliver(1, 3, "Promise");
    net.deliver(3, 1, "AcceptSync");

    let decided_after = net.node(1).get_decided_idx();
    assert!(
        decided_after >= decided_before,
        "decided_idx of node 1 fell from {decided_before} to {decided_after}"
    );
    assert_eq!(
        net.node(1).read(0),
        Some(LogEntry::Decided(Value::with_id(1)))
    );

    net.deliver_all();
    for pid in 1..=3 {
        assert_eq!(net.node(pid).get_decided_idx(), 1, "node {pid}");
        assert_eq!(
            net.node(pid).read(0),
            Some(LogEntry::Decided(Value::with_id(1))),
            "node {pid}"
        );
    }
}
