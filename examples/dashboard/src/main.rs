// To run the example with the UI:
// cargo run --bin dashboard
use crate::{entry::LogEntry, server::OmniPaxosServer, util::*};
use omnipaxos::{messages::Message, util::NodeId, *};
use omnipaxos_storage::memory_storage::MemoryStorage;
use omnipaxos_ui::OmniPaxosUI;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{runtime::Builder, sync::mpsc};

mod entry;
mod server;
mod util;

type OmniPaxosLog = OmniPaxos<LogEntry, MemoryStorage<LogEntry>>;

const SERVERS: [u64; 11] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

#[allow(clippy::type_complexity)]
fn initialise_channels() -> (
    HashMap<NodeId, mpsc::Sender<Message<LogEntry>>>,
    HashMap<NodeId, mpsc::Receiver<Message<LogEntry>>>,
) {
    let mut sender_channels = HashMap::new();
    let mut receiver_channels = HashMap::new();

    for pid in SERVERS {
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        sender_channels.insert(pid, sender);
        receiver_channels.insert(pid, receiver);
    }
    (sender_channels, receiver_channels)
}

/// Here is the main function for the dashboard example. Including the nodes setup, and the main loop.
/// There will be a dashboard UI showing the status from the view of one node in the terminal, and in
/// each loop, some batched log entries will be appended to the leader, then the leader will be killed.
/// Finally there will be a majority of nodes remain and keep append log entries.
fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let configuration_id = 1;
    let majority = (SERVERS.len() / 2) + 1;
    let mut op_server_handles = HashMap::new();
    let (sender_channels, mut receiver_channels) = initialise_channels();

    // set up nodes
    for pid in SERVERS {
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: ELECTION_TICK_TIMEOUT,
            custom_logger: Some(OmniPaxosUI::logger()),
            ..Default::default()
        };
        let cluster_config = ClusterConfig {
            configuration_id,
            nodes: SERVERS.into(),
            ..Default::default()
        };
        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };
        // set up the ui with the same configration as for the OmniPaxos
        let mut omni_paxos_ui = OmniPaxosUI::with(op_config.clone().into());
        if pid == majority as u64 {
            // start UI for the the node with id equals to majority, which will be the leader later
            omni_paxos_ui.start();
        }
        let omni_paxos: Arc<Mutex<OmniPaxosLog>> = Arc::new(Mutex::new(
            op_config.build(MemoryStorage::default()).unwrap(),
        ));
        let mut op_server = OmniPaxosServer {
            omni_paxos_ui,
            omni_paxos: Arc::clone(&omni_paxos),
            incoming: receiver_channels.remove(&pid).unwrap(),
            outgoing: sender_channels.clone(),
        };
        let join_handle = runtime.spawn({
            async move {
                op_server.run().await;
            }
        });
        op_server_handles.insert(pid, (omni_paxos, join_handle));
    }

    // wait for leader to be elected...
    std::thread::sleep(WAIT_LEADER_TIMEOUT);

    // start loop
    let mut idx = SERVERS.len();
    loop {
        let (server, handler) = op_server_handles.get(&(idx as u64)).unwrap();
        // batch append log entries
        for i in 0..BATCH_SIZE {
            let kv = LogEntry(i);
            server.lock().unwrap().append(kv).expect("append failed");
            std::thread::sleep(BATCH_PERIOD);
        }
        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // kill the leader if number of nodes is larger than majority
        if majority < idx {
            handler.abort();
            idx -= 1;
            std::thread::sleep(WAIT_LEADER_TIMEOUT);
        }
    }
}
