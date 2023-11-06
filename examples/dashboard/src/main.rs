use crate::{entry::LogEntry, server::OmniPaxosServer, util::*};
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use omnipaxos_ui::OmniPaxosUI;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::runtime::Builder;

mod entry;
mod server;
mod util;

type OmniPaxosLog = OmniPaxos<LogEntry, MemoryStorage<LogEntry>>;

/// Here is the main function for the dashboard example. Including the nodes setup, and the main loop.
/// There will be a dashboard UI showing the status from the view of one node in the terminal, and in
/// each loop, some batched log entries will be appended to the leader, then the leader will be killed.
/// Finally there will be a majority of nodes remain and keep append log entries.
fn main() {
    let (num_nodes, attach_pid, duration, crash) = parse_arguments().unwrap();
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let servers: Vec<u64> = (1..=num_nodes).collect();
    let configuration_id = 1;
    let mut op_server_handles = HashMap::new();
    let (sender_channels, mut receiver_channels) = initialise_channels(&servers);
    // set up nodes
    for pid in &servers {
        let server_config = ServerConfig {
            pid: *pid,
            election_tick_timeout: ELECTION_TICK_TIMEOUT,
            custom_logger: Some(OmniPaxosUI::logger()),
            ..Default::default()
        };
        let cluster_config = ClusterConfig {
            configuration_id,
            nodes: servers.clone(),
            ..Default::default()
        };
        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };
        // set up the ui with the same configration as for the OmniPaxos
        let mut omni_paxos_ui = OmniPaxosUI::with(op_config.clone().into());
        if pid == &attach_pid {
            // start UI for the the node with id equals to majority, which will be the leader later
            omni_paxos_ui.start();
        }
        let omni_paxos: Arc<Mutex<OmniPaxosLog>> = Arc::new(Mutex::new(
            op_config.build(MemoryStorage::default()).unwrap(),
        ));
        let mut op_server = OmniPaxosServer {
            omni_paxos_ui,
            omni_paxos: Arc::clone(&omni_paxos),
            incoming: receiver_channels.remove(pid).unwrap(),
            outgoing: sender_channels.clone(),
        };
        let join_handle = runtime.spawn({
            async move {
                op_server.run().await;
            }
        });
        op_server_handles.insert(pid, (omni_paxos, join_handle));
    }
    let not_crash_server_pid = servers.iter().find(|x| **x != crash).unwrap();
    let (server, _handler) = op_server_handles.get(not_crash_server_pid).unwrap();
    // wait for leader to be elected...
    std::thread::sleep(WAIT_LEADER_TIMEOUT);
    for i in 0..BATCH_SIZE {
        let kv = LogEntry(i);
        server.lock().unwrap().append(kv).expect("append failed");
        std::thread::sleep(BATCH_PERIOD);
    }
    if crash > 0 {
        let (_crash_server, crash_handler) = op_server_handles.get(&crash).unwrap();
        crash_handler.abort();
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // batch append log entries
        for i in 0..BATCH_SIZE {
            let kv = LogEntry(i);
            server.lock().unwrap().append(kv).expect("append failed");
            std::thread::sleep(BATCH_PERIOD);
        }
    } else {
        std::thread::sleep(duration);
    }
}
