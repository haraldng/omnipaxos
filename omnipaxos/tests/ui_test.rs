// To run the benchmark for ui:
// cargo test --test ui_test --features "ui, derive_entry" -- --nocapture

use omnipaxos_macros::Entry;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::thread;
use std::time::Duration;

pub mod utils;

#[derive(Entry, Clone, Debug)]
struct Entry {}

fn op_buildup() -> OmniPaxos<Entry, MemoryStorage<Entry>> {
    const SERVERS: [u64; 3] = [1, 2, 3];
    const ELECTION_TICK_TIMEOUT: u64 = 10;
    const UI_UPDATE_TICK_TIMEOUT: u64 = 50;
    const RESEND_MESSAGE_TICK_TIMEOUT: u64 = 100;

    let server_config = ServerConfig {
        pid: SERVERS[0],
        election_tick_timeout: ELECTION_TICK_TIMEOUT,
        ui_update_tick_timeout: UI_UPDATE_TICK_TIMEOUT,
        resend_message_tick_timeout: RESEND_MESSAGE_TICK_TIMEOUT,
        ..Default::default()
    };
    let cluster_config = ClusterConfig {
        nodes: SERVERS.into(),
        configuration_id: 1,
        ..Default::default()
    };
    let op_config = OmniPaxosConfig {
        server_config,
        cluster_config,
    };
    let omni_paxos: OmniPaxos<Entry, MemoryStorage<Entry>> = op_config.build(MemoryStorage::default()).unwrap();
    omni_paxos
}

#[test]
fn bench_tick_with_ui() {
    let mut op = op_buildup();
    op.start_ui();
    // Tick with UI started
    let now = std::time::Instant::now();
    for _ in 0..100 {
        op.tick();
    }
    let tick_ui_timeout = now.elapsed().as_nanos();

    // Tick with UI started and 1ms/tick
    let now = std::time::Instant::now();
    for _ in 0..100 {
        op.tick();
        thread::sleep(Duration::from_millis(1));
    }
    let tick_1ms_ui_timeout = now.elapsed().as_millis();
    op.stop_ui();

    // Tick with UI stopped
    let now = std::time::Instant::now();
    for _ in 0..100 {
        op.tick();
    }
    let tick_timeout = now.elapsed().as_nanos();

    // Tick with UI stopped and 1ms/tick
    let now = std::time::Instant::now();
    for _ in 0..100 {
        op.tick();
        thread::sleep(Duration::from_millis(1));
    }
    let tick_1ms_timeout = now.elapsed().as_millis();

    // Output results
    println!("Time elapsed of 100 ticks with UI started: {:.2?} nanoseconds", tick_ui_timeout);
    println!("Time elapsed of 100 ticks without UI: {:.2?} nanoseconds", tick_timeout);
    println!("Time elapsed of 100 ticks with UI started and 1ms/tick: {:.2?} ms", tick_1ms_ui_timeout);
    println!("Time elapsed of 100 ticks without UI and 1ms/tick: {:.2?} ms", tick_1ms_timeout);
}
