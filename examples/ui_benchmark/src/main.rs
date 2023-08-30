// To run the benchmark for ui:
// cargo run --bin ui_benchmark

use omnipaxos::{macros::Entry, ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{thread, time::Duration};

#[derive(Entry, Clone, Debug)]
struct Entry {}

// Tick rate of `TICK_RATE ms/tick`
const TICK_RATE: u64 = 1;
const SERVERS: [u64; 3] = [1, 2, 3];
const ELECTION_TICK_TIMEOUT: u64 = 10;
const UI_UPDATE_TICK_TIMEOUT: u64 = 50;
const RESEND_MESSAGE_TICK_TIMEOUT: u64 = 100;
const NUMBER_OF_TICKS: u64 = 1000;

fn op_buildup() -> OmniPaxos<Entry, MemoryStorage<Entry>> {
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
    let omni_paxos: OmniPaxos<Entry, MemoryStorage<Entry>> =
        op_config.build(MemoryStorage::default()).unwrap();
    omni_paxos
}

fn bench_tick_with_ui() {
    let mut op = op_buildup();
    op.start_ui();
    // Tick with UI started
    let now = std::time::Instant::now();
    for _ in 0..NUMBER_OF_TICKS {
        op.tick();
    }
    let tick_ui_timeout = now.elapsed().as_nanos();

    // Tick with UI started and 1ms/tick
    let now = std::time::Instant::now();
    for _ in 0..NUMBER_OF_TICKS {
        op.tick();
        thread::sleep(Duration::from_millis(TICK_RATE));
    }
    let tick_1ms_ui_timeout = now.elapsed().as_millis();
    op.stop_ui();

    // Tick with UI stopped
    let now = std::time::Instant::now();
    for _ in 0..NUMBER_OF_TICKS {
        op.tick();
    }
    let tick_timeout = now.elapsed().as_nanos();

    // Tick with UI stopped and 1ms/tick
    let now = std::time::Instant::now();
    for _ in 0..NUMBER_OF_TICKS {
        op.tick();
        thread::sleep(Duration::from_millis(TICK_RATE));
    }
    let tick_1ms_timeout = now.elapsed().as_millis();

    // Output results
    println!(
        "Time elapsed of {:?} ticks with UI started: {:.2?} nanoseconds",
        NUMBER_OF_TICKS, tick_ui_timeout
    );
    println!(
        "Time elapsed of {:?} ticks without UI: {:.2?} nanoseconds",
        NUMBER_OF_TICKS, tick_timeout
    );
    println!(
        "Time elapsed of {:?} ticks with UI started and {:?}ms/tick: {:.2?} ms",
        NUMBER_OF_TICKS, TICK_RATE, tick_1ms_ui_timeout
    );
    println!(
        "Time elapsed of {:?} ticks without UI and {:?}ms/tick: {:.2?} ms",
        NUMBER_OF_TICKS, TICK_RATE, tick_1ms_timeout
    );
}

fn main() {
    bench_tick_with_ui();
}
