use crate::entry::LogEntry;
use omnipaxos::{messages::Message, util::NodeId};
use std::{collections::HashMap, env, time::Duration};
use tokio::sync::mpsc;

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const UI_TICK_PERIOD: Duration = Duration::from_millis(100);
pub const BATCH_SIZE: u64 = 100;
pub const BATCH_PERIOD: Duration = Duration::from_millis(50);

#[allow(clippy::type_complexity)]
pub(crate) fn initialise_channels(
    servers: &[u64],
) -> (
    HashMap<NodeId, mpsc::Sender<Message<LogEntry>>>,
    HashMap<NodeId, mpsc::Receiver<Message<LogEntry>>>,
) {
    let mut sender_channels = HashMap::new();
    let mut receiver_channels = HashMap::new();

    for pid in servers {
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        sender_channels.insert(*pid, sender);
        receiver_channels.insert(*pid, receiver);
    }
    (sender_channels, receiver_channels)
}

pub(crate) fn parse_arguments() -> Result<(u64, u64, Duration, u64), String> {
    let args: Vec<String> = env::args().collect();

    let mut number_of_nodes: u64 = 3;
    let mut attach_node = number_of_nodes;
    let mut duration_in_seconds: u64 = 10;
    let mut crash: u64 = 0;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "nodes" => {
                i += 1;
                number_of_nodes = args[i].parse().expect("Invalid number of nodes.");
            }
            "attach" => {
                i += 1;
                attach_node = args[i].parse().expect("Invalid node id for attaching.");
            }
            "duration" => {
                i += 1;
                duration_in_seconds = args[i].parse().expect("Invalid duration."); // Set the default duration to 10
            }
            "crash" => {
                i += 1;
                crash = args[i].parse().expect("Invalid crash node.");
            }
            _ => {
                return Err(format!("Invalid argument: {}", args[i]));
            }
        }
        i += 1;
    }

    Ok((
        number_of_nodes,
        attach_node,
        Duration::from_secs(duration_in_seconds),
        crash,
    ))
}
