use crate::ble::ballot_leader_election::Ballot;

#[derive(Clone, Debug)]
pub enum HeartbeatMsg {
    Request(HeartbeatRequest),
    Reply(HeartbeatReply),
}

#[derive(Clone, Debug)]
pub struct HeartbeatRequest {
    pub round: u32,
}

impl HeartbeatRequest {
    pub fn with(round: u32) -> HeartbeatRequest {
        HeartbeatRequest { round }
    }
}

#[derive(Clone, Debug)]
pub struct HeartbeatReply {
    pub round: u32,
    pub ballot: Ballot,
    pub candidate: bool,
}

impl HeartbeatReply {
    pub fn with(round: u32, ballot: Ballot, candidate: bool) -> HeartbeatReply {
        HeartbeatReply {
            round,
            ballot,
            candidate,
        }
    }
}

/// A struct for a Paxos message that also includes sender and receiver.
#[derive(Clone, Debug)]
pub struct BleMessage {
    /// Sender of `msg`.
    pub from: u64,
    /// Receiver of `msg`.
    pub to: u64,
    /// The message content.
    pub msg: HeartbeatMsg,
}

impl BleMessage {
    /// Creates a message.
    pub fn with(from: u64, to: u64, msg: HeartbeatMsg) -> Self {
        BleMessage { from, to, msg }
    }
}
