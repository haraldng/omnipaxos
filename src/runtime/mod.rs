use std::ops::RangeBounds;
use crate::core::{
    leader_election::ballot_leader_election::{messages::BLEMessage, *},
    messages::{Message},
    sequence_paxos::*,
    storage::*,
};
use std::time::Duration;
use tokio::{
    runtime::{Builder, Runtime},
    sync::{mpsc, oneshot, watch},
    time,
};
use crate::core::util::LogEntry;

#[derive(Debug)]
pub(crate) enum SPRequest<T: Entry, S: Snapshot<T>> {
    Append((T, oneshot::Sender<Result<(), ProposeErr<T>>>)),
    Snapshot(S),
    // Read(Read<T, S>)
}

/*
#[derive(Debug)]
struct Read<T: Entry, S: Snapshot<T>> {
    from_idx: Option<u64>,
    to_idx: Option<u64>,
    resp: oneshot::Sender<Option<Vec<LogEntry<T, S>>>>
}*/

pub(crate) struct SequencePaxosComp<T, S, B>
where
    T: Entry + Send + 'static ,
    S: Snapshot<T> + Send + 'static,
    B: Storage<T, S> + Send + 'static

{
    outgoing: mpsc::Sender<Message<T, S>>,
    incoming: mpsc::Receiver<Message<T, S>>,
    local_requests: mpsc::Receiver<SPRequest<T, S>>,
    ble: watch::Receiver<Ballot>,
    seq_paxos: SequencePaxos<T, S, B>,
    stop: oneshot::Receiver<Stop>,
}

impl<T, S, B> SequencePaxosComp<T, S, B>
where
    T: Entry + Send + 'static ,
    S: Snapshot<T> + Send + 'static,
    B: Storage<T, S> + Send + 'static
{
    pub(crate) fn new(
        sp: SequencePaxos<T, S, B>,
        local: mpsc::Receiver<SPRequest<T, S>>,
        incoming: mpsc::Receiver<Message<T, S>>,
        outgoing: mpsc::Sender<Message<T, S>>,
        ble: watch::Receiver<Ballot>,
        stop: oneshot::Receiver<Stop>,
    ) -> Self {
        Self {
            seq_paxos: sp,
            incoming,
            outgoing,
            ble,
            local_requests: local,
            stop,
        }
    }

    fn handle_local(&mut self, r: SPRequest<T, S>) {
        match r {
            SPRequest::Append((entry, requestee)) => {
                requestee.send(self.seq_paxos.append(entry)).expect("Failed to reply");
            }
            _ => todo!(),
        }
    }

    fn handle_incoming(&mut self, msg: Message<T, S>) {
        self.seq_paxos.handle(msg);
    }

    fn handle_leader_change(&mut self, b: Ballot) {
        self.seq_paxos.handle_leader(b);
    }

    async fn send_outgoing_msgs(&mut self) {
        for msg in self.seq_paxos.get_outgoing_msgs() {
            if let Err(_) = self.outgoing.send(msg).await {
                panic!("Outgoing channel dropped");
            }
        }
    }
    
    pub(crate) async fn run(&mut self) {
        let mut interval = time::interval(Duration::from_millis(100)); // TODO
        loop {
            tokio::select! {
                biased; // TODO

                _ = interval.tick() => {self.send_outgoing_msgs().await; },
                Ok(_) = self.ble.changed() => {
                    let ballot = *self.ble.borrow();
                    self.handle_leader_change(ballot);
                },
                Some(in_msg) = self.incoming.recv() => { self.handle_incoming(in_msg); },
                Some(local) = self.local_requests.recv() => { self.handle_local(local); },
                _ = &mut self.stop => { break; },
                else => { }
            }
        }
    }
}

pub struct SequencePaxosHandle<T: Entry, S: Snapshot<T>> {
    pub incoming: mpsc::Sender<Message<T, S>>,
    pub outgoing: mpsc::Receiver<Message<T, S>>,
}

impl<T: Entry, S: Snapshot<T>> SequencePaxosHandle<T, S> {
    pub(crate) fn with(incoming: mpsc::Sender<Message<T, S>>, outgoing: mpsc::Receiver<Message<T, S>>) -> Self {
        Self { incoming, outgoing }
    }
}

pub(crate) struct InternalSPHandle<T: Entry, S: Snapshot<T>> {
    pub stop: oneshot::Sender<Stop>,
    pub local_requests: mpsc::Sender<SPRequest<T, S>>,
}

impl<T, S> InternalSPHandle<T, S>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
{
    pub(crate) fn with(stop: oneshot::Sender<Stop>, local_requests: mpsc::Sender<SPRequest<T, S>>,) -> Self {
        Self { stop, local_requests }
    }
}

pub struct BLEHandle {
    pub incoming: mpsc::Sender<BLEMessage>,
    pub outgoing: mpsc::Receiver<BLEMessage>,
}

impl BLEHandle {
    pub(crate) fn with(incoming: mpsc::Sender<BLEMessage>, outgoing: mpsc::Receiver<BLEMessage>) -> Self {
        Self { incoming, outgoing }
    }
}

pub(crate) struct InternalBLEHandle {
    pub stop: oneshot::Sender<Stop>,
}

impl InternalBLEHandle {
    pub(crate) fn with(stop: oneshot::Sender<Stop>) -> Self {
        Self { stop }
    }
}

pub(crate) struct BLEComp {
    incoming: mpsc::Receiver<BLEMessage>,
    outgoing: mpsc::Sender<BLEMessage>,
    leader: watch::Sender<Ballot>,
    ble: BallotLeaderElection,
    stop: oneshot::Receiver<Stop>,
}

impl BLEComp {
    pub(crate) fn new(
        ble: BallotLeaderElection,
        leader: watch::Sender<Ballot>,
        incoming: mpsc::Receiver<BLEMessage>,
        outgoing: mpsc::Sender<BLEMessage>,
        stop: oneshot::Receiver<Stop>,
    ) -> Self {
        Self {
            ble,
            leader,
            incoming,
            outgoing,
            stop,
        }
    }

    fn handle_incoming(&mut self, msg: BLEMessage) {
        self.ble.handle(msg);
    }

    async fn send_outgoing_msgs(&mut self) {
        for msg in self.ble.get_outgoing_msgs() {
            if let Err(_) = self.outgoing.send(msg).await {
                panic!("Outgoing channel dropped");
            }
        }
    }

    fn tick(&mut self) {
        if let Some(ballot) = self.ble.tick() {
            self.leader.send(ballot).expect("Failed to trigger leader");
        }
    }
    
    pub(crate) async fn run(&mut self) {
        let mut outgoing_interval = time::interval(Duration::from_millis(50)); // TODO
        let mut tick_interval = time::interval(Duration::from_millis(50)); // TODO
        loop {
            tokio::select! {
                biased;
                Some(in_msg) = self.incoming.recv() => { self.handle_incoming(in_msg); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                _ = tick_interval.tick() => { self.tick(); },
                _ = &mut self.stop => break,
                else => {}
            }
        }
    }
}

pub(crate) struct Stop;
