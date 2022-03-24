use crate::util::defaults::TICK_INTERVAL;
use omnipaxos_core::ballot_leader_election::{
    messages::BLEMessage, BLEConfig, Ballot, BallotLeaderElection,
};
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

use crate::util::Stop;

pub struct BLEHandle {
    pub incoming: mpsc::Sender<BLEMessage>,
    pub outgoing: mpsc::Receiver<BLEMessage>,
}

impl BLEHandle {
    pub(crate) fn with(
        incoming: mpsc::Sender<BLEMessage>,
        outgoing: mpsc::Receiver<BLEMessage>,
    ) -> Self {
        Self { incoming, outgoing }
    }
}

pub(crate) struct InternalBLEHandle {
    pub stop: Option<oneshot::Sender<Stop>>, // wrap in option to be able to move it when stopping
}

impl InternalBLEHandle {
    pub(crate) fn with(stop: oneshot::Sender<Stop>) -> Self {
        Self { stop: Some(stop) }
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
        ble_conf: BLEConfig,
        leader: watch::Sender<Ballot>,
        incoming: mpsc::Receiver<BLEMessage>,
        outgoing: mpsc::Sender<BLEMessage>,
        stop: oneshot::Receiver<Stop>,
    ) -> Self {
        let ble = BallotLeaderElection::with(ble_conf);
        Self {
            ble,
            leader,
            incoming,
            outgoing,
            stop,
        }
    }

    fn handle_incoming(&mut self, msg: BLEMessage) {
        //println!("receive message : {:?}",msg);
        self.ble.handle(msg);
    }

    async fn send_outgoing_msgs(&mut self) {
        for msg in self.ble.get_outgoing_msgs() {
            //println!("try to send {:?}", msg);
            if let Err(_) = self.outgoing.send(msg).await {
                panic!("Outgoing channel dropped");
            }
        }
    }

    fn tick(&mut self) {
        //if not None
        if let Some(ballot) = self.ble.tick() {
            self.leader.send(ballot).expect("Failed to trigger leader");
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut outgoing_interval = time::interval(Duration::from_millis(1));
        let mut tick_interval = time::interval(TICK_INTERVAL);
        loop {
            tokio::select! {
                //control polling order
                //biased;
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await; 
                },
                Some(in_msg) = self.incoming.recv() => {
                    //println!("receive message : {:?}",in_msg);
                    self.handle_incoming(in_msg); 
                },
                _ = tick_interval.tick() => { 
                    self.tick(); 
                },
                _ = &mut self.stop => break,
                else => {}
            }
        }
    }
}
