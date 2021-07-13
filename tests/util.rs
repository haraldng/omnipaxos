use self::{
    ble::{BallotLeaderComp, BallotLeaderElectionPort},
    omnireplica::OmniPaxosReplica,
};
use kompact::prelude::*;
use omnipaxos::leader_election::ballot_leader_election::BallotLeaderElection;
use omnipaxos::paxos::Paxos;
use omnipaxos::storage::memory_storage::{MemorySequence, MemoryState};
use omnipaxos::storage::{PaxosState, Sequence, Storage};
use omnipaxos::{
    leader_election::{
        ballot_leader_election::{messages::BLEMessage, Ballot},
        Leader,
    },
    messages::Message,
};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

pub struct TestSystem {
    kompact_system: KompactSystem,
    ble_paxos_nodes: HashMap<
        u64,
        (
            Arc<Component<BallotLeaderComp>>,
            Arc<Component<OmniPaxosReplica>>,
        ),
    >,
}

impl TestSystem {
    pub fn with(
        num_nodes: usize,
        ble_hb_delay: u64,
        ble_initial_delay_factor: Option<u64>,
        ble_initial_leader: Option<Leader<Ballot>>,
        quick_timeout: bool,
        increment_delay: u64,
    ) -> Self {
        let mut conf = KompactConfig::default();
        conf.set_config_value(&kompact::config_keys::system::THREADS, 8);
        let system = conf.build().expect("KompactSystem");
        let mut ble_paxos_nodes: HashMap<
            u64,
            (
                Arc<Component<BallotLeaderComp>>,
                Arc<Component<OmniPaxosReplica>>,
            ),
        > = HashMap::new();

        let all_pids: Vec<u64> = (1..=num_nodes as u64).collect();
        let mut ble_refs: HashMap<u64, ActorRef<BLEMessage>> = HashMap::new();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Ballot>>> = HashMap::new();

        for pid in 1..=num_nodes as u64 {
            let mut peer_pids = all_pids.clone();
            peer_pids.retain(|i| i != &pid);
            // create components
            let (ble_comp, ble_reg_f) = system.create_and_register(|| {
                BallotLeaderComp::with(
                    pid,
                    BallotLeaderElection::with(
                        peer_pids.clone(),
                        pid,
                        ble_hb_delay,
                        increment_delay,
                        quick_timeout,
                        ble_initial_leader,
                        ble_initial_delay_factor,
                    ),
                )
            });

            ble_reg_f.wait_expect(Duration::from_secs(3), "BLEComp failed to register!");

            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                OmniPaxosReplica::with(
                    pid,
                    Paxos::with(
                        1,
                        pid,
                        peer_pids.clone(),
                        Storage::with(
                            MemorySequence::<Ballot>::new(),
                            MemoryState::<Ballot>::new(),
                        ),
                        None,
                    ),
                )
            });

            omni_reg_f.wait_expect(Duration::from_secs(3), "ReplicaComp failed to register!");

            biconnect_components::<BallotLeaderElectionPort, _, _>(&ble_comp, &omni_replica)
                .expect("Could not connect BLE and OmniPaxosReplica!");

            ble_refs.insert(pid, ble_comp.actor_ref());
            omni_refs.insert(pid, omni_replica.actor_ref());
            ble_paxos_nodes.insert(pid, (ble_comp, omni_replica));
        }
        for (ble, omni) in ble_paxos_nodes.values() {
            ble.on_definition(|b| b.set_peers(ble_refs.clone()));
            omni.on_definition(|o| o.set_peers(omni_refs.clone()));
        }
        Self {
            kompact_system: system,
            ble_paxos_nodes,
        }
    }

    pub fn start_all_nodes(&mut self) {
        for (ble, omni) in self.ble_paxos_nodes.values() {
            self.kompact_system.start(ble);
            self.kompact_system.start(omni);
        }
    }

    pub fn kill_all_nodes(&mut self) {
        for (_pid, (ble, omni)) in std::mem::take(&mut self.ble_paxos_nodes) {
            self.kompact_system.kill(ble);
            self.kompact_system.kill(omni);
        }
    }

    pub fn kill_node(&mut self, id: u64) {
        let (ble, omni) = self.ble_paxos_nodes.remove(&id).unwrap();
        self.kompact_system.kill(ble);
        self.kompact_system.kill(omni);
    }
}

pub mod ble {
    use super::*;

    use std::time::Duration;

    pub struct BallotLeaderElectionPort;

    impl Port for BallotLeaderElectionPort {
        type Indication = Leader<Ballot>;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElectionPort>,
        pid: u64,
        peers: HashMap<u64, ActorRef<BLEMessage>>,
        pub leader: Option<Ballot>,
        timer: Option<ScheduledTimer>,
        ble: BallotLeaderElection,
    }

    impl BallotLeaderComp {
        pub fn with(pid: u64, ble: BallotLeaderElection) -> BallotLeaderComp {
            BallotLeaderComp {
                ctx: ComponentContext::uninitialised(),
                ble_port: ProvidedPort::uninitialised(),
                pid,
                peers: HashMap::new(),
                leader: None,
                timer: None,
                ble,
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<BLEMessage>>) {
            self.peers = peers;
        }

        fn send_outgoing_msgs(&mut self) {
            let outgoing = self.ble.get_outgoing_msgs();

            for out in outgoing {
                let receiver = self.peers.get(&out.to).unwrap();

                receiver.tell(out);
            }
        }
    }

    impl ComponentLifecycle for BallotLeaderComp {
        fn on_start(&mut self) -> Handled {
            self.timer = Some(self.schedule_periodic(
                Duration::from_millis(1),
                Duration::from_millis(1),
                move |c, _| {
                    if let Some(l) = c.ble.tick() {
                        c.ble_port.trigger(l);
                    }
                    c.send_outgoing_msgs();

                    Handled::Ok
                },
            ));

            Handled::Ok
        }

        fn on_kill(&mut self) -> Handled {
            if let Some(timer) = self.timer.take() {
                self.cancel_timer(timer);
            }
            Handled::Ok
        }
    }

    impl Provide<BallotLeaderElectionPort> for BallotLeaderComp {
        fn handle(&mut self, _: <BallotLeaderElectionPort as Port>::Request) -> Handled {
            // ignore
            Handled::Ok
        }
    }

    impl Actor for BallotLeaderComp {
        type Message = BLEMessage;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.ble.handle(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }
}

pub mod omnireplica {
    use super::{ble::BallotLeaderElectionPort, *};
    use omnipaxos::paxos::Paxos;
    use omnipaxos::storage::memory_storage::{MemorySequence, MemoryState};
    use omnipaxos::{
        leader_election::{ballot_leader_election::Ballot, Leader},
        messages::Message,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosReplica {
        ctx: ComponentContext<Self>,
        ble_port: RequiredPort<BallotLeaderElectionPort>,
        pid: u64,
        peers: HashMap<u64, ActorRef<Message<Ballot>>>,
        timer: Option<ScheduledTimer>,
        paxos: Paxos<Ballot, MemorySequence<Ballot>, MemoryState<Ballot>>,
    }

    impl ComponentLifecycle for OmniPaxosReplica {
        fn on_start(&mut self) -> Handled {
            self.timer = Some(self.schedule_periodic(
                Duration::from_millis(1),
                Duration::from_millis(1),
                move |c, _| {
                    c.send_outgoing_msgs();

                    // let decided_ent = c.paxos.get_decided_entries();
                    // for ent in decided_ent {
                    //     match ent {
                    //         Entry::Normal(x) => {
                    //
                    //         },
                    //         Entry::StopSign(x) => {
                    //             x.
                    //         }
                    //     }
                    // }

                    Handled::Ok
                },
            ));

            Handled::Ok
        }

        fn on_kill(&mut self) -> Handled {
            if let Some(timer) = self.timer.take() {
                self.cancel_timer(timer);
            }
            Handled::Ok
        }
    }

    impl OmniPaxosReplica {
        pub fn with(
            pid: u64,
            paxos: Paxos<Ballot, MemorySequence<Ballot>, MemoryState<Ballot>>,
        ) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                ble_port: RequiredPort::uninitialised(),
                pid,
                peers: HashMap::new(),
                timer: None,
                paxos,
            }
        }

        fn send_outgoing_msgs(&mut self) {
            let outgoing = self.paxos.get_outgoing_msgs();

            for out in outgoing {
                let receiver = self.peers.get(&out.to).unwrap();

                receiver.tell(out);
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<Ballot>>>) {
            self.peers = peers;
        }

        pub fn propose(&mut self, data: Vec<u8>) {
            self.paxos.propose_normal(data).expect("Failed to propose!");
        }
    }

    impl Actor for OmniPaxosReplica {
        type Message = Message<Ballot>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.paxos.handle(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }

    impl Require<BallotLeaderElectionPort> for OmniPaxosReplica {
        fn handle(&mut self, l: Leader<Ballot>) -> Handled {
            self.paxos.handle_leader(l);
            Handled::Ok
        }
    }
}
