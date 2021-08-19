use self::{
    ble::{BallotLeaderComp, BallotLeaderElectionPort},
    omnireplica::OmniPaxosReplica,
};
use kompact::config_keys::system;
use kompact::executors::crossbeam_workstealing_pool;
use kompact::prelude::*;
use omnipaxos::leader_election::ballot_leader_election::BallotLeaderElection;
use omnipaxos::paxos::OmniPaxos;
use omnipaxos::storage::memory_storage::{MemorySequence, MemoryState};
use omnipaxos::storage::{PaxosState, Sequence, Storage};
use omnipaxos::{
    leader_election::{
        ballot_leader_election::{messages::BLEMessage, Ballot},
        Leader,
    },
    messages::Message,
};
use std::str;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

const START_TIMEOUT: Duration = Duration::from_millis(1000);
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(1000);
const STOP_COMPONENT_TIMEOUT: Duration = Duration::from_millis(1000);
const BLE_TIMER_TIMEOUT: Duration = Duration::from_millis(100);

pub struct TestSystem {
    pub kompact_system: KompactSystem,
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
        increment_delay: u64,
        num_threads: usize,
    ) -> Self {
        let mut conf = KompactConfig::default();
        conf.set_config_value(&system::LABEL, "KompactSystem".to_string());
        conf.set_config_value(&system::THREADS, num_threads);
        Self::set_executor_for_threads(num_threads, &mut conf);

        let mut net = NetworkConfig::default();
        net.set_tcp_nodelay(true);

        conf.system_components(DeadletterBox::new, net.build());
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
                        ble_initial_leader,
                        ble_initial_delay_factor,
                        None,
                        None,
                    ),
                )
            });

            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                OmniPaxosReplica::with(
                    pid,
                    OmniPaxos::with(
                        1,
                        pid,
                        peer_pids.clone(),
                        Storage::with(
                            MemorySequence::<Ballot>::new(),
                            MemoryState::<Ballot>::new(),
                        ),
                        None,
                        None,
                        None,
                    ),
                )
            });

            biconnect_components::<BallotLeaderElectionPort, _, _>(&ble_comp, &omni_replica)
                .expect("Could not connect BLE and OmniPaxosReplica!");

            ble_reg_f.wait_expect(REGISTRATION_TIMEOUT, "BLEComp failed to register!");
            omni_reg_f.wait_expect(REGISTRATION_TIMEOUT, "ReplicaComp failed to register!");

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

    pub fn start_all_nodes(&self) {
        for (ble, omni) in self.ble_paxos_nodes.values() {
            self.kompact_system
                .start_notify(ble)
                .wait_timeout(START_TIMEOUT)
                .expect("BLEComp never started!");
            self.kompact_system
                .start_notify(omni)
                .wait_timeout(START_TIMEOUT)
                .expect("ReplicaComp never started!");
        }
    }

    pub fn stop_all_nodes(&self) {
        for (_pid, (ble, omni)) in &self.ble_paxos_nodes {
            self.kompact_system
                .stop_notify(ble)
                .wait_timeout(STOP_COMPONENT_TIMEOUT)
                .expect("BLEComp never died!");
            self.kompact_system
                .stop_notify(omni)
                .wait_timeout(STOP_COMPONENT_TIMEOUT)
                .expect("ReplicaComp replica never died!");
        }
    }

    pub fn kill_node(&mut self, id: u64) {
        let (ble, omni) = self.ble_paxos_nodes.remove(&id).unwrap();
        self.kompact_system
            .kill_notify(ble)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("BLEComp never died!");
        self.kompact_system
            .kill_notify(omni)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("ReplicaComp replica never died!");
    }

    pub fn ble_paxos_nodes(
        &self,
    ) -> &HashMap<
        u64,
        (
            Arc<Component<BallotLeaderComp>>,
            Arc<Component<OmniPaxosReplica>>,
        ),
    > {
        &self.ble_paxos_nodes
    }

    fn set_executor_for_threads(threads: usize, conf: &mut KompactConfig) -> () {
        if threads <= 32 {
            conf.executor(|t| crossbeam_workstealing_pool::small_pool(t))
        } else if threads <= 64 {
            conf.executor(|t| crossbeam_workstealing_pool::large_pool(t))
        } else {
            conf.executor(|t| crossbeam_workstealing_pool::dyn_pool(t))
        };
    }
}

pub mod ble {
    use super::*;
    use std::collections::LinkedList;

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
        ask_vector: LinkedList<Ask<(), Leader<Ballot>>>,
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
                ask_vector: LinkedList::new(),
            }
        }

        pub fn add_ask(&mut self, ask: Ask<(), Leader<Ballot>>) {
            self.ask_vector.push_back(ask);
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

        fn answer_future(&mut self, l: Leader<Ballot>) {
            if !self.ask_vector.is_empty() {
                match self.ask_vector.pop_front().unwrap().reply(l) {
                    Ok(_) => {}
                    Err(e) => println!("Error in promise {}", e),
                }
            }
        }
    }

    impl ComponentLifecycle for BallotLeaderComp {
        fn on_start(&mut self) -> Handled {
            self.ble.new_hb_round();
            self.timer =
                Some(
                    self.schedule_periodic(BLE_TIMER_TIMEOUT, BLE_TIMER_TIMEOUT, move |c, _| {
                        if let Some(l) = c.ble.tick() {
                            c.answer_future(l);

                            c.ble_port.trigger(l);
                        }
                        c.send_outgoing_msgs();

                        Handled::Ok
                    }),
                );

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
    use omnipaxos::paxos::OmniPaxos;
    use omnipaxos::storage::memory_storage::{MemorySequence, MemoryState};
    use omnipaxos::storage::Entry;
    use omnipaxos::{
        leader_election::{ballot_leader_election::Ballot, Leader},
        messages::Message,
    };
    use std::collections::{HashMap, LinkedList};
    use std::time::Duration;

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosReplica {
        ctx: ComponentContext<Self>,
        ble_port: RequiredPort<BallotLeaderElectionPort>,
        pid: u64,
        peers: HashMap<u64, ActorRef<Message<Ballot>>>,
        timer: Option<ScheduledTimer>,
        paxos: OmniPaxos<Ballot, MemorySequence<Ballot>, MemoryState<Ballot>>,
        ask_vector: LinkedList<Ask<(), Entry<Ballot>>>,
    }

    impl ComponentLifecycle for OmniPaxosReplica {
        fn on_start(&mut self) -> Handled {
            self.timer = Some(self.schedule_periodic(
                Duration::from_millis(1),
                Duration::from_millis(1),
                move |c, _| {
                    c.send_outgoing_msgs();

                    c.answer_future();

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
            paxos: OmniPaxos<Ballot, MemorySequence<Ballot>, MemoryState<Ballot>>,
        ) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                ble_port: RequiredPort::uninitialised(),
                pid,
                peers: HashMap::new(),
                timer: None,
                paxos,
                ask_vector: LinkedList::new(),
            }
        }

        pub fn add_ask(&mut self, ask: Ask<(), Entry<Ballot>>) {
            self.ask_vector.push_back(ask);
        }

        pub fn stop_and_get_sequence(&mut self) -> Arc<MemorySequence<Ballot>> {
            self.paxos.stop_and_get_sequence()
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

        pub fn garbage_collect(&mut self, index: Option<u64>) {
            self.paxos.garbage_collect(index)
        }

        fn answer_future(&mut self) {
            if !self.ask_vector.is_empty() {
                for ent in self.paxos.get_latest_decided_entries().iter() {
                    match self.ask_vector.pop_front().unwrap().reply(ent.clone()) {
                        Ok(_) => {}
                        Err(e) => println!("Error in promise {}", e),
                    }
                }
            }
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
