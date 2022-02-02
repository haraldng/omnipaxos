use self::{
    ble::{BallotLeaderComp, BallotLeaderElectionPort},
    omnireplica::OmniPaxosReplica,
};
use kompact::{config_keys::system, executors::crossbeam_workstealing_pool, prelude::*};
use omnipaxos::{
    leader_election::ballot_leader_election::{messages::BLEMessage, Ballot, BallotLeaderElection},
    messages::Message,
    paxos::OmniPaxos,
    storage::{memory_storage::MemoryStorage, Snapshot},
};
use std::{collections::HashMap, str, sync::Arc, time::Duration};

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
        ble_initial_leader: Option<Ballot>,
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
        let mut omni_refs: HashMap<u64, ActorRef<Message<u64, LatestValue>>> = HashMap::new();

        for pid in 1..=num_nodes as u64 {
            let mut peer_pids = all_pids.clone();
            peer_pids.retain(|i| i != &pid);
            // create components
            let (ble_comp, ble_reg_f) = system.create_and_register(|| {
                BallotLeaderComp::with(BallotLeaderElection::with(
                    pid,
                    peer_pids.clone(),
                    None,
                    ble_hb_delay,
                    ble_initial_leader,
                    ble_initial_delay_factor,
                    None,
                    None,
                ))
            });

            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                OmniPaxosReplica::with(OmniPaxos::with(
                    1,
                    pid,
                    peer_pids.clone(),
                    MemoryStorage::default(),
                    None,
                    None,
                    None,
                ))
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
        type Indication = Ballot;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElectionPort>,
        peers: HashMap<u64, ActorRef<BLEMessage>>,
        pub leader: Option<Ballot>,
        timer: Option<ScheduledTimer>,
        ble: BallotLeaderElection,
        ask_vector: LinkedList<Ask<(), Ballot>>,
    }

    impl BallotLeaderComp {
        pub fn with(ble: BallotLeaderElection) -> BallotLeaderComp {
            BallotLeaderComp {
                ctx: ComponentContext::uninitialised(),
                ble_port: ProvidedPort::uninitialised(),
                peers: HashMap::new(),
                leader: None,
                timer: None,
                ble,
                ask_vector: LinkedList::new(),
            }
        }

        pub fn add_ask(&mut self, ask: Ask<(), Ballot>) {
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

        fn answer_future(&mut self, l: Ballot) {
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
    use omnipaxos::{
        leader_election::ballot_leader_election::Ballot, messages::Message, paxos::OmniPaxos,
        storage::memory_storage::MemoryStorage, util::LogEntry,
    };
    use std::{
        collections::{HashMap, LinkedList},
        time::Duration,
    };

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosReplica {
        ctx: ComponentContext<Self>,
        ble_port: RequiredPort<BallotLeaderElectionPort>,
        peers: HashMap<u64, ActorRef<Message<u64, LatestValue>>>,
        timer: Option<ScheduledTimer>,
        paxos: OmniPaxos<u64, LatestValue, MemoryStorage<u64, LatestValue>>,
        ask_vector: LinkedList<Ask<(), u64>>,
        decided_idx: u64,
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
        pub fn with(paxos: OmniPaxos<u64, LatestValue, MemoryStorage<u64, LatestValue>>) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                ble_port: RequiredPort::uninitialised(),
                peers: HashMap::new(),
                timer: None,
                paxos,
                ask_vector: LinkedList::new(),
                decided_idx: 0,
            }
        }

        pub fn add_ask(&mut self, ask: Ask<(), u64>) {
            self.ask_vector.push_back(ask);
        }

        pub fn get_trimmed_suffix(&self) -> Vec<u64> {
            if let Some(decided_ents) = self.paxos.read_decided_suffix(0) {
                let ents = match decided_ents.first().unwrap() {
                    LogEntry::Trimmed(_) | LogEntry::Snapshotted(_) => {
                        decided_ents.get(1..).unwrap()
                    }
                    _ => decided_ents.as_slice(),
                };
                ents.iter()
                    .map(|x| match x {
                        LogEntry::Decided(i) => **i,
                        err => panic!("{}", format!("Got unexpected entry: {:?}", err)),
                    })
                    .collect()
            } else {
                vec![]
            }
        }

        fn send_outgoing_msgs(&mut self) {
            let outgoing = self.paxos.get_outgoing_msgs();

            for out in outgoing {
                let receiver = self.peers.get(&out.to).unwrap();

                receiver.tell(out);
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<u64, LatestValue>>>) {
            self.peers = peers;
        }

        pub fn propose(&mut self, data: u64) {
            self.paxos.append(data).expect("Failed to propose!");
        }

        pub fn trim(&mut self, index: Option<u64>) {
            self.paxos.trim(index).expect("Failed to trim!");
        }

        fn answer_future(&mut self) {
            if !self.ask_vector.is_empty() {
                if let Some(entries) = self.paxos.read_decided_suffix(self.decided_idx) {
                    for e in entries {
                        match e {
                            LogEntry::Decided(i) => self
                                .ask_vector
                                .pop_front()
                                .unwrap()
                                .reply(*i)
                                .expect("Failed to reply promise!"),
                            err => panic!("{}", format!("Got unexpected entry: {:?}", err)),
                        }
                    }
                    self.decided_idx = self.paxos.get_decided_idx();
                }
            }
        }
    }

    impl Actor for OmniPaxosReplica {
        type Message = Message<u64, LatestValue>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.paxos.handle(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }

    impl Require<BallotLeaderElectionPort> for OmniPaxosReplica {
        fn handle(&mut self, l: Ballot) -> Handled {
            self.paxos.handle_leader(l);
            Handled::Ok
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialOrd, PartialEq)]
pub struct LatestValue {
    value: u64,
}

impl Snapshot<u64> for LatestValue {
    fn create(entries: &[u64]) -> Self {
        Self {
            value: *entries.last().unwrap_or(&0u64),
        }
    }

    fn merge(&mut self, delta: Self) {
        self.value = delta.value;
    }

    fn snapshottable() -> bool {
        true
    }
}
