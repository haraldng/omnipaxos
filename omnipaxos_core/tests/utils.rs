use self::{
    ble::{BLEComponent, BallotLeaderElectionPort},
    omnireplica::SequencePaxosComponent,
};
use kompact::{config_keys::system, executors::crossbeam_workstealing_pool, prelude::*};
use omnipaxos_core::{
    ballot_leader_election::{messages::BLEMessage, BLEConfig, Ballot, BallotLeaderElection},
    messages::Message,
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{Entry, Snapshot, StopSign, Storage},
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};

use commitlog::LogOptions;
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str, sync::Arc, time::Duration};

const START_TIMEOUT: Duration = Duration::from_millis(1000);
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(1000);
const STOP_COMPONENT_TIMEOUT: Duration = Duration::from_millis(1000);
const BLE_TIMER_TIMEOUT: Duration = Duration::from_millis(50);
pub const SS_METADATA: u8 = 255;
const STORAGE: &str = "storage/";
const COMMITLOG: &str = "/commitlog/";
const PERSISTENT: &str = "persistent";
const MEMORY: &str = "memory";

#[cfg(feature = "hocon_config")]
use hocon::{Error, Hocon, HoconLoader};

#[cfg(feature = "hocon_config")]
/// Configuration for `TestSystem`. TestConfig loads the values from
/// the configuration file `test.conf` using hocon
pub struct TestConfig {
    pub wait_timeout: Duration,
    pub num_threads: usize,
    pub num_nodes: usize,
    pub ble_hb_delay: u64,
    pub num_proposals: u64,
    pub num_elections: u64,
    pub gc_idx: u64,
    pub storage_type: StorageTypeSelector,
}

#[cfg(feature = "hocon_config")]
impl TestConfig {
    pub fn load(name: &str) -> Result<TestConfig, Error> {
        let raw_cfg = HoconLoader::new()
            .load_file("tests/config/test.conf")?
            .hocon()?;

        let cfg: &Hocon = &raw_cfg[name];

        Ok(TestConfig {
            wait_timeout: cfg["wait_timeout"].as_duration().unwrap_or_default(),
            num_threads: cfg["num_threads"].as_i64().unwrap_or_default() as usize,
            num_nodes: cfg["num_nodes"].as_i64().unwrap_or_default() as usize,
            ble_hb_delay: cfg["ble_hb_delay"].as_i64().unwrap_or_default() as u64,
            num_proposals: cfg["num_proposals"].as_i64().unwrap_or_default() as u64,
            num_elections: cfg["num_elections"].as_i64().unwrap_or_default() as u64,
            gc_idx: cfg["gc_idx"].as_i64().unwrap_or_default() as u64,
            storage_type: StorageTypeSelector::with(
                &cfg["storage_type"]
                    .as_string()
                    .unwrap_or(MEMORY.to_string()),
            ),
        })
    }
}

/// An enum for selecting storage type. The type
/// can be set in `config/test.conf` at `storage_type`
#[derive(Clone, Copy)]
pub enum StorageTypeSelector {
    Persistent,
    Memory,
}

impl StorageTypeSelector {
    pub fn with(storage_type: &str) -> Self {
        match storage_type.to_lowercase().as_ref() {
            PERSISTENT => StorageTypeSelector::Persistent,
            MEMORY => StorageTypeSelector::Memory,
            _ => panic!("No such storage type: {}", storage_type),
        }
    }
}

/// An enum which can either be 'PersistentStorage' or
/// 'MemoryStorage' struct, the type depends on the
/// 'StorageTypeSelector' enum
pub enum StorageType<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    Persistent(PersistentStorage<T, S>),
    Memory(MemoryStorage<T, S>),
}

impl<T, S> StorageType<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    pub fn with(storage_type: StorageTypeSelector, path: &str) -> Self {
        match storage_type {
            StorageTypeSelector::Persistent => {
                let my_path = format!("{STORAGE}{path}");
                let my_logopts = LogOptions::new(format!("{my_path}{COMMITLOG}"));
                let mut my_rocksopts = Options::default();
                my_rocksopts.create_if_missing(true);

                let persist_conf =
                    PersistentStorageConfig::with(my_path.to_string(), my_logopts, my_rocksopts);
                StorageType::Persistent(PersistentStorage::with(persist_conf))
            }
            StorageTypeSelector::Memory => StorageType::Memory(MemoryStorage::default()),
        }
    }
}

impl<T, S> Storage<T, S> for StorageType<T, S>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    S: Snapshot<T> + Serialize + for<'a> Deserialize<'a>,
{
    fn append_entry(&mut self, entry: T) -> u64 {
        match self {
            StorageType::Persistent(persist_s) => persist_s.append_entry(entry),
            StorageType::Memory(mem_s) => mem_s.append_entry(entry),
        }
    }

    fn append_entries(&mut self, entries: Vec<T>) -> u64 {
        match self {
            StorageType::Persistent(persist_s) => persist_s.append_entries(entries),
            StorageType::Memory(mem_s) => mem_s.append_entries(entries),
        }
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
        match self {
            StorageType::Persistent(persist_s) => persist_s.append_on_prefix(from_idx, entries),
            StorageType::Memory(mem_s) => mem_s.append_on_prefix(from_idx, entries),
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_promise(n_prom),
            StorageType::Memory(mem_s) => mem_s.set_promise(n_prom),
        }
    }

    fn set_decided_idx(&mut self, ld: u64) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_decided_idx(ld),
            StorageType::Memory(mem_s) => mem_s.set_decided_idx(ld),
        }
    }

    fn get_decided_idx(&self) -> u64 {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_decided_idx(),
            StorageType::Memory(mem_s) => mem_s.get_decided_idx(),
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_accepted_round(na),
            StorageType::Memory(mem_s) => mem_s.set_accepted_round(na),
        }
    }

    fn get_accepted_round(&self) -> Ballot {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_accepted_round(),
            StorageType::Memory(mem_s) => mem_s.get_accepted_round(),
        }
    }

    fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_entries(from, to),
            StorageType::Memory(mem_s) => mem_s.get_entries(from, to),
        }
    }

    fn get_log_len(&self) -> u64 {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_log_len(),
            StorageType::Memory(mem_s) => mem_s.get_log_len(),
        }
    }

    fn get_suffix(&self, from: u64) -> Vec<T> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_suffix(from),
            StorageType::Memory(mem_s) => mem_s.get_suffix(from),
        }
    }

    fn get_promise(&self) -> Ballot {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_promise(),
            StorageType::Memory(mem_s) => mem_s.get_promise(),
        }
    }

    fn set_stopsign(&mut self, s: omnipaxos_core::storage::StopSignEntry) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_stopsign(s),
            StorageType::Memory(mem_s) => mem_s.set_stopsign(s),
        }
    }

    fn get_stopsign(&self) -> Option<omnipaxos_core::storage::StopSignEntry> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_stopsign(),
            StorageType::Memory(mem_s) => mem_s.get_stopsign(),
        }
    }

    fn trim(&mut self, idx: u64) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.trim(idx),
            StorageType::Memory(mem_s) => mem_s.trim(idx),
        }
    }

    fn set_compacted_idx(&mut self, idx: u64) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_compacted_idx(idx),
            StorageType::Memory(mem_s) => mem_s.set_compacted_idx(idx),
        }
    }

    fn get_compacted_idx(&self) -> u64 {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_compacted_idx(),
            StorageType::Memory(mem_s) => mem_s.get_compacted_idx(),
        }
    }

    fn set_snapshot(&mut self, snapshot: S) {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_snapshot(snapshot),
            StorageType::Memory(mem_s) => mem_s.set_snapshot(snapshot),
        }
    }

    fn get_snapshot(&self) -> Option<S> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_snapshot(),
            StorageType::Memory(mem_s) => mem_s.get_snapshot(),
        }
    }
}

pub struct TestSystem {
    pub kompact_system: Option<KompactSystem>,
    pub ble_paxos_nodes: HashMap<
        u64,
        (
            Arc<Component<BLEComponent>>,
            Arc<Component<SequencePaxosComponent>>,
        ),
    >,
}

impl TestSystem {
    pub fn with(
        num_nodes: usize,
        ble_hb_delay: u64,
        num_threads: usize,
        storage_type: StorageTypeSelector,
        test_name: &str,
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
                Arc<Component<BLEComponent>>,
                Arc<Component<SequencePaxosComponent>>,
            ),
        > = HashMap::new();

        let all_pids: Vec<u64> = (1..=num_nodes as u64).collect();
        let mut ble_refs: HashMap<u64, ActorRef<BLEMessage>> = HashMap::new();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Value, LatestValue>>> = HashMap::new();

        for pid in 1..=num_nodes as u64 {
            let peers: Vec<u64> = all_pids.iter().filter(|id| id != &&pid).cloned().collect();
            let mut ble_config = BLEConfig::default();
            ble_config.set_pid(pid);
            ble_config.set_peers(peers.clone());
            ble_config.set_hb_delay(ble_hb_delay);
            // create components
            let (ble_comp, ble_reg_f) = system
                .create_and_register(|| BLEComponent::with(BallotLeaderElection::with(ble_config)));

            let mut sp_config = SequencePaxosConfig::default();
            sp_config.set_pid(pid);
            sp_config.set_peers(peers);

            let storage: StorageType<Value, LatestValue> =
                StorageType::with(storage_type, &format!("{test_name}{pid}"));

            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                SequencePaxosComponent::with(SequencePaxos::with(sp_config, storage))
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
            kompact_system: Some(system),
            ble_paxos_nodes,
        }
    }

    pub fn start_all_nodes(&self) {
        for (ble, omni) in self.ble_paxos_nodes.values() {
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .start_notify(ble)
                .wait_timeout(START_TIMEOUT)
                .expect("BLEComp never started!");
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .start_notify(omni)
                .wait_timeout(START_TIMEOUT)
                .expect("ReplicaComp never started!");
        }
    }

    pub fn stop_all_nodes(&self) {
        for (_pid, (ble, omni)) in &self.ble_paxos_nodes {
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .stop_notify(ble)
                .wait_timeout(STOP_COMPONENT_TIMEOUT)
                .expect("BLEComp never died!");
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .stop_notify(omni)
                .wait_timeout(STOP_COMPONENT_TIMEOUT)
                .expect("ReplicaComp replica never died!");
        }
    }

    pub fn kill_node(&mut self, id: u64) {
        let (ble, omni) = self.ble_paxos_nodes.remove(&id).unwrap();
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .kill_notify(ble)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("BLEComp never died!");
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .kill_notify(omni)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("ReplicaComp replica never died!");
    }

    pub fn create_node(
        &mut self,
        pid: u64,
        num_nodes: usize,
        ble_hb_delay: u64,
        storage_type: StorageTypeSelector,
        test_name: &str,
    ) {
        let peers: Vec<u64> = (1..=num_nodes as u64)
            .collect::<Vec<u64>>()
            .iter()
            .filter(|id| id != &&pid)
            .cloned()
            .collect();
        let mut ble_refs: HashMap<u64, ActorRef<BLEMessage>> = HashMap::new();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Value, LatestValue>>> = HashMap::new();

        //ble
        let mut ble_config = BLEConfig::default();
        ble_config.set_pid(pid);
        ble_config.set_peers(peers.clone());
        ble_config.set_hb_delay(ble_hb_delay);
        let (ble_comp, ble_reg_f) = self
            .kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .create_and_register(|| BLEComponent::with(BallotLeaderElection::with(ble_config)));

        //sp
        let mut sp_config = SequencePaxosConfig::default();
        sp_config.set_pid(pid);
        sp_config.set_peers(peers);
        let storage: StorageType<Value, LatestValue> =
            StorageType::with(storage_type, &format!("{test_name}{pid}"));
        let (omni_replica, omni_reg_f) = self
            .kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .create_and_register(|| {
                SequencePaxosComponent::with(SequencePaxos::with(sp_config, storage))
            });

        biconnect_components::<BallotLeaderElectionPort, _, _>(&ble_comp, &omni_replica)
            .expect("Could not connect BLE and OmniPaxosReplica!");
        ble_reg_f.wait_expect(REGISTRATION_TIMEOUT, "BLEComp failed to register!");
        omni_reg_f.wait_expect(REGISTRATION_TIMEOUT, "ReplicaComp failed to register!");

        ble_refs.insert(pid, ble_comp.actor_ref());
        omni_refs.insert(pid, omni_replica.actor_ref());
        for (_, (other_pid, (ble, omni))) in self.ble_paxos_nodes.iter().enumerate() {
            ble_refs.insert(*other_pid, ble.actor_ref());
            omni_refs.insert(*other_pid, omni.actor_ref());
            ble.on_definition(|b| b.peers.insert(pid, ble_comp.actor_ref()));
            omni.on_definition(|o| o.peers.insert(pid, omni_replica.actor_ref()));
        }

        ble_comp.on_definition(|b| b.set_peers(ble_refs));
        omni_replica.on_definition(|o| o.set_peers(omni_refs));
        self.ble_paxos_nodes.insert(pid, (ble_comp, omni_replica));
    }

    pub fn start_node(&self, pid: u64) {
        let (new_ble, new_px) = self
            .ble_paxos_nodes
            .get(&pid)
            .expect(&format!("Cannot find node {pid}"));
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .start_notify(new_ble)
            .wait_timeout(START_TIMEOUT)
            .expect("BLEComp never started!");
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .start_notify(new_px)
            .wait_timeout(START_TIMEOUT)
            .expect("ReplicaComp never started!");
    }

    pub fn ble_paxos_nodes(
        &self,
    ) -> &HashMap<
        u64,
        (
            Arc<Component<BLEComponent>>,
            Arc<Component<SequencePaxosComponent>>,
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

impl Drop for TestSystem {
    fn drop(&mut self) {
        clear_storage();
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
    pub struct BLEComponent {
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElectionPort>,
        pub peers: HashMap<u64, ActorRef<BLEMessage>>,
        pub leader: Option<Ballot>,
        timer: Option<ScheduledTimer>,
        pub ble: BallotLeaderElection,
        ask_vector: LinkedList<Ask<(), Ballot>>,
    }

    impl BLEComponent {
        pub fn with(ble: BallotLeaderElection) -> BLEComponent {
            BLEComponent {
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

    impl ComponentLifecycle for BLEComponent {
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

    impl Provide<BallotLeaderElectionPort> for BLEComponent {
        fn handle(&mut self, _: <BallotLeaderElectionPort as Port>::Request) -> Handled {
            // ignore
            Handled::Ok
        }
    }

    impl Actor for BLEComponent {
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
    use omnipaxos_core::{
        ballot_leader_election::Ballot, messages::Message, sequence_paxos::SequencePaxos,
        util::LogEntry,
    };
    use std::{
        collections::{HashMap, LinkedList},
        time::Duration,
    };

    #[derive(ComponentDefinition)]
    pub struct SequencePaxosComponent {
        ctx: ComponentContext<Self>,
        ble_port: RequiredPort<BallotLeaderElectionPort>,
        pub peers: HashMap<u64, ActorRef<Message<Value, LatestValue>>>,
        timer: Option<ScheduledTimer>,
        pub paxos: SequencePaxos<Value, LatestValue, StorageType<Value, LatestValue>>,
        ask_vector: LinkedList<Ask<(), Value>>,
        decided_idx: u64,
    }

    impl ComponentLifecycle for SequencePaxosComponent {
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

    impl SequencePaxosComponent {
        pub fn with(
            paxos: SequencePaxos<Value, LatestValue, StorageType<Value, LatestValue>>,
        ) -> Self {
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

        pub fn add_ask(&mut self, ask: Ask<(), Value>) {
            self.ask_vector.push_back(ask);
        }

        pub fn get_trimmed_suffix(&self) -> Vec<Value> {
            if let Some(decided_ents) = self.paxos.read_decided_suffix(0) {
                let ents = match decided_ents.first().unwrap() {
                    LogEntry::Trimmed(_) | LogEntry::Snapshotted(_) => {
                        decided_ents.get(1..).unwrap()
                    }
                    _ => decided_ents.as_slice(),
                };
                ents.iter()
                    .map(|x| match x {
                        LogEntry::Decided(i) => *i, //todo removed *
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

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<Value, LatestValue>>>) {
            self.peers = peers;
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
                                .reply(i) //todo removed *
                                .expect("Failed to reply promise!"),
                            LogEntry::Snapshotted(s) => self
                                .ask_vector
                                .pop_front()
                                .unwrap()
                                .reply(s.snapshot.value)
                                .expect("Failed to reply promise!"),
                            LogEntry::StopSign(ss) => self
                                .ask_vector
                                .pop_front()
                                .unwrap()
                                .reply(stopsign_meta_to_value(&ss))
                                .expect("Failed to reply stopsign promise"),
                            err => panic!("{}", format!("Got unexpected entry: {:?}", err)),
                        }
                    }
                    self.decided_idx = self.paxos.get_decided_idx();
                }
            }
        }
    }

    impl Actor for SequencePaxosComponent {
        type Message = Message<Value, LatestValue>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.paxos.handle(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }

    impl Require<BallotLeaderElectionPort> for SequencePaxosComponent {
        fn handle(&mut self, l: Ballot) -> Handled {
            self.paxos.handle_leader(l);
            Handled::Ok
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Value(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct LatestValue {
    value: Value,
}

impl Snapshot<Value> for LatestValue {
    fn create(entries: &[Value]) -> Self {
        Self {
            value: *entries.last().unwrap_or(&Value(0)),
        }
    }

    fn merge(&mut self, delta: Self) {
        self.value = delta.value;
    }

    fn use_snapshots() -> bool {
        true
    }
}

fn stopsign_meta_to_value(ss: &StopSign) -> Value {
    let v = ss
        .metadata
        .as_ref()
        .expect("StopSign Metadata was None")
        .first()
        .expect("Empty metadata");
    Value(*v as u64)
}

pub fn clear_storage() {
    let _ = std::fs::remove_dir_all(STORAGE.to_string());
}
