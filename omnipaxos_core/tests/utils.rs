use self::omnireplica::OmniPaxosComponent;
use commitlog::LogOptions;
use kompact::{config_keys::system, executors::crossbeam_workstealing_pool, prelude::*};
use omnipaxos_core::{
    ballot_leader_election::Ballot,
    messages::Message,
    storage::{Entry, Snapshot, StopSign, Storage},
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str, sync::Arc, time::Duration};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_millis(1000);
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(1000);
const STOP_COMPONENT_TIMEOUT: Duration = Duration::from_millis(1000);
const CHECK_DECIDED_TIMEOUT: Duration = Duration::from_millis(1);
pub const SS_METADATA: u8 = 255;
const COMMITLOG: &str = "/commitlog/";
const PERSISTENT: &str = "persistent";
const MEMORY: &str = "memory";

#[cfg(feature = "hocon_config")]
use hocon::{Error, Hocon, HoconLoader};
use omnipaxos_core::omni_paxos::OmniPaxosConfig;
use sled::Config;

#[cfg(feature = "hocon_config")]
/// Configuration for `TestSystem`. TestConfig loads the values from
/// the configuration file `test.conf` using hocon
pub struct TestConfig {
    pub wait_timeout: Duration,
    pub num_threads: usize,
    pub num_nodes: usize,
    pub election_timeout: Duration,
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
            election_timeout: Duration::from_millis(
                cfg["election_timeout_ms"].as_i64().unwrap_or_default() as u64,
            ),
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

/// An enum which can either be a 'PersistentStorage' or 'MemoryStorage', the type depends on the
/// 'StorageTypeSelector' enum. Used for testing purposes with SequencePaxos and BallotLeaderElection.
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
    pub fn with(storage_type: StorageTypeSelector, my_path: &str) -> Self {
        match storage_type {
            StorageTypeSelector::Persistent => {
                let my_logopts = LogOptions::new(format!("{my_path}{COMMITLOG}"));
                let my_sledopts = Config::new();
                let persist_conf =
                    PersistentStorageConfig::with(my_path.to_string(), my_logopts, my_sledopts);
                StorageType::Persistent(PersistentStorage::open(persist_conf))
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
    pub temp_dir_path: String,
    pub kompact_system: Option<KompactSystem>,
    pub nodes: HashMap<u64, Arc<Component<OmniPaxosComponent>>>,
}

impl TestSystem {
    pub fn with(
        num_nodes: usize,
        election_timeout: Duration,
        num_threads: usize,
        storage_type: StorageTypeSelector,
    ) -> Self {
        let temp_dir_path = create_temp_dir();

        let mut conf = KompactConfig::default();
        conf.set_config_value(&system::LABEL, "KompactSystem".to_string());
        conf.set_config_value(&system::THREADS, num_threads);
        Self::set_executor_for_threads(num_threads, &mut conf);

        let mut net = NetworkConfig::default();
        net.set_tcp_nodelay(true);

        conf.system_components(DeadletterBox::new, net.build());
        let system = conf.build().expect("KompactSystem");

        let mut nodes = HashMap::new();

        let all_pids: Vec<u64> = (1..=num_nodes as u64).collect();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Value, LatestValue>>> = HashMap::new();

        for pid in 1..=num_nodes as u64 {
            let peers: Vec<u64> = all_pids.iter().filter(|id| id != &&pid).cloned().collect();
            let mut op_config = OmniPaxosConfig::default();
            op_config.pid = pid;
            op_config.peers = peers;
            op_config.configuration_id = 1;
            let storage: StorageType<Value, LatestValue> =
                StorageType::with(storage_type, &format!("{temp_dir_path}{pid}"));
            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                OmniPaxosComponent::with(pid, op_config.build(storage), election_timeout)
            });
            omni_reg_f.wait_expect(REGISTRATION_TIMEOUT, "ReplicaComp failed to register!");
            omni_refs.insert(pid, omni_replica.actor_ref());
            nodes.insert(pid, omni_replica);
        }

        for omni in nodes.values() {
            omni.on_definition(|o| o.set_peers(omni_refs.clone()));
        }

        Self {
            kompact_system: Some(system),
            nodes,
            temp_dir_path,
        }
    }

    pub fn start_all_nodes(&self) {
        for node in self.nodes.values() {
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .start_notify(node)
                .wait_timeout(START_TIMEOUT)
                .expect("ReplicaComp never started!");
        }
    }

    pub fn stop_all_nodes(&self) {
        for node in self.nodes.values() {
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .stop_notify(node)
                .wait_timeout(STOP_COMPONENT_TIMEOUT)
                .expect("ReplicaComp replica never died!");
        }
    }

    pub fn kill_node(&mut self, id: u64) {
        let node = self.nodes.remove(&id).unwrap();
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .kill_notify(node)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("ReplicaComp replica never died!");
        println!("Killed node {}", id);
    }

    pub fn create_node(
        &mut self,
        pid: u64,
        num_nodes: usize,
        election_timeout: Duration,
        storage_type: StorageTypeSelector,
        storage_path: &str,
    ) {
        let peers: Vec<u64> = (1..=num_nodes as u64).filter(|id| id != &pid).collect();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Value, LatestValue>>> = HashMap::new();
        let mut op_config = OmniPaxosConfig::default();
        op_config.pid = pid;
        op_config.peers = peers;
        op_config.configuration_id = 1;
        let storage: StorageType<Value, LatestValue> =
            StorageType::with(storage_type, &format!("{storage_path}{pid}"));
        let (omni_replica, omni_reg_f) = self
            .kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .create_and_register(|| {
                OmniPaxosComponent::with(pid, op_config.build(storage), election_timeout)
            });

        omni_reg_f.wait_expect(REGISTRATION_TIMEOUT, "ReplicaComp failed to register!");

        // Insert the new node into vector of peers.
        omni_refs.insert(pid, omni_replica.actor_ref());

        for (other_pid, node) in self.nodes.iter() {
            // Insert each peer node into HashMap as peers to the new node
            omni_refs.insert(*other_pid, node.actor_ref());
            // Also insert the new node as a peer into their Hashmaps
            node.on_definition(|o| o.peers.insert(pid, omni_replica.actor_ref()));
        }

        // Set the peers of the new node, add it to HashMaps of nodes
        omni_replica.on_definition(|o| o.set_peers(omni_refs));
        self.nodes.insert(pid, omni_replica);
    }

    pub fn start_node(&self, pid: u64) {
        let node = self
            .nodes
            .get(&pid)
            .expect(&format!("Cannot find node {pid}"));
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .start_notify(node)
            .wait_timeout(START_TIMEOUT)
            .expect("ReplicaComp never started!");
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

pub mod omnireplica {
    use super::*;
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        messages::Message,
        omni_paxos::OmniPaxos,
        util::{LogEntry, NodeId},
    };
    use std::collections::HashMap;

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosComponent {
        ctx: ComponentContext<Self>,
        #[allow(dead_code)]
        pid: NodeId,
        pub peers: HashMap<u64, ActorRef<Message<Value, LatestValue>>>,
        paxos_timer: Option<ScheduledTimer>,
        tick_timer: Option<ScheduledTimer>,
        pub paxos: OmniPaxos<Value, LatestValue, StorageType<Value, LatestValue>>,
        pub decided_futures: Vec<Ask<(), Value>>,
        pub election_futures: Vec<Ask<(), Ballot>>,
        current_leader_ballot: Ballot,
        decided_idx: u64,
        election_timeout: Duration,
    }

    impl ComponentLifecycle for OmniPaxosComponent {
        fn on_start(&mut self) -> Handled {
            self.paxos_timer = Some(self.schedule_periodic(
                CHECK_DECIDED_TIMEOUT,
                CHECK_DECIDED_TIMEOUT,
                move |c, _| {
                    c.send_outgoing_msgs();
                    c.answer_decided_future();
                    Handled::Ok
                },
            ));
            self.tick_timer = Some(self.schedule_periodic(
                self.election_timeout,
                self.election_timeout,
                move |c, _| {
                    c.paxos.election_timeout();
                    if let Some(leader_ballot) = c.paxos.get_current_leader_ballot() {
                        if leader_ballot != c.current_leader_ballot {
                            c.current_leader_ballot = leader_ballot;
                            c.answer_election_future(leader_ballot);
                        }
                    }
                    Handled::Ok
                },
            ));
            Handled::Ok
        }

        fn on_kill(&mut self) -> Handled {
            if let Some(timer) = self.paxos_timer.take() {
                self.cancel_timer(timer);
            }
            Handled::Ok
        }
    }

    impl OmniPaxosComponent {
        pub fn with(
            pid: NodeId,
            paxos: OmniPaxos<Value, LatestValue, StorageType<Value, LatestValue>>,
            election_timeout: Duration,
        ) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                pid,
                peers: HashMap::new(),
                paxos_timer: None,
                tick_timer: None,
                paxos,
                decided_futures: vec![],
                election_futures: vec![],
                current_leader_ballot: Ballot::default(),
                decided_idx: 0,
                election_timeout,
            }
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
                        LogEntry::Decided(i) => *i,
                        err => panic!("{}", format!("Got unexpected entry: {:?}", err)),
                    })
                    .collect()
            } else {
                vec![]
            }
        }

        fn send_outgoing_msgs(&mut self) {
            let outgoing = self.paxos.outgoing_messages();
            for out in outgoing {
                let receiver = self.peers.get(&out.get_receiver()).unwrap();
                receiver.tell(out);
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<Value, LatestValue>>>) {
            self.peers = peers;
        }

        fn answer_election_future(&mut self, l: Ballot) {
            if !self.election_futures.is_empty() {
                self.election_futures.pop().unwrap().reply(l).unwrap();
            }
        }

        fn answer_decided_future(&mut self) {
            if !self.decided_futures.is_empty() {
                if let Some(entries) = self.paxos.read_decided_suffix(self.decided_idx) {
                    for e in entries {
                        match e {
                            LogEntry::Decided(i) => self
                                .decided_futures
                                .pop()
                                .unwrap()
                                .reply(i)
                                .expect("Failed to reply promise!"),
                            LogEntry::Snapshotted(s) => self
                                .decided_futures
                                .pop()
                                .unwrap()
                                .reply(s.snapshot.value)
                                .expect("Failed to reply promise!"),
                            LogEntry::StopSign(ss) => self
                                .decided_futures
                                .pop()
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

    impl Actor for OmniPaxosComponent {
        type Message = Message<Value, LatestValue>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.paxos.handle_incoming(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
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

/// Create a temporary directory in /tmp/
pub fn create_temp_dir() -> String {
    let dir = TempDir::new().expect("Failed to create temporary directory");
    let dir_path = dir.path().to_path_buf();
    dir_path.to_string_lossy().to_string()
}
