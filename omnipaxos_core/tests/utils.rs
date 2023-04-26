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
use std::{collections::HashMap, error::Error, fs, str, sync::Arc, time::Duration};
use tempfile::TempDir;
use toml;

const START_TIMEOUT: Duration = Duration::from_millis(1000);
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(1000);
const STOP_COMPONENT_TIMEOUT: Duration = Duration::from_millis(1000);
const CHECK_DECIDED_TIMEOUT: Duration = Duration::from_millis(1);
pub const SS_METADATA: u8 = 255;
const COMMITLOG: &str = "/commitlog/";
const PERSISTENT: &str = "persistent";
const MEMORY: &str = "memory";

use omnipaxos_core::omni_paxos::OmniPaxosConfig;
use sled::Config;

/// Configuration for `TestSystem`. TestConfig loads the values from
/// the configuration file `/tests/config/test.toml` using toml
#[derive(Deserialize)]
#[serde(default)]
pub struct TestConfig {
    pub num_threads: usize,
    pub num_nodes: usize,
    pub wait_timeout_sec: u64,
    pub election_timeout_ms: u64,
    pub storage_type: StorageTypeSelector,
    pub num_proposals: u64,
    pub num_elections: u64,
    pub gc_idx: u64,
}

impl TestConfig {
    pub fn load(name: &str) -> Result<TestConfig, Box<dyn Error>> {
        let config_file =
            fs::read_to_string("tests/config/test.toml").expect("Couldn't find config file.");
        let mut configs: HashMap<String, TestConfig> = toml::from_str(&config_file)?;
        let config = configs
            .remove(name)
            .expect(&format!("Couldnt find config for {}", name));
        Ok(config)
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            num_threads: 3,
            num_nodes: 3,
            wait_timeout_sec: 1,
            election_timeout_ms: 50,
            storage_type: StorageTypeSelector::Memory,
            num_proposals: 100,
            num_elections: 0,
            gc_idx: 0,
        }
    }
}
/// An enum for selecting storage type. The type
/// can be set in `config/test.conf` at `storage_type`
#[derive(Clone, Copy, Deserialize)]
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
        election_timeout_ms: u64,
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
                OmniPaxosComponent::with(
                    pid,
                    op_config.build(storage),
                    Duration::from_millis(election_timeout_ms),
                )
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
        election_timeout_ms: u64,
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
                OmniPaxosComponent::with(
                    pid,
                    op_config.build(storage),
                    Duration::from_millis(election_timeout_ms),
                )
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

    pub fn stop_node(&self, pid: u64) {
        let node = self
            .nodes
            .get(&pid)
            .expect(&format!("Cannot find node {pid}"));
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .stop_notify(node)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("ReplicaComp never stopped!");
    }

    /// Return the elected leader from `node`'s viewpoint. If there is no leader yet then
    /// wait until a leader is elected in the allocated time.
    pub fn get_elected_leader(&self, node: u64, wait_timeout: Duration) -> u64 {
        let node = self.nodes.get(&node).expect("No BLE component found");

        node.on_definition(|x| {
            let leader_pid = x.paxos.get_current_leader();
            leader_pid.unwrap_or_else(|| {
                // Leader is not elected yet
                let (kprom, kfuture) = promise::<Ballot>();
                x.election_futures.push(Ask::new(kprom, ()));

                let ballot = kfuture
                    .wait_timeout(wait_timeout)
                    .expect("No leader has been elected in the allocated time!");
                ballot.pid
            })
        })
    }

    /// Use node `proposer` to propose `proposals` then waits for the proposals
    /// to be decided.
    pub fn make_proposals(&self, proposer: u64, proposals: Vec<Value>, timeout: Duration) {
        let proposer = self
            .nodes
            .get(&proposer)
            .expect("No SequencePaxos component found");

        let mut proposal_futures = vec![];
        for val in proposals {
            let (kprom, kfuture) = promise::<Value>();
            proposer.on_definition(|x| {
                x.paxos.append(val).expect("Failed to append");
                x.decided_futures.push(Ask::new(kprom, ()));
            });
            proposal_futures.push(kfuture);
        }

        match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, timeout) {
            Ok(_) => {}
            Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
        }
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
    use std::collections::{HashMap, HashSet};

    const SNAPSHOTTED_DECIDE: Value = Value(0);

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosComponent {
        ctx: ComponentContext<Self>,
        #[allow(dead_code)]
        pid: NodeId,
        pub peers: HashMap<u64, ActorRef<Message<Value, LatestValue>>>,
        pub peer_disconnections: HashSet<u64>,
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
                peer_disconnections: HashSet::new(),
                paxos_timer: None,
                tick_timer: None,
                decided_idx: paxos.get_decided_idx(),
                paxos,
                decided_futures: vec![],
                election_futures: vec![],
                current_leader_ballot: Ballot::default(),
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
                if self.is_connected_to(&out.get_receiver()) {
                    let receiver = self.peers.get(&out.get_receiver()).unwrap();
                    receiver.tell(out);
                }
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<Value, LatestValue>>>) {
            self.peers = peers;
        }

        // Used to simulate a network fault to Component `pid`.
        pub fn set_connection(&mut self, pid: u64, is_connected: bool) {
            match is_connected {
                true => self.peer_disconnections.remove(&pid),
                false => self.peer_disconnections.insert(pid),
            };
        }

        pub fn is_connected_to(&self, pid: &u64) -> bool {
            self.peer_disconnections.get(pid).is_none()
        }

        fn answer_election_future(&mut self, l: Ballot) {
            if !self.election_futures.is_empty() {
                self.election_futures.pop().unwrap().reply(l).unwrap();
            }
        }

        fn answer_decided_future(&mut self) {
            if let Some(entries) = self.paxos.read_decided_suffix(self.decided_idx) {
                if !self.decided_futures.is_empty() {
                    for e in entries {
                        match e {
                            LogEntry::Decided(i) => self
                                .decided_futures
                                .pop()
                                .unwrap()
                                .reply(i)
                                .expect("Failed to reply promise!"),
                            LogEntry::Snapshotted(s) => {
                                // Reply with dummy value for futures which were trimmed away
                                for _ in 1..(s.trimmed_idx - self.decided_idx) {
                                    self.decided_futures
                                        .pop()
                                        .unwrap()
                                        .reply(SNAPSHOTTED_DECIDE)
                                        .expect("Failed to reply promise!");
                                }
                                self.decided_futures
                                    .pop()
                                    .unwrap()
                                    .reply(s.snapshot.value)
                                    .expect("Failed to reply promise!");
                            }
                            LogEntry::StopSign(ss) => self
                                .decided_futures
                                .pop()
                                .unwrap()
                                .reply(stopsign_meta_to_value(&ss))
                                .expect("Failed to reply stopsign promise"),
                            err => panic!("{}", format!("Got unexpected entry: {:?}", err)),
                        }
                    }
                }
                self.decided_idx = self.paxos.get_decided_idx();
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

pub mod verification {
    use super::{LatestValue, Value};
    use omnipaxos_core::{
        storage::{Snapshot, StopSign},
        util::LogEntry,
    };

    /// Verify that the log matches the proposed values, Depending on
    /// the timing the log should match one of the following cases.
    /// * All entries are decided, verify the decided entries
    /// * Only a snapshot was taken, verify the snapshot
    /// * A snapshot was taken and entries decided on afterwards, verify both the snapshot and entries
    pub fn verify_log(
        read_log: Vec<LogEntry<Value, LatestValue>>,
        proposals: Vec<Value>,
        num_proposals: u64,
    ) {
        match &read_log[..] {
            [LogEntry::Decided(_), ..] => verify_entries(&read_log, &proposals, 0, num_proposals),
            [LogEntry::Snapshotted(s)] => {
                let exp_snapshot = LatestValue::create(proposals.as_slice());
                verify_snapshot(&read_log, s.trimmed_idx, &exp_snapshot);
            }
            [LogEntry::Snapshotted(s), LogEntry::Decided(_), ..] => {
                let (snapshotted_proposals, last_proposals) =
                    proposals.split_at(s.trimmed_idx as usize);
                let (snapshot_entry, decided_entries) = read_log.split_at(1); // separate the snapshot from the decided entries
                let exp_snapshot = LatestValue::create(snapshotted_proposals);
                verify_snapshot(snapshot_entry, s.trimmed_idx, &exp_snapshot);
                verify_entries(decided_entries, last_proposals, 0, num_proposals);
            }
            _ => panic!("Unexpected entries in the log: {:?} ", read_log),
        }
    }

    /// Verify that the log has a single snapshot of the latest entry.
    pub fn verify_snapshot(
        read_entries: &[LogEntry<Value, LatestValue>],
        exp_compacted_idx: u64,
        exp_snapshot: &LatestValue,
    ) {
        assert_eq!(
            read_entries.len(),
            1,
            "Expected snapshot, got: {:?}",
            read_entries
        );
        match read_entries
            .first()
            .expect("Expected entry from first element")
        {
            LogEntry::Snapshotted(s) => {
                assert_eq!(s.trimmed_idx, exp_compacted_idx);
                assert_eq!(&s.snapshot, exp_snapshot);
            }
            e => {
                panic!("{}", format!("Not a snapshot: {:?}", e));
            }
        }
    }

    /// Verify that all log entries are decided and matches the proposed entries.
    pub fn verify_entries(
        read_entries: &[LogEntry<Value, LatestValue>],
        exp_entries: &[Value],
        offset: u64,
        decided_idx: u64,
    ) {
        assert_eq!(
            read_entries.len(),
            exp_entries.len(),
            "read: {:?}, expected: {:?}",
            read_entries,
            exp_entries
        );
        for (idx, entry) in read_entries.iter().enumerate() {
            let log_idx = idx as u64 + offset;
            match entry {
                LogEntry::Decided(i) if log_idx <= decided_idx => assert_eq!(*i, exp_entries[idx]),
                LogEntry::Undecided(i) if log_idx > decided_idx => assert_eq!(*i, exp_entries[idx]),
                e => panic!(
                    "{}",
                    format!(
                        "Unexpected entry at idx {}: {:?}, decided_idx: {}",
                        idx, e, decided_idx
                    )
                ),
            }
        }
    }

    /// Verify that the log entry contains only a stopsign matching `exp_stopsign`
    pub fn verify_stopsign(read_entries: &[LogEntry<Value, LatestValue>], exp_stopsign: &StopSign) {
        assert_eq!(
            read_entries.len(),
            1,
            "Expected StopSign, read: {:?}",
            read_entries
        );
        match read_entries.first().unwrap() {
            LogEntry::StopSign(ss) => {
                assert_eq!(ss, exp_stopsign);
            }
            e => {
                panic!("{}", format!("Not a StopSign: {:?}", e))
            }
        }
    }
}
