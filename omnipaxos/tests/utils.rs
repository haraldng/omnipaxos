use self::omnireplica::OmniPaxosComponent;
use commitlog::LogOptions;
use kompact::{config_keys::system, executors::crossbeam_workstealing_pool, prelude::*};
use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::Message,
    storage::{Entry, Snapshot, Storage, StorageResult},
    util::{FlexibleQuorum, NodeId},
    ClusterConfig, ServerConfig,
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    error::Error,
    fs, str,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use toml;

const START_TIMEOUT: Duration = Duration::from_millis(1000);
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(1000);
const STOP_COMPONENT_TIMEOUT: Duration = Duration::from_millis(1000);
const CHECK_DECIDED_TIMEOUT: Duration = Duration::from_millis(1);
const COMMITLOG: &str = "/commitlog/";
use omnipaxos::OmniPaxosConfig;
use sled::Config;

/// Configuration for `TestSystem`. TestConfig loads the values from
/// the configuration file `/tests/config/test.toml` using toml
#[derive(Deserialize, Clone, Copy)]
#[serde(default)]
pub struct TestConfig {
    pub num_threads: usize,
    pub num_nodes: usize,
    pub wait_timeout_ms: u64,
    pub election_timeout_ms: u64,
    pub resend_message_timeout_ms: u64,
    pub storage_type: StorageTypeSelector,
    pub num_proposals: u64,
    pub num_elections: u64,
    pub gc_idx: u64,
    pub flexible_quorum: Option<(usize, usize)>,
    pub batch_size: usize,
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

    pub fn into_omnipaxos_config(&self, pid: NodeId) -> OmniPaxosConfig {
        let all_pids: Vec<u64> = (1..=self.num_nodes as u64).collect();
        let flexible_quorum =
            self.flexible_quorum
                .and_then(|(read_quorum_size, write_quorum_size)| {
                    Some(FlexibleQuorum {
                        read_quorum_size,
                        write_quorum_size,
                    })
                });
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: all_pids.clone(),
            flexible_quorum,
            ..Default::default()
        };
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: 1,
            // Make tick timeouts reletive to election timeout
            resend_message_tick_timeout: self.resend_message_timeout_ms / self.election_timeout_ms,
            batch_size: self.batch_size,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            num_threads: 3,
            num_nodes: 3,
            wait_timeout_ms: 3000,
            election_timeout_ms: 50,
            resend_message_timeout_ms: 500,
            storage_type: StorageTypeSelector::Memory,
            num_proposals: 100,
            num_elections: 0,
            gc_idx: 0,
            flexible_quorum: None,
            batch_size: 1,
        }
    }
}
/// An enum for selecting storage type. The type
/// can be set in `config/test.conf` at `storage_type`
#[derive(Clone, Copy, Deserialize)]
#[serde(tag = "type")]
pub enum StorageTypeSelector {
    Persistent,
    Memory,
    Broken(BrokenStorageConfig),
}

#[derive(Clone, Copy, Debug, Deserialize, Default)]
#[serde(default)]
pub struct BrokenStorageConfig {
    /// Fail once after this many operations
    fail_in: usize,
    op_counter: usize,
}

impl BrokenStorageConfig {
    /// Should be called before every operation on the broken storage.
    /// Returns Ok(_) if the operation should be performed without error.
    /// Returns Err(_) if the operation should fail.
    pub fn next(&mut self) -> StorageResult<()> {
        let err = Err("test error from mocked broken storage".into());
        self.op_counter += 1;
        if self.fail_in > 0 {
            self.fail_in -= 1;
            if self.fail_in == 0 {
                return err;
            }
        }
        Ok(())
    }

    /// Schedules a single failure after n operations.
    /// If `n == 1`, the next operation fails.
    pub fn schedule_failure_in(&mut self, n: usize) {
        self.fail_in = n;
    }
}

/// An enum which can either be a 'PersistentStorage' or 'MemoryStorage', the type depends on the
/// 'StorageTypeSelector' enum. Used for testing purposes with SequencePaxos and BallotLeaderElection.
/// Supports simulating storage failures in the `Broken` variant.
pub enum StorageType<T>
where
    T: Entry,
{
    Persistent(PersistentStorage<T>),
    Memory(MemoryStorage<T>),
    /// Mocks a storage that fails depending of the config.
    /// Arc<Mutex<_>> is needed since we need to mutate conf through immutable references.
    Broken(
        Arc<Mutex<MemoryStorage<T>>>,
        Arc<Mutex<BrokenStorageConfig>>,
    ),
}

impl<T> StorageType<T>
where
    T: Entry,
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
            StorageTypeSelector::Broken(config) => StorageType::Broken(
                Arc::new(Mutex::new(MemoryStorage::default())),
                Arc::new(Mutex::new(config)),
            ),
        }
    }

    pub fn with_memory(mem: MemoryStorage<T>) -> Self {
        StorageType::Memory(mem)
    }
}

impl<T> Storage<T> for StorageType<T>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.append_entry(entry),
            StorageType::Memory(mem_s) => mem_s.append_entry(entry),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().append_entry(entry)
            }
        }
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.append_entries(entries),
            StorageType::Memory(mem_s) => mem_s.append_entries(entries),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().append_entries(entries)
            }
        }
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.append_on_prefix(from_idx, entries),
            StorageType::Memory(mem_s) => mem_s.append_on_prefix(from_idx, entries),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().append_on_prefix(from_idx, entries)
            }
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_promise(n_prom),
            StorageType::Memory(mem_s) => mem_s.set_promise(n_prom),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().set_promise(n_prom)
            }
        }
    }

    fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_decided_idx(ld),
            StorageType::Memory(mem_s) => mem_s.set_decided_idx(ld),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().set_decided_idx(ld)
            }
        }
    }

    fn get_decided_idx(&self) -> StorageResult<u64> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_decided_idx(),
            StorageType::Memory(mem_s) => mem_s.get_decided_idx(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_decided_idx()
            }
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_accepted_round(na),
            StorageType::Memory(mem_s) => mem_s.set_accepted_round(na),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().set_accepted_round(na)
            }
        }
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_accepted_round(),
            StorageType::Memory(mem_s) => mem_s.get_accepted_round(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_accepted_round()
            }
        }
    }

    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_entries(from, to),
            StorageType::Memory(mem_s) => mem_s.get_entries(from, to),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_entries(from, to)
            }
        }
    }

    fn get_log_len(&self) -> StorageResult<u64> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_log_len(),
            StorageType::Memory(mem_s) => mem_s.get_log_len(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_log_len()
            }
        }
    }

    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_suffix(from),
            StorageType::Memory(mem_s) => mem_s.get_suffix(from),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_suffix(from)
            }
        }
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_promise(),
            StorageType::Memory(mem_s) => mem_s.get_promise(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_promise()
            }
        }
    }

    fn set_stopsign(&mut self, s: Option<omnipaxos::storage::StopSign>) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_stopsign(s),
            StorageType::Memory(mem_s) => mem_s.set_stopsign(s),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().set_stopsign(s)
            }
        }
    }

    fn get_stopsign(&self) -> StorageResult<Option<omnipaxos::storage::StopSign>> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_stopsign(),
            StorageType::Memory(mem_s) => mem_s.get_stopsign(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_stopsign()
            }
        }
    }

    fn trim(&mut self, idx: u64) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.trim(idx),
            StorageType::Memory(mem_s) => mem_s.trim(idx),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().trim(idx)
            }
        }
    }

    fn set_compacted_idx(&mut self, idx: u64) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_compacted_idx(idx),
            StorageType::Memory(mem_s) => mem_s.set_compacted_idx(idx),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().set_compacted_idx(idx)
            }
        }
    }

    fn get_compacted_idx(&self) -> StorageResult<u64> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_compacted_idx(),
            StorageType::Memory(mem_s) => mem_s.get_compacted_idx(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_compacted_idx()
            }
        }
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.set_snapshot(snapshot),
            StorageType::Memory(mem_s) => mem_s.set_snapshot(snapshot),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().set_snapshot(snapshot)
            }
        }
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        match self {
            StorageType::Persistent(persist_s) => persist_s.get_snapshot(),
            StorageType::Memory(mem_s) => mem_s.get_snapshot(),
            StorageType::Broken(mem_s, conf) => {
                conf.lock().unwrap().next()?;
                mem_s.lock().unwrap().get_snapshot()
            }
        }
    }
}

pub struct TestSystem {
    pub temp_dir_path: String,
    pub kompact_system: Option<KompactSystem>,
    pub nodes: HashMap<u64, Arc<Component<OmniPaxosComponent>>>,
}

impl TestSystem {
    pub fn with(test_config: TestConfig) -> Self {
        let temp_dir_path = create_temp_dir();

        let mut conf = KompactConfig::default();
        conf.set_config_value(&system::LABEL, "KompactSystem".to_string());
        conf.set_config_value(&system::THREADS, test_config.num_threads);
        Self::set_executor_for_threads(test_config.num_threads, &mut conf);

        let mut net = NetworkConfig::default();
        net.set_tcp_nodelay(true);

        conf.system_components(DeadletterBox::new, net.build());
        let system = conf.build().expect("KompactSystem");

        let mut nodes = HashMap::new();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Value>>> = HashMap::new();

        for pid in 1..=test_config.num_nodes as u64 {
            let op_config = test_config.into_omnipaxos_config(pid);
            let storage: StorageType<Value> =
                StorageType::with(test_config.storage_type, &format!("{temp_dir_path}{pid}"));
            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                OmniPaxosComponent::with(
                    pid,
                    op_config.build(storage).unwrap(),
                    test_config.election_timeout_ms,
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
        pid: NodeId,
        test_config: &TestConfig,
        storage: StorageType<Value>,
    ) {
        let mut omni_refs: HashMap<u64, ActorRef<Message<Value>>> = HashMap::new();
        let op_config = test_config.into_omnipaxos_config(pid);
        let (omni_replica, omni_reg_f) = self
            .kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .create_and_register(|| {
                OmniPaxosComponent::with(
                    pid,
                    op_config.build(storage).unwrap(),
                    test_config.election_timeout_ms,
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

    pub fn set_node_connections(&self, pid: u64, connection_status: bool) {
        // Remove outgoing connections
        let node = self.nodes.get(&pid).expect("Cannot find {pid}");
        node.on_definition(|comp| {
            for node_id in self.nodes.keys() {
                comp.set_connection(*node_id, connection_status);
            }
        });
        // Remove incoming connections
        for node_id in self.nodes.keys() {
            let node = self.nodes.get(&node_id).expect("Cannot find {pid}");
            node.on_definition(|comp| {
                comp.set_connection(pid, connection_status);
            });
        }
    }

    /// Return the elected leader from `node`'s viewpoint. If there is no leader yet then
    /// wait until a leader is elected in the allocated time.
    pub fn get_elected_leader(&self, node_id: u64, wait_timeout: Duration) -> u64 {
        let node = self.nodes.get(&node_id).expect("No BLE component found");

        let leader_pid = node.on_definition(|x| x.paxos.get_current_leader());
        leader_pid.unwrap_or_else(|| self.get_next_leader(node_id, wait_timeout))
    }

    /// Return the next new elected leader from `node`'s viewpoint. If there is no leader yet then
    /// wait until a leader is elected in the allocated time.
    pub fn get_next_leader(&self, node_id: u64, wait_timeout: Duration) -> u64 {
        let node = self.nodes.get(&node_id).expect("No BLE component found");
        let (kprom, kfuture) = promise::<Ballot>();
        node.on_definition(|x| x.election_futures.push(Ask::new(kprom, ())));
        let ballot = kfuture
            .wait_timeout(wait_timeout)
            .expect("No leader has been elected in the allocated time!");
        ballot.pid
    }

    /// Use node `proposer` to propose `proposals` then waits for the proposals
    /// to be decided.
    pub fn make_proposals(&self, proposer: u64, proposals: Vec<Value>, timeout: Duration) {
        let proposer = self
            .nodes
            .get(&proposer)
            .expect("No SequencePaxos component found");

        let mut proposal_futures = vec![];
        proposer.on_definition(|x| {
            for val in proposals {
                let (kprom, kfuture) = promise::<Value>();
                x.paxos.append(val).expect("Failed to append");
                x.decided_futures.push(Ask::new(kprom, ()));
                proposal_futures.push(kfuture);
            }
        });

        match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, timeout) {
            Ok(_) => {}
            Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
        }
    }

    pub fn reconfigure(
        &self,
        proposer: u64,
        new_configuration: ClusterConfig,
        metadata: Option<Vec<u8>>,
        timeout: Duration,
    ) {
        let proposer = self
            .nodes
            .get(&proposer)
            .expect("No SequencePaxos component found");

        let reconfig_future = proposer.on_definition(|x| {
            let (kprom, kfuture) = promise::<Value>();
            x.paxos
                .reconfigure(new_configuration, metadata)
                .expect("Failed to reconfigure");
            x.decided_futures.push(Ask::new(kprom, ()));
            kfuture
        });

        reconfig_future
            .wait_timeout(timeout)
            .expect("Failed to collect reconfiguration future");
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
    use omnipaxos::{
        ballot_leader_election::Ballot,
        messages::Message,
        util::{LogEntry, NodeId},
        OmniPaxos,
    };
    use std::collections::{HashMap, HashSet};

    const SNAPSHOTTED_DECIDE: Value = Value(0);

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosComponent {
        ctx: ComponentContext<Self>,
        #[allow(dead_code)]
        pid: NodeId,
        pub peers: HashMap<u64, ActorRef<Message<Value>>>,
        pub peer_disconnections: HashSet<u64>,
        paxos_timer: Option<ScheduledTimer>,
        tick_timer: Option<ScheduledTimer>,
        tick_timeout_ms: u64,
        pub paxos: OmniPaxos<Value, StorageType<Value>>,
        pub decided_futures: Vec<Ask<(), Value>>,
        pub election_futures: Vec<Ask<(), Ballot>>,
        current_leader_ballot: Ballot,
        decided_idx: u64,
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
                Duration::from_millis(self.tick_timeout_ms),
                Duration::from_millis(self.tick_timeout_ms),
                move |c, _| {
                    c.paxos.tick();
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
            paxos: OmniPaxos<Value, StorageType<Value>>,
            tick_timeout_ms: u64,
        ) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                pid,
                peers: HashMap::new(),
                peer_disconnections: HashSet::new(),
                paxos_timer: None,
                tick_timer: None,
                tick_timeout_ms,
                decided_idx: paxos.get_decided_idx(),
                paxos,
                decided_futures: vec![],
                election_futures: vec![],
                current_leader_ballot: Ballot::default(),
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

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<Value>>>) {
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
                            LogEntry::StopSign(ss, _is_decided) => self
                                .decided_futures
                                .pop()
                                .unwrap()
                                .reply(Value(ss.next_config.configuration_id as u64))
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
        type Message = Message<Value>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.paxos.handle_incoming(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialOrd, PartialEq, Serialize, Deserialize, Eq, Hash)]
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

#[cfg(not(feature = "unicache"))]
impl Entry for Value {
    type Snapshot = LatestValue;
}

#[cfg(feature = "unicache")]
impl Entry for Value {
    type Snapshot = LatestValue;
    type Encoded = u16;
    type Encodable = Self;
    type NotEncodable = ();
    type UniCache = LRUniCache<Self>;

    // fn pre_process(&self) -> omnipaxos::unicache::PreProcessedEntry<Self> {
    //     PreProcessedEntry {
    //         encodable: vec![Value(self.0)],
    //         not_encodable: vec![],
    //     }
    // }

    // fn recreate(item: omnipaxos::unicache::PreProcessedEntry<Self>) -> Self {
    //     *item.encodable.first().unwrap()
    // }
}

/// Create a temporary directory in /tmp/
pub fn create_temp_dir() -> String {
    let dir = TempDir::new().expect("Failed to create temporary directory");
    let dir_path = dir.path().to_path_buf();
    dir_path.to_string_lossy().to_string()
}

pub mod verification {
    use super::{LatestValue, Value};
    use omnipaxos::{
        storage::{Snapshot, StopSign},
        util::LogEntry,
    };

    /// Verify that the log matches the proposed values, Depending on
    /// the timing the log should match one of the following cases.
    /// * All entries are decided, verify the decided entries
    /// * Only a snapshot was taken, verify the snapshot
    /// * A snapshot was taken and entries decided on afterwards, verify both the snapshot and entries
    pub fn verify_log(read_log: Vec<LogEntry<Value>>, proposals: Vec<Value>) {
        let num_proposals = proposals.len() as u64;
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
            [] => assert!(
                proposals.len() == 0,
                "Log is empty but should be {:?}",
                proposals
            ),
            _ => panic!("Unexpected entries in the log: {:?} ", read_log),
        }
    }

    /// Verify that the log has a single snapshot of the latest entry.
    pub fn verify_snapshot(
        read_entries: &[LogEntry<Value>],
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
        read_entries: &[LogEntry<Value>],
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
    pub fn verify_stopsign(read_entries: &[LogEntry<Value>], exp_stopsign: &StopSign) {
        assert_eq!(
            read_entries.len(),
            1,
            "Expected StopSign, read: {:?}",
            read_entries
        );
        match read_entries.first().unwrap() {
            LogEntry::StopSign(ss, _is_decided) => {
                assert_eq!(ss, exp_stopsign);
            }
            e => {
                panic!("{}", format!("Not a StopSign: {:?}", e))
            }
        }
    }
}
