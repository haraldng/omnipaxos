use self::{
    ble::{BallotLeaderComp, BallotLeaderElection},
    omnireplica::OmniPaxosReplica,
};
use kompact::prelude::*;
use omnipaxos::{
    leader_election::{
        ballot_leader_election::{
            messages::{BLEMessage, HeartbeatMsg},
            Ballot,
        },
        Leader,
    },
    messages::Message,
};
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
        ble_initial_election_factor: Option<u64>,
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
        let mut ble_refs: HashMap<u64, ActorRef<HeartbeatMsg>> = HashMap::new();
        let mut omni_refs: HashMap<u64, ActorRef<Message<Ballot>>> = HashMap::new();

        for pid in 1..=num_nodes as u64 {
            let mut peer_pids = all_pids.clone();
            peer_pids.retain(|i| i != &pid);
            // create components
            let (ble_comp, ble_reg_f) = system.create_and_register(|| {
                BallotLeaderComp::with(
                    pid,
                    peer_pids.clone(),
                    ble_hb_delay,
                    ble_initial_delay_factor,
                    ble_initial_leader,
                    ble_initial_election_factor,
                )
            });
            let (omni_replica, omni_reg_f) =
                system.create_and_register(|| OmniPaxosReplica::with(pid, peer_pids.clone()));
            biconnect_components::<BallotLeaderElection, _, _>(&ble_comp, &omni_replica)
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
    use omnipaxos::leader_election::{
        ballot_leader_election::{messages::*, *},
        *,
    };

    pub struct BallotLeaderElection;

    impl Port for BallotLeaderElection {
        type Indication = Leader<Ballot>;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElection>,
        pid: u64,
        peers: HashMap<u64, ActorRef<HeartbeatMsg>>,
        pub leader: Option<Ballot>,
        hb_delay: u64,
        timer: Option<ScheduledTimer>,
        // TODO add BLE field
    }

    impl BallotLeaderComp {
        pub fn with(
            pid: u64,
            peer_pids: Vec<u64>,
            hb_delay: u64,
            initial_delay_factor: Option<u64>,
            initial_leader: Option<Leader<Ballot>>,
            initial_election_factor: Option<u64>,
            // TODO add BLE
        ) -> BallotLeaderComp {
            BallotLeaderComp {
                ctx: ComponentContext::uninitialised(),
                ble_port: ProvidedPort::uninitialised(),
                pid,
                peers: HashMap::new(),
                leader: None,
                hb_delay,
                timer: None,
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<HeartbeatMsg>>) {
            self.peers = peers;
        }
    }

    impl ComponentLifecycle for BallotLeaderComp {
        fn on_start(&mut self) -> Handled {
            todo!("create timers for tick and outgoing")
        }

        fn on_kill(&mut self) -> Handled {
            if let Some(timer) = self.timer.take() {
                self.cancel_timer(timer);
            }
            Handled::Ok
        }
    }

    impl Provide<BallotLeaderElection> for BallotLeaderComp {
        fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> Handled {
            // ignore
            Handled::Ok
        }
    }

    impl Actor for BallotLeaderComp {
        type Message = HeartbeatMsg;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            todo!("handle incoming message")
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }
}

pub mod omnireplica {
    use super::{ble::BallotLeaderElection, *};
    use omnipaxos::{
        leader_election::{ballot_leader_election::Ballot, Leader},
        messages::Message,
    };
    use std::collections::HashMap;

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosReplica {
        ctx: ComponentContext<Self>,
        ble_port: RequiredPort<BallotLeaderElection>,
        pid: u64,
        peers: HashMap<u64, ActorRef<Message<Ballot>>>,
        timer: Option<ScheduledTimer>,
        // TODO add Paxos field
    }

    impl ComponentLifecycle for OmniPaxosReplica {
        fn on_start(&mut self) -> Handled {
            todo!("create timers for outgoing and decided")
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
            peer_pids: Vec<u64>,
            // TODO Paxos
        ) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                ble_port: RequiredPort::uninitialised(),
                pid,
                peers: HashMap::new(),
                timer: None,
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<u64, ActorRef<Message<Ballot>>>) {
            self.peers = peers;
        }

        pub fn propose(&mut self, data: Vec<u8>) {
            todo!("Propose client data to Paxos")
        }
    }

    impl Actor for OmniPaxosReplica {
        type Message = Message<Ballot>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            todo!("Handle incoming message")
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }

    impl Require<BallotLeaderElection> for OmniPaxosReplica {
        fn handle(&mut self, l: Leader<Ballot>) -> Handled {
            todo!("Handle new leader event in Paxos")
        }
    }
}
