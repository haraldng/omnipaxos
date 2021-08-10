use std::fmt::Debug;

/// Rounds in Omni-Paxos must be totally ordered.
pub trait Round: Clone + Debug + Ord + Default + Send + 'static {}

/// Leader event that indicates a leader has been elected. Should be created when the user-defined BLE algorithm
/// outputs a leader event. Should be then handled in Omni-Paxos by calling [`crate::paxos::Paxos::handle_leader()`].
#[derive(Copy, Clone, Debug)]
pub struct Leader<R>
where
    R: Round,
{
    /// The pid of the elected leader.
    pub pid: u64,
    /// The round in which `pid` is elected in.
    pub round: R,
}

impl<R> Leader<R>
where
    R: Round,
{
    /// Constructor for [`Leader`].
    pub fn with(pid: u64, round: R) -> Self {
        Leader { pid, round }
    }
}

/// Ballot Leader Election algorithm for electing new leaders
pub mod ballot_leader_election {
    use crate::leader_election::{Leader, Round};
    use crate::utils::hocon_kv::{
        HB_DELAY, INCREMENT_DELAY, INITIAL_DELAY_FACTOR, LOG_FILE_PATH, PID,
    };
    use crate::utils::logger::create_logger;
    use hocon::Hocon;
    use messages::{BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest};
    use slog::{debug, info, trace, warn, Logger};

    /// Used to define an epoch
    #[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq)]
    pub struct Ballot {
        /// Ballot number
        pub n: u32,
        /// The pid of the process
        pub pid: u64,
    }

    impl Ballot {
        /// Creates a new Ballot
        /// # Arguments
        /// * `n` - Ballot number.
        /// * `pid` -  Used as tiebreaker for total ordering of ballots.
        pub fn with(n: u32, pid: u64) -> Ballot {
            Ballot { n, pid }
        }
    }

    impl Round for Ballot {}

    /// A Ballot Leader Election component. Used in conjunction with Omni-Paxos handles the election of a leader for a group of omni-paxos replicas,
    /// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
    /// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
    pub struct BallotLeaderElection {
        /// Process identifier used to uniquely identify this instance.
        pid: u64,
        /// Vector that holds all the other replicas.
        peers: Vec<u64>,
        /// The current round of the heartbeat cycle.
        hb_round: u32,
        /// Vector which holds all the received ballots.
        ballots: Vec<(Ballot, bool)>,
        /// Holds the current ballot of this instance.
        current_ballot: Ballot, // (round, pid)
        /// States if the instance is a candidate to become a leader.
        majority_connected: bool,
        /// Current elected leader.
        leader: Option<Ballot>,
        /// Internal delay used for timeout.
        hb_current_delay: u64,
        /// Fixed delay of timeout. It is measured in ticks.
        hb_delay: u64,
        /// How long time is waited before timing out on a Heartbeat response and possibly resulting in a leader-change. Measured in number of times [`tick()`] is called.
        increment_delay: u64,
        /// The majority of replicas inside a cluster. It is measured in ticks.
        majority: usize,
        /// A factor used in the beginning for a shorter hb_delay.
        /// Used to faster elect a leader when starting up.
        /// If used, then hb_delay is set to hb_delay/initial_delay_factor until the first leader is elected.
        initial_delay_factor: Option<u64>,
        /// Internal timer which simulates the passage of time.
        ticks_elapsed: u64,
        /// Vector which holds all the outgoing messages of the BLE instance.
        outgoing: Vec<BLEMessage>,
        /// Logger used to output the status of the component.
        logger: Logger,
    }

    impl BallotLeaderElection {
        /// Construct a new BallotLeaderComponent
        /// # Arguments
        /// * `peers` - Vector that holds all the other replicas.
        /// * `pid` -  Process identifier used to uniquely identify this instance.
        /// * `hb_delay` -  A fixed delay that is added to the current_delay. It is measured in ticks.
        /// * `increment_delay` - A fixed delay that is added to the current_delay. It is measured in ticks.
        /// * `initial_leader` -  Initial leader which will be elected.
        /// * `initial_delay_factor` -  A factor used in the beginning for a shorter hb_delay.
        /// * `logger` - Used for logging events of Ballot Leader Election.
        /// * `log_file_path` - Path where the default logger logs events.
        pub fn with(
            peers: Vec<u64>,
            pid: u64,
            hb_delay: u64,
            increment_delay: u64,
            initial_leader: Option<Leader<Ballot>>,
            initial_delay_factor: Option<u64>,
            logger: Option<Logger>,
            log_file_path: Option<&str>,
        ) -> BallotLeaderElection {
            let n = &peers.len() + 1;
            let (leader, initial_ballot) = match initial_leader {
                Some(l) => {
                    let leader_ballot = Ballot::with(l.round.n, l.pid);
                    let initial_ballot = if l.pid == pid {
                        leader_ballot
                    } else {
                        Ballot::with(0, pid)
                    };
                    (Some(leader_ballot), initial_ballot)
                }
                None => {
                    let initial_ballot = Ballot::with(0, pid);
                    (None, initial_ballot)
                }
            };

            let l = logger.unwrap_or_else(|| {
                create_logger(log_file_path.unwrap_or(format!("logs/ble_{}.log", pid).as_str()))
            });

            info!(l, "Ballot Leader Election component pid: {} created!", pid);

            BallotLeaderElection {
                pid,
                majority: n / 2 + 1, // +1 because peers is exclusive ourselves
                peers,
                hb_round: 0,
                ballots: Vec::with_capacity(n),
                current_ballot: initial_ballot,
                majority_connected: true,
                leader,
                hb_current_delay: hb_delay,
                hb_delay,
                increment_delay,
                initial_delay_factor,
                ticks_elapsed: 0,
                outgoing: vec![],
                logger: l,
            }
        }

        /// Construct a new BallotLeaderComponent
        /// # Arguments
        /// * `cfg` - Hocon configuration used for ble replica.
        /// * `peers` - Vector that holds all the other replicas.
        /// * `initial_leader` -  Initial leader which will be elected.
        /// * `logger` - Used for logging events of Ballot Leader Election.
        pub fn with_hocon(
            &self,
            cfg: &Hocon,
            peers: Vec<u64>,
            initial_leader: Option<Leader<Ballot>>,
            logger: Option<Logger>,
        ) -> BallotLeaderElection {
            BallotLeaderElection::with(
                peers,
                cfg[PID].as_i64().expect("Failed to load PID") as u64,
                cfg[HB_DELAY]
                    .as_i64()
                    .expect("Failed to load heartbeat delay") as u64,
                cfg[INCREMENT_DELAY]
                    .as_i64()
                    .expect("Failed to load increment delay") as u64,
                initial_leader,
                Option::from(
                    cfg[INITIAL_DELAY_FACTOR]
                        .as_i64()
                        .expect("Failed to load initial delay factor") as u64,
                ),
                logger,
                Option::from(
                    cfg[LOG_FILE_PATH]
                        .as_string()
                        .expect("Failed to load log file path")
                        .as_str(),
                ),
            )
        }

        /// Returns the outgoing vector
        pub fn get_outgoing_msgs(&mut self) -> Vec<BLEMessage> {
            std::mem::take(&mut self.outgoing)
        }

        /// Returns the currently elected leader.
        pub fn get_leader(&self) -> Option<Leader<Ballot>> {
            self.leader
                .map(|ballot: Ballot| -> Leader<Ballot> { Leader::with(ballot.pid, ballot) })
        }

        /// Tick is run by all servers to simulate the passage of time
        /// If one wishes to have hb_delay of 500ms, one can set a periodic timer of 100ms to call tick(). After 5 calls to this function, the timeout will occur.
        /// Returns an Option with the elected leader otherwise None
        pub fn tick(&mut self) -> Option<Leader<Ballot>> {
            self.ticks_elapsed += 1;

            if self.ticks_elapsed >= self.hb_current_delay {
                self.ticks_elapsed = 0;
                self.hb_timeout()
            } else {
                None
            }
        }

        /// Handle an incoming message.
        /// # Arguments
        /// * `m` - the message to be handled.
        pub fn handle(&mut self, m: BLEMessage) {
            match m.msg {
                HeartbeatMsg::Request(req) => self.handle_request(m.from, req),
                HeartbeatMsg::Reply(rep) => self.handle_reply(rep),
            }
        }

        /// Sets initial state after creation. Should only be used before being started.
        /// # Arguments
        /// * `l` - Initial leader.
        pub fn set_initial_leader(&mut self, l: Leader<Ballot>) {
            assert!(self.leader.is_none());
            let leader_ballot = Ballot::with(l.round.n, l.pid);
            self.leader = Some(leader_ballot);
            if l.pid == self.pid {
                self.current_ballot = leader_ballot;
                self.majority_connected = true;
            } else {
                self.current_ballot = Ballot::with(0, self.pid);
                self.majority_connected = false;
            };
        }

        fn check_leader(&mut self) -> Option<Leader<Ballot>> {
            let ballots = std::mem::take(&mut self.ballots);
            let top_ballot = ballots
                .into_iter()
                .filter_map(
                    |(ballot, candidate)| {
                        if candidate {
                            Some(ballot)
                        } else {
                            None
                        }
                    },
                )
                .max()
                .unwrap_or_default();

            if top_ballot < self.leader.unwrap_or_default() {
                // did not get HB from leader
                self.current_ballot.n = self.leader.unwrap_or_default().n + 1;
                self.leader = None;
                self.majority_connected = true;

                None
            } else if self.leader != Some(top_ballot) {
                // got a new leader with greater ballot
                self.leader = Some(top_ballot);
                let top_pid = top_ballot.pid;
                if self.pid == top_pid {
                    self.majority_connected = true;
                } else {
                    self.majority_connected = false;
                }

                debug!(
                    self.logger,
                    "New Leader elected, pid: {}, ballot: {:?}", top_pid, top_ballot
                );
                Some(Leader::with(top_pid, top_ballot))
            } else {
                None
            }
        }

        /// Initiates a new heartbeat round.
        pub fn new_hb_round(&mut self) {
            self.hb_round += 1;

            trace!(
                self.logger,
                "Initiate new heartbeat round: {}",
                self.hb_round
            );

            self.hb_current_delay = if let Some(initial_delay) = self.initial_delay_factor {
                debug!(self.logger, "Using initial heartbeat delay");
                // use short timeout if still no first leader
                let delay = self.hb_delay / initial_delay;
                self.initial_delay_factor = None;
                delay
            } else {
                self.hb_delay
            };

            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.hb_round);

                self.outgoing.push(BLEMessage::with(
                    *peer,
                    self.pid,
                    HeartbeatMsg::Request(hb_request),
                ));
            }
        }

        fn hb_timeout(&mut self) -> Option<Leader<Ballot>> {
            trace!(self.logger, "Heartbeat timeout round: {}", self.hb_round);

            let result: Option<Leader<Ballot>> = if self.ballots.len() + 1 >= self.majority {
                debug!(
                    self.logger,
                    "Received a majority of heartbeats {:?}", self.ballots
                );
                self.ballots
                    .push((self.current_ballot, self.majority_connected));
                self.check_leader()
            } else {
                warn!(
                    self.logger,
                    "Did not receive a majority of heartbeats {:?}", self.ballots
                );
                self.ballots.clear();
                self.majority_connected = false;
                None
            };
            self.new_hb_round();

            result
        }

        fn handle_request(&mut self, from: u64, req: HeartbeatRequest) {
            trace!(self.logger, "Heartbeat request from {}", from);

            let hb_reply =
                HeartbeatReply::with(req.round, self.current_ballot, self.majority_connected);

            self.outgoing.push(BLEMessage::with(
                self.pid,
                from,
                HeartbeatMsg::Reply(hb_reply),
            ));
        }

        fn handle_reply(&mut self, rep: HeartbeatReply) {
            trace!(self.logger, "Heartbeat reply {:?}", rep.ballot);

            if rep.round == self.hb_round {
                self.ballots.push((rep.ballot, rep.majority_connected));
            } else {
                warn!(
                    self.logger,
                    "Got late response, round {}, current delay {}, ballot {:?}",
                    self.hb_round,
                    self.hb_current_delay,
                    rep.ballot
                );
                self.hb_current_delay += self.increment_delay;
            }
        }
    }

    /// The different messages BLE uses to communicate with other replicas.
    pub mod messages {
        use crate::leader_election::ballot_leader_election::Ballot;

        /// An enum for all the different BLE message types.
        #[allow(missing_docs)]
        #[derive(Clone, Debug)]
        pub enum HeartbeatMsg {
            Request(HeartbeatRequest),
            Reply(HeartbeatReply),
        }

        /// Requests a reply from all the other replicas.
        #[derive(Clone, Debug)]
        pub struct HeartbeatRequest {
            /// Number of the current round.
            pub round: u32,
        }

        impl HeartbeatRequest {
            /// Creates a new HeartbeatRequest
            /// # Arguments
            /// * `round` - number of the current round.
            pub fn with(round: u32) -> HeartbeatRequest {
                HeartbeatRequest { round }
            }
        }

        /// Replies
        #[derive(Clone, Debug)]
        pub struct HeartbeatReply {
            /// Number of the current round.
            pub round: u32,
            /// Ballot of a replica.
            pub ballot: Ballot,
            /// States if the replica is a candidate to become a leader.
            pub majority_connected: bool,
        }

        impl HeartbeatReply {
            /// Creates a new HeartbeatRequest
            /// # Arguments
            /// * `round` - Number of the current round.
            /// * `ballot` -  Ballot of a replica.
            /// * `majority_connected` -  States if the replica is majority_connected to become a leader.
            pub fn with(round: u32, ballot: Ballot, majority_connected: bool) -> HeartbeatReply {
                HeartbeatReply {
                    round,
                    ballot,
                    majority_connected,
                }
            }
        }

        /// A struct for a Paxos message that also includes sender and receiver.
        #[derive(Clone, Debug)]
        pub struct BLEMessage {
            /// Sender of `msg`.
            pub from: u64,
            /// Receiver of `msg`.
            pub to: u64,
            /// The message content.
            pub msg: HeartbeatMsg,
        }

        impl BLEMessage {
            /// Creates a BLE message.
            /// # Arguments
            /// * `from` - Sender of `msg`.
            /// * `to` -  Receiver of `msg`.
            /// * `msg` -  The message content.
            pub fn with(from: u64, to: u64, msg: HeartbeatMsg) -> Self {
                BLEMessage { from, to, msg }
            }
        }
    }
}
