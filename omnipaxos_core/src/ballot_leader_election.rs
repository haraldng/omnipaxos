/// Ballot Leader Election algorithm for electing new leaders
use crate::util::defaults::{BLE_BUFFER_SIZE as DEFAULT_BUFFER_SIZE, *};
#[cfg(feature = "hocon_config")]
use crate::utils::hocon_kv::{
    BLE_BUFFER_SIZE, HB_DELAY, INITIAL_DELAY, LOG_FILE_PATH, PEERS, PID, PRIORITY,
};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
#[cfg(feature = "hocon_config")]
use hocon::Hocon;
use messages::{BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};

/// Used to define an epoch
#[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq)]
pub struct Ballot {
    /// Ballot number
    pub n: u32,
    /// Custom priority parameter
    pub priority: u64,
    /// The pid of the process
    pub pid: u64,
}

impl Ballot {
    /// Creates a new Ballot
    /// # Arguments
    /// * `n` - Ballot number.
    /// * `priority` - Custom priority parameter.
    /// * `pid` -  Used as tiebreaker for total ordering of ballots.
    pub fn with(n: u32, priority: u64, pid: u64) -> Ballot {
        Ballot { n, priority, pid }
    }
}

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
    /// How long time is waited before timing out on a Heartbeat response and possibly resulting in a leader-change. Measured in number of times [`tick()`] is called.
    hb_delay: u64,
    /// The majority of replicas inside a cluster. It is measured in ticks.
    majority: usize,
    /// A factor used in the beginning for a shorter hb_delay.
    /// Used to faster elect a leader when starting up.
    /// If used, then hb_delay is set to hb_delay/initial_delay_factor until the first leader is elected.
    initial_delay: Option<u64>,
    /// Internal timer which simulates the passage of time.
    ticks_elapsed: u64,
    /// Vector which holds all the outgoing messages of the BLE instance.
    outgoing: Vec<BLEMessage>,
    /// Logger used to output the status of the component.
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl BallotLeaderElection {
    /// Construct a new BallotLeaderElection node
    pub fn with(config: BLEConfig) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let n = &peers.len() + 1;
        let initial_ballot = match &config.initial_leader {
            Some(leader_ballot) if leader_ballot.pid == pid => *leader_ballot,
            _ => Ballot::with(0, config.priority.unwrap_or_default(), pid),
        };
        let hb_delay = config.hb_delay;
        let ble = BallotLeaderElection {
            pid,
            majority: n / 2 + 1, // +1 because peers is exclusive ourselves
            peers,
            hb_round: 0,
            ballots: Vec::with_capacity(n),
            current_ballot: initial_ballot,
            majority_connected: true,
            leader: config.initial_leader,
            hb_current_delay: hb_delay,
            hb_delay,
            initial_delay: config.initial_delay,
            ticks_elapsed: 0,
            outgoing: vec![],
            #[cfg(feature = "logging")]
            logger: {
                let path = config.logger_file_path;
                config.logger.unwrap_or_else(|| {
                    let s = path.unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                })
            },
        };
        #[cfg(feature = "logging")]
        {
            info!(
                ble.logger,
                "Ballot Leader Election component pid: {} created!", pid
            );
        }
        ble
    }

    /// Update the custom priority used in the Ballot for this server.
    pub fn set_priority(&mut self, p: u64) {
        self.current_ballot.priority = p;
    }

    /// Returns outgoing messages
    pub fn get_outgoing_msgs(&mut self) -> Vec<BLEMessage> {
        std::mem::take(&mut self.outgoing)
    }

    /// Returns the currently elected leader.
    pub fn get_leader(&self) -> Option<Ballot> {
        self.leader
    }

    /// Tick is run by all servers to simulate the passage of time
    /// If one wishes to have hb_delay of 500ms, one can set a periodic timer of 100ms to call tick(). After 5 calls to this function, the timeout will occur.
    /// Returns an Option with the elected leader otherwise None
    pub fn tick(&mut self) -> Option<Ballot> {
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

    /// Sets initial state after creation. *Must only be used before being started*.
    /// # Arguments
    /// * `leader_ballot` - Initial leader.
    pub fn set_initial_leader(&mut self, leader_ballot: Ballot) {
        assert!(self.leader.is_none());
        if leader_ballot.pid == self.pid {
            self.current_ballot = leader_ballot;
            self.majority_connected = true;
        }
        self.leader = Some(leader_ballot);
    }

    fn check_leader(&mut self) -> Option<Ballot> {
        self.majority_connected = true;
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
            None
        } else if self.leader != Some(top_ballot) {
            // got a new leader with greater ballot
            self.leader = Some(top_ballot);
            self.initial_delay = None;
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "BLE {}, New Leader elected: {:?}", self.pid, top_ballot
            );
            Some(top_ballot)
        } else {
            None
        }
    }

    /// Initiates a new heartbeat round.
    pub fn new_hb_round(&mut self) {
        self.hb_round += 1;
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Initiate new heartbeat round: {}",
            self.hb_round
        );

        self.hb_current_delay = self.initial_delay.unwrap_or(self.hb_delay);

        for peer in &self.peers {
            let hb_request = HeartbeatRequest::with(self.hb_round);

            self.outgoing.push(BLEMessage::with(
                self.pid,
                *peer,
                HeartbeatMsg::Request(hb_request),
            ));
        }
    }

    fn hb_timeout(&mut self) -> Option<Ballot> {
        let result: Option<Ballot> = if self.ballots.len() + 1 >= self.majority {
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "Received a majority of heartbeats, round: {}, {:?}", self.hb_round, self.ballots
            );
            self.ballots
                .push((self.current_ballot, self.majority_connected));
            self.check_leader()
        } else {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Did not receive a majority of heartbeats, round: {}, {:?}",
                self.hb_round,
                self.ballots
            );
            self.ballots.clear();
            self.majority_connected = false;
            None
        };
        self.new_hb_round();
        result
    }

    fn handle_request(&mut self, from: u64, req: HeartbeatRequest) {
        let hb_reply =
            HeartbeatReply::with(req.round, self.current_ballot, self.majority_connected);

        self.outgoing.push(BLEMessage::with(
            self.pid,
            from,
            HeartbeatMsg::Reply(hb_reply),
        ));
    }

    fn handle_reply(&mut self, rep: HeartbeatReply) {
        if rep.round == self.hb_round {
            self.ballots.push((rep.ballot, rep.majority_connected));
        } else {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Got late response, round {}, current delay {}, ballot {:?}",
                self.hb_round,
                self.hb_current_delay,
                rep.ballot
            );
        }
    }
}

/// The different messages BLE uses to communicate with other replicas.
pub mod messages {
    use crate::ballot_leader_election::Ballot;

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

/// Configuration for `BallotLeaderElection`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other replicas in the configuration.
/// * `priority`: Set custom priority for this node to be elected as the leader.
/// * `hb_delay`: Timeout for waiting on heartbeat messages. It is measured in number of ticks.
/// * `initial_leader`: The initial leader of the cluster.
/// * `initial_timeout`: Optional initial timeout that can be used to elect a leader faster initially.
/// * `logger`: Custom logger for logging events of Ballot Leader Election.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `buffer_size`: The buffer size for outgoing messages.
#[derive(Clone, Debug)]
pub struct BLEConfig {
    pid: u64,
    peers: Vec<u64>,
    priority: Option<u64>,
    hb_delay: u64,
    initial_leader: Option<Ballot>,
    initial_delay: Option<u64>,
    buffer_size: usize,
    #[cfg(feature = "logging")]
    logger: Option<Logger>,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
}

#[allow(missing_docs)]
impl BLEConfig {
    pub fn set_pid(&mut self, pid: u64) {
        self.pid = pid;
    }

    pub fn get_pid(&self) -> u64 {
        self.pid
    }

    pub fn set_peers(&mut self, peers: Vec<u64>) {
        self.peers = peers;
    }

    pub fn get_peers(&self) -> &[u64] {
        self.peers.as_slice()
    }

    pub fn set_priority(&mut self, priority: u64) {
        self.priority = Some(priority);
    }

    pub fn get_priority(&self) -> Option<u64> {
        self.priority
    }

    pub fn set_hb_delay(&mut self, hb_delay: u64) {
        self.hb_delay = hb_delay;
    }

    pub fn get_hb_delay(&self) -> u64 {
        self.hb_delay
    }

    pub fn set_initial_leader(&mut self, b: Ballot) {
        self.initial_leader = Some(b);
    }

    pub fn get_initial_leader(&self) -> Option<Ballot> {
        self.initial_leader
    }

    pub fn set_initial_delay(&mut self, initial_delay: u64) {
        self.initial_delay = Some(initial_delay);
    }

    pub fn get_initial_delay(&self) -> Option<u64> {
        self.initial_delay
    }

    #[cfg(feature = "logging")]
    pub fn set_logger(&mut self, l: Logger) {
        self.logger = Some(l);
    }

    #[cfg(feature = "logging")]
    pub fn get_logger(&self) -> Option<&Logger> {
        self.logger.as_ref()
    }

    #[cfg(feature = "logging")]
    pub fn set_logger_file_path(&mut self, s: String) {
        self.logger_file_path = Some(s);
    }

    #[cfg(feature = "logging")]
    pub fn get_logger_file_path(&self) -> Option<&String> {
        self.logger_file_path.as_ref()
    }

    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    #[cfg(feature = "hocon_config")]
    pub fn with_hocon(h: &Hocon) -> Self {
        let mut config = Self::default();
        config.set_pid(h[PID].as_i64().expect("Failed to load PID") as u64);
        match &h[PEERS] {
            Hocon::Array(v) => {
                let peers = v
                    .iter()
                    .map(|x| x.as_i64().expect("Failed to load pid in Hocon array") as u64)
                    .collect();
                config.set_peers(peers);
            }
            _ => {
                unimplemented!("Peers in Hocon should be parsed as array!")
            }
        }
        #[cfg(feature = "logging")]
        if let Some(p) = h[LOG_FILE_PATH].as_string() {
            config.set_logger_file_path(p);
        }
        if let Some(p) = h[PRIORITY].as_i64().map(|p| p as u64) {
            config.set_priority(p);
        }
        if let Some(d) = h[INITIAL_DELAY].as_i64().map(|i| i as u64) {
            config.set_initial_delay(d);
        }
        if let Some(b) = h[BLE_BUFFER_SIZE].as_i64() {
            config.set_buffer_size(b as usize);
        }
        config.set_hb_delay(
            h[HB_DELAY]
                .as_i64()
                .expect("Failed to load heartbeat delay") as u64,
        );
        config
    }

    pub fn build(self) -> BallotLeaderElection {
        assert_ne!(self.pid, 0, "Pid cannot be 0");
        assert!(!self.peers.is_empty(), "Peers cannot be empty");
        assert!(
            !self.peers.contains(&self.pid),
            "Peers should not include self pid"
        );
        assert!(self.buffer_size > 0, "Buffer size must be greater than 0");
        assert!(self.hb_delay > 0, "hb_delay must be greater than 0");
        if let Some(x) = self.initial_leader {
            assert_ne!(x.pid, 0, "Initial leader cannot be 0")
        };
        BallotLeaderElection::with(self)
    }
}

impl Default for BLEConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            peers: vec![],
            priority: None,
            hb_delay: HB_TIMEOUT,
            initial_leader: None,
            initial_delay: None,
            buffer_size: DEFAULT_BUFFER_SIZE,
            #[cfg(feature = "logging")]
            logger: None,
            #[cfg(feature = "logging")]
            logger_file_path: None,
        }
    }
}
