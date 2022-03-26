/// Ballot Leader Election algorithm for electing new leaders
use crate::{
    util::defaults::{BLE_BUFFER_SIZE as DEFAULT_BUFFER_SIZE, *},
    utils::{
        hocon_kv::{BLE_BUFFER_SIZE, HB_DELAY, INITIAL_DELAY, LOG_FILE_PATH, PEERS, PID, PRIORITY},
        logger::create_logger,
    },
};
use hocon::Hocon;
use messages::{BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest};
use slog::{debug, info, trace, warn, Logger};
use serde::{Deserialize, Serialize};

/// Used to define an epoch
#[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
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
    logger: Logger,
}

impl BallotLeaderElection {
    /// Construct a new BallotLeaderElection node
    pub fn with(config: BLEConfig) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let n = &peers.len() + 1;
        let initial_ballot = match config.initial_leader {
            //config.initial_leader is not None
            //将这个leader_ballot设为我们自己的ballot
            Some(leader_ballot) if leader_ballot.pid == pid => leader_ballot,
            //config.initial_leader None
            //创建一个我们自己的ballot
            _ => Ballot::with(0, config.priority.unwrap_or_default(), pid),
        };
        let path = config.logger_file_path;
        let l = config.logger.unwrap_or_else(|| {
            let s = path.unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
            //这里就是创建一个logger,之后可以往terminal或者log文件中输出东西！
            create_logger(s.as_str())
        });
        let hb_delay = config.hb_delay;
        info!(l, "Ballot Leader Election component pid: {} created!", pid);
        BallotLeaderElection {
            pid,
            majority: n / 2 + 1, // +1 because peers is exclusive ourselves
            peers,
            hb_round: 0,
            ballots: Vec::with_capacity(n),
            //还记得之前我们处理的initial_ballot吗？
            current_ballot: initial_ballot,
            majority_connected: true,
            leader: config.initial_leader,
            hb_current_delay: hb_delay,
            hb_delay,
            initial_delay: config.initial_delay,
            ticks_elapsed: 0,
            outgoing: vec![],
            logger: l,
        }
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
        // println!("from : {:?}",m.from);
        // println!("to : {:?}",m.to);
        // println!("msg : {:?}",m.msg);
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
            //使自己的n比原来leader的n大1,使自己能够参与竞选
            self.current_ballot.n = self.leader.unwrap_or_default().n + 1;
            //没有找出leader
            self.leader = None;
            //返回None
            None
        } else if self.leader != Some(top_ballot) {
            // got a new leader with greater ballot
            self.leader = Some(top_ballot);
            self.initial_delay = None;
            info!(
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
    //心跳
    fn hb_timeout(&mut self) -> Option<Ballot> {
        //println!("Heartbeat timeout round: {}", self.hb_round);
        trace!(self.logger, "Heartbeat timeout round: {}", self.hb_round);
        //如果现在还能数量收到大于majority的心跳包
        let result: Option<Ballot> = if self.ballots.len() + 1 >= self.majority {
            debug!(
                self.logger,
                "Received a majority of heartbeats {:?}", self.ballots
            );
            //进行check_leader前把自己也放进去
            self.ballots
                .push((self.current_ballot, self.majority_connected));
            self.check_leader()
        //如果不能收到数量大于majority的心跳包了
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
        //println!("Heartbeat request from {}", from);
        trace!(self.logger, "Heartbeat request from {}", from);
        let hb_reply =
            HeartbeatReply::with(req.round, self.current_ballot, self.majority_connected);
        
    
        //println!("HeartbeatReply :  {:?}", hb_reply);   
        
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
            println!("rep.round: {}", rep.round);
            println!("self.hb_round: {}", self.hb_round);
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
    use serde::{Deserialize, Serialize};

    /// An enum for all the different BLE message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum HeartbeatMsg {
        ///HeartbeatRequest类型的
        Request(HeartbeatRequest),
        Reply(HeartbeatReply),
    }

    /// Requests a reply from all the other replicas.
    #[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[derive(Clone, Debug, Serialize, Deserialize)]
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
    logger: Option<Logger>,
    logger_file_path: Option<String>,
    buffer_size: usize,
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

    pub fn set_logger(&mut self, l: Logger) {
        self.logger = Some(l);
    }

    pub fn get_logger(&self) -> Option<&Logger> {
        self.logger.as_ref()
    }

    pub fn set_logger_file_path(&mut self, s: String) {
        self.logger_file_path = Some(s);
    }

    pub fn get_logger_file_path(&self) -> Option<&String> {
        self.logger_file_path.as_ref()
    }

    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

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
            logger: None,
            logger_file_path: None,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}
