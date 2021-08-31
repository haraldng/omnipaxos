# Components

The OmniPaxos library can be divided into two big components
[**OmniPaxos**](../omnipaxos/index.md) and [**Ballot Leader Election**](../ble/index.md) (*BLE*). By default OmniPaxos comes without a leader election alghorithm as it can be provided by the user. The library includes the BLE alghorithm as an already implemented alternative.

## OmniPaxos

An OmniPaxos replica maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
The user also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.

A OmniPaxos replica can be initialised by calling one the two constructors. The second constructor uses a hocon configuration which will be discussed in further details in [configuration](../omnipaxos/configuration.md).

```rust,edition2018,no_run,noplaypen
OmniPaxos::with(
	config_id: u32,
	pid: u64,
	peers: Vec<u64>,
	storage: Storage<R, S, P>,
	skip_prepare_use_leader: Option<Leader<R>>,
	logger: Option<Logger>,
	log_file_path: Option<&str>,
)

OmniPaxos::with_hocon(
	cfg: &Hocon,
	peers: Vec<u64>,
	storage: Storage<R, S, P>,
	skip_prepare_use_leader: Option<Leader<R>>,
	logger: Option<Logger>,
)
```

## Ballot Leader Election

The user is able to use an already implemented leader election alghoritm provided by the library. A BLE component can be initialised by calling one the two constructors. The second constructor uses a hocon configuration which will be discussed in further details in [configuration](../ble/configuration.md).

```rust,edition2018,no_run,noplaypen
BallotLeaderElection::with(
	peers: Vec<u64>,
	pid: u64,
	hb_delay: u64,
	increment_delay: u64,
	initial_leader: Option<Leader<Ballot>>,
	initial_delay_factor: Option<u64>,
	logger: Option<Logger>,
	log_file_path: Option<&str>,
)

BallotLeaderElection::with_hocon(
	cfg: &Hocon,
	peers: Vec<u64>,
	initial_leader: Option<Leader<Ballot>>,
	logger: Option<Logger>,
)
```
