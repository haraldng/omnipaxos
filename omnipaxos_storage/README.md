# OmniPaxos Storage
Out-of-the-box storage implementations for [OmniPaxos](https://omnipaxos.com/).

## Usage

```toml
[dependencies]
omnipaxos = "0.2.3"
omnipaxos_storage = "0.2.3"
```

Enable RocksDB-backed persistence with the `persistent_storage` feature:

```toml
omnipaxos_storage = { version = "0.2.3", features = ["persistent_storage"] }
```
