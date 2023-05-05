## Cargo
Add OmniPaxos to your cargo project as a dependency:

```toml
[dependencies]
omnipaxos = "LATEST_VERSION"
``` 
The latest version can be found on [crates.io](https://crates.io/crates/omnipaxos).

### Github master
You can also point cargo to the latest [Github](https://github.com/haraldng/omnipaxos) master version, instead of a release.
To do so add the following to your Cargo.toml:

```toml
[dependencies]
omnipaxos = { git = "https://github.com/haraldng/omnipaxos" }
```

In ``omnipaxos/examples/kv_store``, we show a minimal example of how to use OmniPaxos to replicate KV operations using tokio.

## Documentation
Apart from this tutorial, the API documentation can be found on [https://docs.rs/omnipaxos](https://docs.rs/omnipaxos).