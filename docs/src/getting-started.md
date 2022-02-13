# Getting Started
We aim to make a release on crates soon, but for now, the easiest way to use OmniPaxos is to have it as a GitHub dependency.
<!-- ## Setting up Rust
It is recommended to run OmniPaxos on a *nightly* version of the Rust toolchain.

We recommend using the [rustup](https://rustup.rs/) tool to easily install the latest nightly version of rust and keep it updated. Instructions should be on the screen once rustup is downloaded.

> **Using the nightly toolchain:** Rustup can be configured to default to the nightly toolchain by running `rustup default nightly`. 

## Cargo

Add OmniPaxos to your cargo project as a dependency:

```toml
[dependencies]
omnipaxos = "LATEST_VERSION"
``` 
The latest version can be found on [crates.io](https://crates.io/crates/omnipaxos). -->

### Github master

<!--You can also point cargo to the latest [Github](https://github.com/haraldng/omnipaxos) master version, instead of a release.  -->
To do so add the following to your Cargo.toml:

```toml
[dependencies]
omnipaxos_core = { git = "https://github.com/haraldng/omnipaxos" }
```

or 

```toml
[dependencies]
omnipaxos_runtime = { git = "https://github.com/haraldng/omnipaxos" }
```

Depending on if you want to use just the core structs or the runtime (see [here](./foreword.md)). For the first chapters of this tutorial we will use ``omnipaxos_core``.