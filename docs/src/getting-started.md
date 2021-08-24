# Getting Started

## Setting up Rust
It is recommmended to run OmniPaxos on a *nightly* version of the rust toolchain.

We recommend using the [rustup](https://rustup.rs/) tool to easily install the latest nightly version of rust and keep it updated. Instructions should be on screen once rustup is downloaded.

> **Using the nightly toolchain:** Rustup can be configured to default to the nightly toolchain by running `rustup default nightly`.

## Cargo

Add OmniPaxos to your cargo project as a dependency:

```toml
[dependencies]
omnipaxos = "LATEST_VERSION"
```
The latest version can be found on [crates.io](https://crates.io/crates/omnipaxos).

### Github master

You can also point cargo to the latest [Github](https://github.com/haraldng/omnipaxos) master version, instead of a release.
To do so add the following to your Cargo.toml instead:

```toml
[dependencies]
omnipaxos = { git = "https://github.com/haraldng/omnipaxos" }
```

<!-- ## Hello World

With the above, you are set to run the simplest of OmniPaxos projects, the venerable "Hello World".

Create a new executable file, such as `main.rs`, and write a very simple component that just logs "Hello World" at the `info` level when it's started and ignores all other messages and events: 

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../examples/src/bin/helloworld.rs:component}}
```

In order to start our component, we need a Kompact system, which we will create from a default configuration.
And then we just wait for the component to do its work and shut the system down again:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../examples/src/bin/helloworld.rs:main}}
```

The `await_termination()` call blocks the main-thread while the Kompact system is operating on its own thread pool. We will get into more details on scheduling and thread pools a bit later in this tutorial. For now it is sufficient to know, that once the Kompact system has been shut down by our `HelloWorldComponent` using `shutdown_async()`, the main-thread will eventually continue.

We can run this code, depending on how you set up your project, with:
```bash
cargo run --release
```
This will give us something like the following output:
```bash
lkroll $ cargo run --release --bin helloworld
    Finished release [optimized] target(s) in 0.09s
     Running `/Users/lkroll/Programming/Kompics/kompact/target/release/helloworld`
Jul 07 16:28:45.870 INFO Hello World!, ctype: HelloWorldComponent, cid: 804ed483-54d5-41ab-ad8f-145f90bc7b45, system: kompact-runtime-1, location: docs/examples/src/bin/helloworld.rs:17
```

We can see the "Hello World" being logged, alongside a bunch of other contextual information that is automatically inserted by the runtime, such as the type name of the component doing the logging (`ctype`), the unique id of the component (`cid`) which differentiates from other instances of the same type, the name of the Kompact `system`, as well as the concrete location in the file where the logging statement occurs.

If we run in debug mode, instead of release, using the simple `cargo run` we get a lot of additional output at the `debug` level, concerning system and component lifecycle – more on that later.

> **Note:** If you have checked out the [examples folder](https://github.com/kompics/kompact/tree/master/docs/examples) and are trying to run from there, you need to specify the concrete binary with:
> ```bash
> cargo run --release --bin helloworld
> ``` -->
