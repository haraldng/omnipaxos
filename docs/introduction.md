
The OmniPaxos library is mainly driven by the [**OmniPaxos**](omnipaxos/index.md) struct.  It is a plain Rust struct and the user therefore needs to provide a network implementation themselves to actually send and receive messages. In this tutorial we will show how a user should interact with this struct in order to implement a strongly consistent, replicated log. This tutorial will focus on how to use the library and showcase its features. 
<!-- For the properties and advantages of OmniPaxos in comparison to other similar protocols, we refer to the Omni-Paxos paper. -->

## Bleeding Edge
This tutorial is built off the `master` branch on GitHub and thus tends to be a bit ahead of what is available in a release.
If you would like to try out new features before they are released, you can add the following to your `Cargo.toml`:

```toml
omnipaxos = { git = "https://github.com/haraldng/omnipaxos", branch = "master" }
```

If you need the API docs for the latest master run the following at an appropriate location (e.g., outside another local git repository):

```bash
git clone https://github.com/haraldng/omnipaxos
cd omnipaxos
cargo doc --open --no-deps
```