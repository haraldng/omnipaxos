# Project Info

While OmniPaxos is primarily being developed at the [KTH Royal Institute of Technology](https://www.kth.se/en) and [RISE Research Institutes of Sweden](https://www.ri.se/en) in Stockholm, Sweden, we do wish to thank all [contributors](https://github.com/haraldng/omnipaxos/graphs/contributors).

<!-- ## Releases

OmniPaxos releases are hosted on [crates.io](https://crates.io/crates/omnipaxos). -->

<!-- ## API Documentation

OmniPaxos API docs are hosted on [docs.rs](https://docs.rs/kompact/latest/kompact/). -->

## Sources & Issues

The sources for OmniPaxos can be found on [Github](https://github.com/haraldng/omnipaxos).

All issues and requests related to OmniPaxos should be posted there.

## Bleeding Edge

This tutorial is built off the `master` branch on GitHub and thus tends to be a bit ahead of what is available in a release.
If you would like to try out new features before they are released, you can add the following to your `Cargo.toml`:

```toml
omnipaxos = { git = "https://github.com/haraldng/omnipaxos", branch = "master" }
```

### Documentation

If you need the API docs for the latest master run the following at an appropriate location (e.g., outside another local git repository):

```bash
git checkout https://github.com/haraldng/omnipaxos
cd omnipaxos
cargo doc --open --no-deps
```
