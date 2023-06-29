# Crates Release Checklist
1. Update the version number for the crates badge in `README.md`
2. Update version number in `Cargo.toml` as required for each of the crates: `omnipaxos_macros`, `omnipaxos`, and `omnipaxos_storage` 
3. Make sure that `./check.sh` passes
4. Run `cargo publish --dry-run` on each of the crates.
5. The crates have inter-dependencies, so to publish them, we run `cargo publish` in the following order: 
   1. `omnipaxos_macros`
   2. `omnipaxos`
   3. `omnipaxos_storage`