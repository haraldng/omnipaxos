Thanks for showing interest in improving OmniPaxos! All contributions (small or big) are welcomed and highly appreciated.

## Issues
For bug reports and feature requests, please create an issue following the appropriate template. Questions and other general discussions related to OmniPaxos should be posted as a [GitHub discussion](https://github.com/haraldng/omnipaxos/discussions).

## Pull Requests
Pull requests ise the way to make concrete changes to the code, documentation, and other things related to the OmniPaxos repository. Before implementing a new feature or a big change, please first create an issue that describes the motivation and goal of it. This enables others to give feedback and guidance that will make the PR more likely to get merged.

### Commands
Before making a PR, please make sure that your branch passes the local checker:

```console
./check.sh
```

This runs a check for formatting and all tests using the default features. Since OmniPaxos has quite a few [feature flags](../docs/omnipaxos/features), please make sure that you run tests with the appropriate features:

```
cargo test --features <your_features>
```

### Documentation
Any feature that is public and exposed to users must be documented. The documentation should describe the feature and provide examples of how it can be used.

The documentation lives in `docs` and is automatically rendered to the [OmniPaxos website](https://omnipaxos.com/docs/) when a new commit is pushed to the master branch.

The structure of the docs are defined in `structure.yaml` and can be modified to add new pages or re-order pages. The `tags` field is optional but indexes the page so that it can be found via the search box on the website.

```yaml
upper-level-menu:
    doc-title:
      path: "Relative-path-of-doc-file"
      tags: [ "tag1", "tag2" ]	    # optional field
```

#### Cross-references
To make a cross-reference in the documentation, please make sure to remove the ``.md`` suffix from the relative path. For instance, to make a cross-reference from `docs/omnipaxos/compaction.md` to `docs/omnipaxos/logging.md`:

```
// in docs/omnipaxos/compaction.md
This is a [cross-reference](../logging) from Compaction to Logging.
```

Furthermore, to make a cross-reference to a parent page i.e., an ``index.md``, please use the parent name instead:
```
// in docs/omnipaxos/compaction.md
This is a [cross-reference](../omnipaxos) from Compaction to index.md.
```

