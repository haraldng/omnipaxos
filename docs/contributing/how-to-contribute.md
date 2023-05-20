Contribute to code of OmniPaxos, improve documentation, and help others.

## Improve documentation

> The documentation lives in `docs` and rendered to website [Docs - OmniPaxos](https://omnipaxos.com/docs/).

- Follow the structure definition in the `docs/stucture`:

  ```yaml
  upper-level-menu:
      doc-title:
      	path: "Relative-path-of-doc-file"
      	tags: [ "tag1", "tag2" ]	    # optional field
  ```

- Run the check script after updating documentation:

  ```shell
  # runs on root path of omnipaxos repository
  node .github/scripts/doc_structure_checker.js
  ```

  

  

