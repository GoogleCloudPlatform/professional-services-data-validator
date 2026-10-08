# Config Directory Concurrency

Config-directory runs are serial by default. To run up to four YAML files at
once:

```bash
data-validation configs run --config-dir configs --concurrency 4
```

Each file builds its own source and target clients. Validations within a file
remain serial, and DVT attempts backend-specific client cleanup when that file
finishes. The existing cleanup helper disposes Oracle and PostgreSQL clients;
this feature does not add cleanup support for other backends. All
files are attempted even if loading or executing one fails; the command then
raises a combined validation exception. Complete report and dry-run output
blocks are serialized, but file completion order is not guaranteed. A failing
validation can stop the remaining validations in its file, as with serial runs.
Reported validation failures are not execution exceptions.

Each validation can also run its source and target queries concurrently, so
choose the worker count according to database connection and workload limits.
Large row validations also consume local memory. The flag bounds the number of
active files, not the number of database connections or queries.
Kubernetes and Cloud Run indexed execution retain their existing single-file
selection behavior: concurrency greater than 1 is rejected with
`--kube-completions` or without `--config-dir`. The default of 1 preserves the
existing single-file and indexed execution paths.

Run-ID/config-file logging helps identify validations when logs interleave.
Output blocks remain per validation: concurrent CSV/JSON output is not a single
combined CSV or JSON document.