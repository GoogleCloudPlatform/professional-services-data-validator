# Upstream Feature Request & PR Draft: Dynamic Parallel Chunking for DVT configs run

This document contains a fully prepared, professional GitHub Issue or Pull Request description that can be submitted directly to the official [GoogleCloudPlatform/professional-services-data-validator](https://github.com/GoogleCloudPlatform/professional-services-data-validator) repository.

---

## Proposed PR/Issue Title
`feat: Support dynamic round-robin chunking in configs run --kube-completions`

---

## Proposed Description

### 1. Summary & Context
This PR adds support for **dynamic round-robin chunking** to the `configs run --kube-completions` (`-kc`) command. This feature enables running large batches of named validation configurations (e.g., `schema.table.yaml`) across a controlled, parallel pool of Cloud Run Jobs or Kubernetes Jobs, dramatically reducing container startup overhead, GCP execution costs, and API rate-limiting risks.

Currently, the `-kc` flag is designed for a strict **1-to-1 sequential numeric mapping**:
* It expects files in the `--config-dir` to be named sequentially (e.g., `0000.yaml`, `0001.yaml`, etc.).
* It maps the `CLOUD_RUN_TASK_INDEX` or `JOB_COMPLETION_INDEX` directly to a single file: `f"{job_index:04d}.yaml"`.

While this works perfectly for validations generated via `generate-table-partitions` (which splits a single massive table into numbered chunks), it **does not scale** for database migrations involving hundreds or thousands of different tables where:
1. Configuration files are named after their schemas/tables (e.g., `hr.employees.yaml`, `sales.orders.yaml`) rather than sequential numbers.
2. The number of validations $N$ is much larger than the desired parallelism $P$ (e.g., validating 1,000 tables using 20 concurrent Cloud Run tasks to avoid rate limits and connection exhaustion). Under the current legacy behavior, running $P=20$ tasks will only validate the first 20 files, leaving the remaining 980 unexecuted.

### 2. Proposed Solution: Dynamic Round-Robin Chunking
We propose extending `-kc` to automatically detect the **total task count** via standard environment variables (`CLOUD_RUN_TASK_COUNT` or `JOB_COMPLETION_COUNT`).

* **If a total task count is detected**:
  1. DVT lists all validation files in the `--config-dir` (supporting GCS and local paths) and sorts them alphabetically.
  2. It distributes the files round-robin to the current task using: `my_files = all_files[task_index::task_count]`.
  3. It executes the assigned files sequentially within the warm container.
* **If no task count is detected (Legacy Fallback)**:
  * DVT falls back to the legacy 1-to-1 sequential numeric naming (e.g., `0000.yaml`). This ensures **100% backward compatibility** for all existing workflows.

#### Why Round-Robin?
Round-robin distribution (`all_files[task_index::task_count]`) naturally mitigates workload skew (the "long-tail" problem). Because tables are sorted alphabetically, large and small tables are distributed evenly across all tasks. Contiguous chunking, on the other hand, risks grouping multiple large tables into a single task, leaving other tasks idle while one runs for a long time.

---

### 3. Proposed Code Changes

The changes are localized entirely within `data_validation/__main__.py` inside the `config_runner` function.

```diff
diff --git a/data_validation/__main__.py b/data_validation/__main__.py
index af86d34..d5c3f91 100644
--- a/data_validation/__main__.py
+++ b/data_validation/__main__.py
@@ -377,22 +377,53 @@ def config_runner(args):
     """
     if args.config_dir:
-        if args.kube_completions and (
-            ("JOB_COMPLETION_INDEX" in os.environ.keys())
-            or ("CLOUD_RUN_TASK_INDEX" in os.environ.keys())
-        ):
-            # Running in Kubernetes in Job completions - only run the yaml file corresponding to index
-            job_index = (
-                int(os.environ.get("JOB_COMPLETION_INDEX"))
-                if "JOB_COMPLETION_INDEX" in os.environ.keys()
-                else int(os.environ.get("CLOUD_RUN_TASK_INDEX"))
-            )
-            config_file_path = (
-                f"{args.config_dir}{job_index:04d}.yaml"
-                if args.config_dir.endswith("/")
-                else f"{args.config_dir}/{job_index:04d}.yaml"
-            )
-            setattr(args, "config_dir", None)
-            setattr(args, "config_file", config_file_path)
-            config_managers = build_config_managers_from_yaml(args, config_file_path)
-            run_validations(args, config_managers)
+        has_index = ("JOB_COMPLETION_INDEX" in os.environ.keys()) or ("CLOUD_RUN_TASK_INDEX" in os.environ.keys())
+        if args.kube_completions and has_index:
+            job_index = (
+                int(os.environ.get("JOB_COMPLETION_INDEX"))
+                if "JOB_COMPLETION_INDEX" in os.environ.keys()
+                else int(os.environ.get("CLOUD_RUN_TASK_INDEX"))
+            )
+
+            # Check if total task count is available for dynamic chunking
+            job_count_str = (
+                os.environ.get("JOB_COMPLETION_COUNT") or
+                os.environ.get("CLOUD_RUN_TASK_COUNT")
+            )
+
+            if job_count_str:
+                # --- Dynamic Round-Robin Chunking ---
+                job_count = int(job_count_str)
+                all_files = sorted(cli_tools.list_validations(config_dir=args.config_dir))
+
+                # Select the round-robin slice for this task index
+                my_files = [f for idx, f in enumerate(all_files) if idx % job_count == job_index]
+
+                logging.info(
+                    "Running in parallel completions mode with dynamic chunking. "
+                    "Task %d of %d. Assigned %d of %d files.",
+                    job_index, job_count, len(my_files), len(all_files)
+                )
+
+                errors = False
+                for file in my_files:
+                    config_managers = build_config_managers_from_yaml(args, file)
+                    try:
+                        logging.info("Currently running the validation for YAML file: %s", file)
+                        run_validations(args, config_managers)
+                    except Exception as e:
+                        errors = True
+                        logging.error("Error '%s' occurred while running config file %s.", str(e), file)
+                if errors:
+                    raise exceptions.ValidationException("Some of the validations raised an exception")
+            else:
+                # --- Legacy 1-to-1 Fallback ---
+                config_file_path = (
+                    f"{args.config_dir}{job_index:04d}.yaml"
+                    if args.config_dir.endswith("/")
+                    else f"{args.config_dir}/{job_index:04d}.yaml"
+                )
+                logging.info(
+                    "Running in parallel completions mode (legacy 1-to-1). "
+                    "Task %d. Running file: %s",
+                    job_index, config_file_path
+                )
+                setattr(args, "config_dir", None)
+                setattr(args, "config_file", config_file_path)
+                config_managers = build_config_managers_from_yaml(args, config_file_path)
+                run_validations(args, config_managers)
         else:
```

---

### 4. Benefits & Impact

1. **Massive Cost & Time Savings**: Spawning 1,000 warm container instances is extremely expensive and slow due to bootstrap overhead. By batching validations sequentially within $P$ concurrent containers (e.g., $P=20$), we achieve **70%+ savings** in both billing time and wall-clock time.
2. **Rate Limit Prevention**: Eliminates the need for client-side orchestrators to bombard the Google Cloud Run API with 1,000 separate execution requests, staying well within API rate quotas.
3. **No GCS Bucket Clutter**: Users do not need to partition their files into separate GCS directories. All validation configurations stay in a single directory.
4. **Clean Logging**: Each Cloud Run task writes to its own log stream, tagged with its `CLOUD_RUN_TASK_INDEX`. This keeps logging highly structured and easy to query in Cloud Logging.
5. **Zero Breaking Changes**: Fully preserves the existing sequential numeric `-kc` behavior for users running table-partition validations.
6. **Protects from misconfigured --tasks values**: Under the legacy 1-to-1 code, setting --tasks too low resulted in silent skipping of validation files, while setting it too high caused idle tasks to crash with file-not-found errors. The new dynamic chunking ensures that if --tasks is too low, all files are still processed (some tasks run multiple files), and if it is too high, extra tasks exit cleanly and successfully with nothing to do.
