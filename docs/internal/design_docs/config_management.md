# DVT Validation Configuration Management Design

This document provides a comprehensive overview of how the Data Validation Tool (DVT) utilizes, stores, and executes configuration files and directories. It serves as the single source of truth for DVT's validation configuration architecture.

---

## 1. Overview of Configuration Options

DVT supports storing validation definitions in configurations rather than executing them immediately. This enables:
*   **Reproducibility**: Re-running the same validations easily.
*   **Orchestration**: Splitting large validations and running them concurrently in distributed environments (e.g., Kubernetes, Cloud Run).
*   **Application Integration**: Exposing validation definitions as JSON for consumption by external applications.

DVT provides two primary storage strategies: **Single File** (YAML/JSON) and **Directory-Based** (YAML/JSON).

### CLI Options Matrix

| CLI Command | Storage Option | Format | Output Structure | Target Requirement | Empty Dir Enforcement |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`validate`** (standard) | `--config-file` / `-c` | YAML | Single File | Optional (runs validation if omitted) | N/A |
| | `--config-file-json` / `-cj` | JSON | Single File | Optional (runs validation if omitted) | N/A |
| | `--config-dir` / `-cdir` | YAML | Flat Directory | Optional (runs validation if omitted) | **Yes** (Aborts if not empty) |
| | `--config-dir-json` / `-cdirj` | JSON | Flat Directory | Optional (runs validation if omitted) | **Yes** (Aborts if not empty) |
| **`generate-table-partitions`** | `--config-dir` / `-cdir` | YAML | Nested Directory | **Required** | **No** (Overwrites/Merges) |
| **`configs run`** | `--config-file` / `-c` | YAML | N/A (Input File) | **Required** (if no `--config-dir`) | N/A |
| | `--config-dir` / `-cdir` | YAML | N/A (Input Dir) | **Required** (if no `--config-file`) | N/A |

*Note: Custom-query validations (`validate custom-query`) do not support directory-based storage. It is assumed that custom-query validations with have unique inputs that require a single output file.*

---

## 2. Storage Strategies & Directory Structures

### A. Single-File Configuration (Default)
When using `--config-file` or `--config-file-json`, DVT consolidates all validation tasks specified in the command (even across multiple tables) into a **single** file.

*   **YAML (`--config-file`)**: Standard format for manual editing and re-runs.
*   **JSON (`--config-file-json`)**: Intended for programmatic integration.

### B. Flat Directory-Based Configuration (Standard Validations)
When running standard validations (`validate column/row/schema`) with `--config-dir` or `--config-dir-json`, DVT generates **one configuration file per table validation task**.

*   **Directory Structure**: Flat. All files are written directly to the specified directory.
*   **Naming Convention**: `[schema].[table].[extension]`
    *   If schema is omitted: `[table].[extension]`
    *   Example: `dbo.customers.yaml` or `sales.json`
*   **Collision Prevention**: If the same table appears multiple times in the same execution list, DVT appends an incremental suffix (e.g., `dbo.customers_1.yaml`, `dbo.customers_2.yaml`).
*   **Overwrite Protection**: DVT strictly enforces that the target directory must be empty or not exist. If files are present, DVT aborts execution with a `ValueError` to prevent accidental data loss.

### C. Nested Directory-Based Configuration (Partitioned Validations)
The `generate-table-partitions` command splits a single large table validation (specifically `row` validations) into multiple chunked validations based on primary keys. It **requires** directory-based storage.

*   **Directory Structure**: Nested. DVT automatically creates a subdirectory for each table.
    *   Structure: `<config_dir>/<schema>.<table_name>/`
    *   Example: `my_partitions/dbo.customers/`
*   **Naming Convention**: Files inside the subdirectory are sequentially numbered: `0000.yaml`, `0001.yaml`, `0002.yaml` up to `9999.yaml`.
*   **Parts Per File**: The `--parts-per-file` (or `-ppf`) option (default: 1) controls how many partition validation blocks are packed into a single YAML file.
*   **Overwrite Protection**: Unlike standard validations, `generate-table-partitions` **does not** enforce an empty directory. It will write or overwrite files in the target directory directly.
*   **File Limit**: DVT limits the number of generated files to 10,000 per run. If `partition-num / parts-per-file` exceeds 10,000, it raises a parser error.

---

## 3. Execution and Orchestration Flow

### A. Standard Directory Execution
To run all validations stored in a directory, use:
```bash
data-validation configs run --config-dir <path_to_directory>
```
*   **Non-Recursive Behavior**: `configs run` lists and executes `.yaml` files **only** in the immediate directory (sorted alphabetically by filename). It does not traverse subdirectories.
*   **Empty Directory Validation**: If no `.yaml` files exist in the target directory (for example, due to a misspelled Cloud Storage prefix), DVT raises a `ValueError` rather than silently reporting a successful no-op run.
*   **Continue-on-Error Execution**: Files are executed sequentially. If a validation file raises an exception, the error (with stack trace) is logged and execution continues through the remaining files; a `ValidationException` is raised once all files have been attempted.
*   **Executing Partitioned Validations**: Because `generate-table-partitions` nests files under `<config_dir>/<schema>.<table_name>/`, you **must target the specific table subdirectory** to run them:
    ```bash
    data-validation configs run --config-dir my_partitions/dbo.customers/
    ```
*   **YAML Exclusivity**: CLI execution via `configs run` only supports YAML files. Any `.json` files in the directory are ignored (as `list_validations` filters strictly for `.yaml` extensions).

### B. Distributed Orchestration (`--kube-completions`)
For large-scale migrations, validations stored in a directory (either flat table-per-file directories or partitioned subdirectories) can be executed concurrently in containerized environments like Kubernetes (Indexed Jobs) or Cloud Run (Jobs).

When executing with `--kube-completions` (or `-kc`):
```bash
data-validation configs run --config-dir <path_to_directory> --kube-completions
```
1.  DVT detects the orchestrator-supplied task index environment variable:
    *   `JOB_COMPLETION_INDEX` (Kubernetes)
    *   `CLOUD_RUN_TASK_INDEX` (Cloud Run)
2.  How the `.yaml` files in `--config-dir` are distributed across tasks depends on whether the total task count (`JOB_COMPLETION_COUNT` for Kubernetes, or `CLOUD_RUN_TASK_COUNT` which Cloud Run injects automatically) is available:
    *   **Dynamic Round-Robin Chunking (when `JOB_COMPLETION_COUNT` or `CLOUD_RUN_TASK_COUNT` is set)**:
        1. All `.yaml` files in the directory are sorted by filename (`all_files`).
        2. The current task is assigned the slice `all_files[job_index::job_count]` (starting at `job_index` and taking every `job_count`-th file).
        3. The task executes its assigned subset of files sequentially. This works with **any** directory of YAML files regardless of naming convention (both `dbo.customers.yaml` and `0000.yaml`), and the number of tasks does not need to equal the number of YAML files.
    *   **Legacy 1-to-1 Sequential Mapping Fallback (when neither task-count variable is set)**:
        1. Occurs in Kubernetes Indexed Jobs if `JOB_COMPLETION_COUNT` is omitted from the Job manifest.
        2. DVT maps `job_index` (e.g., `3`) to the sequentially named file `<job_index:04d>.yaml` (e.g., `0003.yaml`) and executes only that file.
        3. If `.yaml` files exist in `--config-dir` but `<job_index:04d>.yaml` is not present among them, DVT raises a `ValueError` prompting the user to set `JOB_COMPLETION_COUNT` in the Job manifest.

---

## 4. Technical Debt & Future Alignments

To maintain backward compatibility, several inconsistencies currently exist in DVT's configuration handling. These should be targeted for alignment in future updates:

1.  **Empty Directory Enforcement**:
    *   *Current*: Standard validations abort if the directory is not empty; partition generation silently overwrites/merges.
    *   *Goal*: Implement safe overwrite protection (or explicit `--force` flags) consistently across all directory-writing commands.
2.  **JSON Execution Support**:
    *   *Current*: DVT can write JSON directories, but cannot execute them via `configs run`.
    *   *Goal*: Allow `configs run` to optionally parse and execute JSON-based configuration directories.
3.  **Unified File Grouping (`--configs-per-file` / `--parts-per-file` Deprecation)**:
    *   *Proposal*: Deprecate `--parts-per-file` (currently exclusive to `generate-table-partitions`) and replace it with a unified, global CLI option: `--configs-per-file` (or `-cpf`).
    *   *Scope*: Support this option in both partition generation and standard validations (`validate column/row/schema`).
    *   *Behavior for Standard Validations (`--config-dir`)*:
        *   If `--configs-per-file` is **omitted** or set to **1** (default): DVT maintains backward compatibility, using **descriptive naming** (`dbo_customers_column.yaml`) and writing 1 validation per file.
        *   If `--configs-per-file` is **strictly greater than 1**: DVT groups up to `N` table validations per file and switches to **sequential naming** (`0000.yaml`, `0001.yaml`...) directly under the flat target directory.
    *   *Behavior for Partitions (`generate-table-partitions`)*:
        *   `--configs-per-file` acts as a direct replacement for `--parts-per-file` (with the latter kept as a deprecated alias). It always uses sequential naming inside the nested subdirectory, matching current behavior.
    *   *Orchestration Benefit*: While `--kube-completions` now supports dynamic round-robin distribution of individual table YAML files across workers when `CLOUD_RUN_TASK_COUNT` or `JOB_COMPLETION_COUNT` is set, packing multiple table validations into a single YAML file reduces per-file client initialization overhead and also enables legacy 1-to-1 indexed execution without setting `JOB_COMPLETION_COUNT`.
