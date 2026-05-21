# DVT --config-dir & --config-dir-json Option Implementation Plan

This document outlines the plan to introduce two new directory output options for DVT validations (`column`, `row`, `schema`):
- `--config-dir`: stores validation configurations as individual YAML files.
- `--config-dir-json`: stores validation configurations as individual JSON files (for application/orchestration use cases).

## 1. Scope and Design Goals

- **CLI Exclusivity**: A validation command can specify at most one of `--config-file` (YAML file), `--config-file-json` (JSON file), `--config-dir` (YAML directory), and `--config-dir-json` (JSON directory).
- **GCS/Local Support**: Both directory options represent GCS (`gs://...`) or local paths. Filenames are generated dynamically under the specified path.
- **Table-Specific Configuration Files**: Generates one configuration file per validation task (represented by a `ConfigManager` instance).
- **Collision Prevention**: Prevents name collisions for duplicate table runs or validations split into multiple parts (due to `max_concat_columns`).
- **Out of Scope**:
  - Custom-query validations (`validate custom-query`)
  - Config run commands (`configs run`) for JSON directories (running `--config-dir-json` via `configs run` is out of scope for now; a future feature request should be created to address this).
  - Parts per file (`--parts-per-file` / `--configs-per-file`)

---

## 2. Proposed Changes

### A. CLI Argument Parsing (`data_validation/cli_tools.py`)

1. **Add `--config-dir` and `--config-dir-json` to Mutual Exclusion Group**:
   Update `_add_common_arguments` to place `--config-file`, `--config-file-json`, `--config-dir`, and `--config-dir-json` in a mutually exclusive group when `is_generate_partitions` is `False`.
   
   ```python
   if not is_generate_partitions:
       config_group = optional_arguments.add_mutually_exclusive_group()
       config_group.add_argument(
           "--config-file",
           "-c",
           help="Store the validation config in the YAML File Path specified",
       )
       config_group.add_argument(
           "--config-file-json",
           "-cj",
           help="Store the validation config in the JSON File Path specified to be used for application use cases",
       )
       config_group.add_argument(
           "--config-dir",
           "-cdir",
           help="Store the validation configs as individual YAML files in the specified directory path (GCS or local)",
       )
       config_group.add_argument(
           "--config-dir-json",
           "-cdirj",
           help="Store the validation configs as individual JSON files in the specified directory path (GCS or local)",
       )
   ```

2. **Argument Integrity Checks**:
   Ensure that custom-query validations cannot be run with either directory option. We will update `_check_custom_query_args`:
   
   ```python
   if getattr(parsed_args, "command", None) == "validate" and getattr(parsed_args, "validate_cmd", None) == "custom-query":
       if getattr(parsed_args, "config_dir", None) or getattr(parsed_args, "config_dir_json", None):
           parser.error("validate custom-query: directory-based validation storage (--config-dir / --config-dir-json) is not supported for custom-query validations")
   ```

---

### B. Entry Point & Storage Flow (`data_validation/__main__.py`)

1. **Update Main Runner Flow**:
   Modify the `run(args)` function to handle both `--config-dir` and `--config-dir-json`.
   
   ```diff
    def run(args) -> None:
        config_managers = build_config_managers_from_args(args)
        if args.config_file:
            store_yaml_config_file(args, config_managers)
        elif args.config_file_json:
            store_json_config_file(args, config_managers)
+       elif getattr(args, "config_dir", None):
+           store_config_dir(args, config_managers, is_json=False)
+       elif getattr(args, "config_dir_json", None):
+           store_config_dir(args, config_managers, is_json=True)
        else:
            run_validations(args, config_managers)
   ```

2. **Define Parameterized Directory Storing Logic**:
   Implement `store_config_dir(args, config_managers, is_json=False)` in `__main__.py`.
   
   ```python
   def store_config_dir(args, config_managers, is_json=False):
       """Build and store validation configs inside a directory (as YAML or JSON files).

       Args:
           args (Namespace): User specified Arguments.
           config_managers (list[ConfigManager]): List of config manager instances.
           is_json (bool): If True, store as JSON files. Otherwise, store as YAML.
       """
       if any(cm.validation_type == consts.CUSTOM_QUERY for cm in config_managers):
           raise ValueError("Saving custom-query validations to directory configs is not supported.")

       config_dir = args.config_dir_json if is_json else args.config_dir
       
       # Enforce empty directory to prevent accidental overwrites (Abort and Fail)
       if gcs_helper._is_gcs_path(config_dir):
           if gcs_helper.list_gcs_directory(config_dir):
               raise ValueError(f"GCS directory {config_dir} is not empty. Aborting.")
       else:
           if os.path.exists(config_dir) and os.listdir(config_dir):
               raise ValueError(f"Directory {config_dir} is not empty. Aborting.")

       seen_names = {}
       extension = "json" if is_json else "yaml"
       logging.info(f"Writing validation configs to directory: {config_dir}")

       for config_manager in config_managers:
           source_schema = config_manager.source_schema
           source_table = config_manager.source_table
           validation_type = config_manager.validation_type.lower()

           if source_schema:
               base_name = f"{source_schema}_{source_table}_{validation_type}"
           else:
               base_name = f"{source_table}_{validation_type}"

           # Prevent internal collisions within the same execution list
           if base_name not in seen_names:
               seen_names[base_name] = 0
               file_name = f"{base_name}.{extension}"
           else:
               seen_names[base_name] += 1
               file_name = f"{base_name}_{seen_names[base_name]}.{extension}"

           # Format config format matching is_json
           if is_json:
               config_to_store = convert_config_to_json([config_manager])
           else:
               config_to_store = convert_config_to_yaml(args, [config_manager])

           target_file_path = os.path.join(config_dir, file_name)
           cli_tools.store_validation(target_file_path, config_to_store, include_log=True)

       logging.info(f"Success! Validation configs written to directory: {config_dir}")
   ```

---

## 3. Testing Plan

### A. CLI Parsing Verification
In `tests/unit/test_cli_tools.py` (or `test__main__.py`):
- Test that combining any two or more of `--config-file`, `--config-file-json`, `--config-dir`, and `--config-dir-json` causes a parser argument validation error.
- Test that running `validate custom-query` with `--config-dir` or `--config-dir-json` triggers a parser error.

### B. Storage and Collision Avoidance Verification
In `tests/unit/test__main__.py`:
- Verify `store_config_dir(is_json=False)` writes correct YAML file structures.
- Verify `store_config_dir(is_json=True)` writes correct JSON file structures.
- Verify incremental suffixes are appended correctly (`_1`, `_2`) on name clashes for both formats.
- Verify that `store_config_dir` aborts with `ValueError` if the target directory (local or GCS) is not empty.

### C. Integration Tests
In `tests/system/data_sources/test_bigquery.py`:
- **YAML Integration Test:**
  - Call `validate column` with multiple tables and `--config-dir <dir>`.
  - Verify that descriptive YAML files are generated (e.g., `schema_table1_column.yaml`, `schema_table2_column.yaml`).
  - Call `configs run --config-dir <dir>` to run them and confirm no exceptions are raised.
- **JSON Integration Test:**
  - Call `validate column` with multiple tables and `--config-dir-json <dir>`.
  - Verify that descriptive JSON files are generated (e.g., `schema_table1_column.json`, `schema_table2_column.json`).
  - Programmatically load the generated JSON files in Python and run them via DVT core `DataValidation(config).execute()` to confirm no exceptions are raised.

---

## 4. Documentation Plan

The following documentation updates are planned and will be implemented upon successful code merge:

### A. Updates to `README.md`
1.  **Add Options to Validation Commands:**
    Add `--config-dir` and `--config-dir-json` syntax and descriptions to standard validation commands:
    *   `validate column`
    *   `validate row`
    *   `validate schema`
    
    *Note: Exclude them from `validate custom-query` commands as they are out of scope.*

    *Example addition:*
    ```markdown
      [--config-dir or -cdir CONFIG_DIR]
                            Directory path (local or GCS) to store validation configs as individual YAML files (one per table).
                            Target directory must be empty or not exist.
      [--config-dir-json or -cdirj CONFIG_DIR_JSON]
                            Directory path (local or GCS) to store validation configs as individual JSON files (one per table).
                            Target directory must be empty or not exist. Warning: JSON configs contain full credentials.
    ```

2.  **New Subsection under "Running DVT with YAML Configuration Files":**
    Create a new subsection titled `### Storing Validations to a Directory` explaining:
    *   The dynamic filename format: `[schema]_[table]_[validation_type].[extension]`.
    *   Collision prevention (suffix versioning `_1`, `_2` on splits).
    *   The **Abort and Fail** restriction when the target directory is not empty.
    *   A **Security Warning** about credential exposure in generated JSON configurations, recommending YAML for shared directories.

### B. Updates to `docs/examples.md`
Add three new code example blocks:
1.  **Store validation configs to a directory (Multiple Tables):**
    Show a `validate column` command using `-cdir` with a comma-separated table list, explaining the generated YAML files.
2.  **Store validation configs to a directory as JSON:**
    Show the same using `-cdirj`, noting it as an programmatic/orchestration use case.
3.  **Run validations from a configuration directory:**
    Show how to execute the directory's YAML files using `configs run --config-dir <dir>`. Document that running JSON directories via CLI is not supported.
