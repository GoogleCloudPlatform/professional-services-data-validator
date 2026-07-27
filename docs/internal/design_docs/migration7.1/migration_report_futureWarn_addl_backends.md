# DVT Migration to Ibis 7.1.0: FutureWarning Resolution & Additional Backends Walkthrough

This walkthrough documents the changes made in the current session to successfully resolve Ibis v7 / Pandas deprecation `FutureWarning`s and enable compatibility for additional backends (Filesystem and Hive/Impala).

---

## Summary of Changes

### 1. FutureWarning Resolutions

* **Ibis Table `relabel` Deprecation**:
  * In [clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py), replaced the deprecated `Table.relabel` call with `Table.rename`, swapping keys and values to align with the new API signature:
    ```diff
    -iq = iq.relabel(dict(zip(iq.columns, [_.lower() for _ in iq.columns])))
    +iq = iq.rename(dict(zip([_.lower() for _ in iq.columns], iq.columns)))
    ```

* **Ibis Backend `list_databases` Deprecation**:
  * In [clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py), updated `list_schemas(client)` to prioritize `client.list_schemas()` over `client.list_databases()`, preventing runtime deprecation warnings for modern backends.

* **Ibis Registry `.output_dtype` Deprecation**:
  * Replaced legacy `.output_dtype` references with `.dtype` across all custom backend dialect registries:
    * `third_party/ibis/ibis_addon/operations.py`
    * `third_party/ibis/ibis_bigquery/registry.py`
    * `third_party/ibis/ibis_cloud_spanner/registry.py`
    * `third_party/ibis/ibis_db2/registry.py`
    * `third_party/ibis/ibis_db2_zos/registry.py`
    * `third_party/ibis/ibis_impala/registry.py`
    * `third_party/ibis/ibis_mssql/registry.py`
    * `third_party/ibis/ibis_postgres/registry.py`
    * `third_party/ibis/ibis_sybase/registry.py`
    * `third_party/ibis/ibis_teradata/registry.py`

* **Pandas Equality Testing for Null/NaN Values**:
  * Replaced direct `pandas.testing.assert_frame_equal` assertions in [test_combiner.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/unit/test_combiner.py) and [test_bigquery.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/system/result_handlers/test_bigquery.py) with a helper function `_assert_frame_equal`.
  * The helper uses `pandas.option_context("future.no_silent_downcasting", True)` and explicitly fills NaNs (`.fillna(value=float("nan"))`) to avoid warnings about mismatched null-like values (NaN vs. None) in pandas.

### 2. Additional Backend Compatibility

* **Impala & Hive Connection Customizations**:
  * In [clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py), registered `binary` in the Impala UDF type mapper to fix binary column inference.
  * Created an `impala_connect` wrapper that runs `set hive.resultset.use.unique.column.names=false` against raw SQL on connection to ensure schema column names match standard patterns without unique column name prefixes, and mapped it in `CLIENT_LOOKUP`.
* **Hive Test Auth Mechanism**:
  * In [test_hive.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/system/data_sources/test_hive.py), added an environment variable fallback `HIVE_AUTH_MECH` (defaulting to `"PLAIN"`) to the connection parameters to support secure Hive/Impala setups.

---

## Verification Results

1. **Unit Tests**:
   * Command: `pytest tests/unit`
   * Results: **393 passed, 11 skipped, 2 warnings**
2. **Filesystem Integration Tests**:
   * Command: `pytest tests/system/data_sources/test_filesystem.py`
   * Results: **8 passed, 1 warning**
3. **Hive Integration Tests**:
   * Command: `PROJECT_ID=XXXXXX pytest tests/system/data_sources/test_hive.py`
   * Results: **12 passed, 8 skipped, 96 warnings**
