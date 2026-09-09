# Implementation Plan - Fix Unit & BigQuery Tests for DuckDB Combiner (Issue #1702)

Resolve test failures in `pytest tests/unit` and `pytest tests/system/data_sources/test_bigquery.py` resulting from migrating the in-memory pandas client to DuckDB and Ibis 7.1.

## User Review Required

> [!NOTE]
> DuckDB is a strongly typed columnar SQL engine. Unlike pandas in-memory joins which treat `NaN == NaN` as matching by default, DuckDB follows SQL standards where `NULL = NULL` is unknown/false. We will use Ibis's `identical_to` (`IS NOT DISTINCT FROM`) for outer join predicates in `_join_pivots` to preserve NULL-matching semantics across group-by keys.

## Analysis of Root Causes & Proposed Fixes

1. **`AttributeError: module 'ibis' has no attribute 'duckdb'` in tests using `pyfakefs`**
   - **Cause:** `pyfakefs` intercepts filesystem calls during test execution. When `ibis.duckdb.connect()` is invoked dynamically, Ibis attempts to discover backend entrypoints on the mocked filesystem and fails.
   - **Fix:** Import `ibis.backends.duckdb` explicitly at the top of `data_validation/combiner.py` and `data_validation/clients.py` so the backend is registered in `sys.modules` before any test mocks the filesystem. Also update fixtures in `tests/unit/test_data_validation.py`.

2. **`AssertionError: DataFrame.iloc[:, 5] (column name="num_random_rows") are different (<NA> != nan)` in `test_combiner.py`**
   - **Cause:** DuckDB returns pandas nullable integer (`Int64`) columns containing `<NA>` for integer nulls, whereas DVT downstream consumers and tests expect `numpy.nan` (float/object).
   - **Fix:** In `_generate_report_slice` of `combiner.py`, cast `num_random_rows` to `float64` / replace `<NA>` with `np.nan` before returning the final report DataFrame.

3. **`Binder Error: No function matches the given name and argument types 'encode(BLOB, STRING_LITERAL)'` in `test_generate_report_with_binary_data`**
   - **Cause:** In `_pivot_result`, casting binary/BLOB columns to string compiles to `encode(..., 'escape')` in SQLAlchemy DuckDB dialect, which DuckDB does not support.
   - **Fix:** Register the DuckDB compiler translator for `ops.Cast` in `third_party/ibis/ibis_addon/operations.py` to translate binary-to-string casts as `lower(hex(arg))`.

4. **`test_grouped_count_validator` failure in BigQuery integration tests**
   - **Cause 1:** Joining grouped tables on columns containing `NULL` values failed to match rows because standard SQL joins evaluate `NULL = NULL` as false.
   - **Cause 2:** `df[consts.SOURCE_AGG_VALUE].sum()` failed with `TypeError` when summing string series instead of converting to numeric/float first (like `test_count_validator` does with `.astype(float)`).
   - **Fix:** Use `identical_to` in join keys in `_join_pivots` and ensure numeric aggregation columns are summed with `.astype(float)` in `test_bigquery.py`.

5. **`test_list_databases` failure in `tests/unit/ibis_oracle/test_init.py`**
   - **Cause:** `ORACLE_SYSTEM_SCHEMAS` filter did not exclude Oracle container common users starting with `C##`.
   - **Fix:** Add `and not schema.upper().startswith("C##")` in `third_party/ibis/ibis_oracle/__init__.py`.

---

## Proposed Changes

### Core Combiner & Clients

#### [MODIFY] [combiner.py](file:///Users/mudupalli/professional-services-data-validator/data_validation/combiner.py)
- Import `ibis.backends.duckdb` at module level.
- Update `_join_pivots` to use `[left[k].identical_to(right[k]) for k in join_keys]` for null-safe outer joining.
- Ensure `num_random_rows` and untyped null columns are safely typed.
- Ensure output DataFrame converts nullable `Int64` nulls to `float64` / `np.nan` where expected.

#### [MODIFY] [clients.py](file:///Users/mudupalli/professional-services-data-validator/data_validation/clients.py)
- Import `ibis.backends.duckdb` at module level.

---

### Ibis Addon & Backend Operations

#### [MODIFY] [operations.py](file:///Users/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py)
- Register `ops.Cast` and `ops.ExtractEpochSeconds` compiler translations for DuckDB backend.

#### [MODIFY] [ibis_oracle/__init__.py](file:///Users/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/__init__.py)
- Update `list_databases` to filter out schemas starting with `C##`.

---

### Tests

#### [MODIFY] [test_data_validation.py](file:///Users/mudupalli/professional-services-data-validator/tests/unit/test_data_validation.py)
- Update `ibis_pandas` fixture to connect to DuckDB and import `ibis.backends.duckdb`.

#### [MODIFY] [test_bigquery.py](file:///Users/mudupalli/professional-services-data-validator/tests/system/data_sources/test_bigquery.py)
- Update `test_grouped_count_validator` to use `df[consts.SOURCE_AGG_VALUE].astype(float).sum()`.

---

## Verification Plan

### Automated Tests
1. **Unit Tests:**
   ```bash
   source env/bin/activate && pytest tests/unit
   ```
2. **BigQuery System Tests:**
   ```bash
   source env/bin/activate && PROJECT_ID=pso-kokoro-resources pytest tests/system/data_sources/test_bigquery.py
   ```
