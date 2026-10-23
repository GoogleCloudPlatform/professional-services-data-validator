# Implementation Plan - Fix Oracle & BigQuery Integration Tests (Issue #1702)

Resolve integration test failures reported in `docs/internal/design_docs/issue-1702-duckdb/instructions_second.md`:
1. `test_column_validation_all_null_oracle_to_postgres`: `IbisTypeError: Arguments differences_target_value:float64 and differences_source_value:string are not comparable`
2. `test_row_validation_comp_fields_oracle_to_postgres`: `duckdb.duckdb.ConversionException: Conversion Error: Could not convert Interval to Microsecond`
3. Verify BigQuery system integration tests (`tests/system/data_sources/test_bigquery.py`).

## User Review Required

> [!NOTE]
> * **Type-Coerced Equality in Difference Calculation**: In `_calculate_difference()`, when columns have differing inferred types across heterogeneous data sources (e.g. Oracle returning `string` "None" for null aggregates while Postgres returns `float64` `NaN`), direct AST equality `target_value == source_value` triggers `IbisTypeError`. In the string/value comparison branch, we will cast both expressions to `string` (`target_value.cast("string") == source_value.cast("string")`).
> * **DuckDB Interval & Timedelta Sanitization**: DuckDB's Arrow engine cannot directly convert certain complex interval types (such as Oracle `INTERVAL DAY TO SECOND` returned as Python `timedelta` / pandas `Timedelta`). We will sanitize `timedelta` / interval columns to `string` in `_sanitize_df_for_duckdb()` before creating DuckDB tables.

## Proposed Changes

### In-Memory Combiner Engine

#### [MODIFY] [combiner.py](file:///Users/mudupalli/professional-services-data-validator/data_validation/combiner.py)
1. **Fix `IbisTypeError` on cross-source column validation**:
   In `_calculate_difference()`, update the string/value-comparison equality check:
   ```python
   validation_status = (
       ibis.case()
       .when(
           target_value.isnull() & source_value.isnull(),
           consts.VALIDATION_STATUS_SUCCESS,
       )
       .when(
           target_value.cast("string") == source_value.cast("string"),
           consts.VALIDATION_STATUS_SUCCESS,
       )
       .else_(consts.VALIDATION_STATUS_FAIL)
       .end()
   )
   ```
2. **Fix `ConversionException` on Interval/Timedelta columns**:
   In `_sanitize_df_for_duckdb()`, detect `timedelta64`, `datetime.timedelta`, and `pandas.Timedelta` columns and convert them to string (`df[col] = df[col].astype("string")`) so DuckDB can ingest them safely as `VARCHAR`.

---

## Verification Plan

### Automated Tests
1. **Oracle Integration Tests**:
   ```bash
   source env/bin/activate
   export PROJECT_ID=pso-kokoro-resources
   export ORACLE_PASSWORD=$(gcloud secrets versions access latest --secret="oracle_password" --project="pso-kokoro-resources")
   export POSTGRES_PASSWORD=$(gcloud secrets versions access latest --secret="postgres_password" --project="pso-kokoro-resources")
   pytest tests/system/data_sources/test_oracle.py -k "test_column_validation_all_null_oracle_to_postgres or test_row_validation_comp_fields_oracle_to_postgres" -vv
   ```
2. **Full Unit Test Suite**:
   ```bash
   source env/bin/activate
   pytest tests/unit
   ```
3. **BigQuery System Integration Tests**:
   ```bash
   source env/bin/activate
   PROJECT_ID=pso-kokoro-resources pytest tests/system/data_sources/test_bigquery.py
   ```
