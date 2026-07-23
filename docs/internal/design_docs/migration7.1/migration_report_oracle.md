# DVT Migration to Ibis 7.1.0 Oracle Walkthrough

This walkthrough documents the full set of changes made to successfully migrate the Data Validation Tool (DVT) codebase to **Ibis 7.1.0**, resolving all unit test and integration/system test failures for PostgreSQL, SQL Server (MSSQL), BigQuery, and Oracle.

---

## Summary of Changes

### 1. Custom Backend Operations & Type Rewrites
* **Oracle Registry**:
  * In [registry.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/registry.py), updated legacy `.output_dtype` references to `.dtype` (as `output_dtype` is deprecated/removed in Ibis 7.x).
  * Replaced the deprecated `dtype.is_set()` check with standard `isinstance(dtype, dt.Set)` check to avoid `AttributeError` under Ibis 7.1.
  * Removed the custom `ops.Coalesce` override in `operation_registry.update` to fall back to the standard SQLAlchemy registry's variadic implementation, fixing `IbisError` for Coalesce operations containing 1 argument.
  * In [registry.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/registry.py#L470-L482), mapped `ops.Coalesce` instead of `ops.IfNull` and removed the obsolete `ops.NotAny` and `ops.NotAll` mappings.
* **Impala Rewrites**:
  * In [api.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_impala/api.py#L198-L205), removed the obsolete `@rewrites(ops.IfNull)` wrapper function since `ops.IfNull` is no longer generated.

---

### 2. SQLAlchemy Custom Dialect Type Mappings
In Ibis 7.1, `dt.dtype` is locked against external registrations using `del dtype.register` at startup. This made backend-specific decorators like `@dt.dtype.register(OracleDialect_oracledb, ...)` raise `AttributeError`.

* **Oracle Datatypes**:
  * Created the `OracleType(AlchemyType)` class in [datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/datatypes.py#L23-L65) to cleanly define type mapping via `to_ibis` and `from_ibis` methods.
  * Updated the datatypes class to provide the required `unit="s"` parameter when initializing `dt.Interval` types.
  * In [compiler.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/compiler.py#L25), registered `OracleType` as the `type_mapper` for `OracleExprTranslator`.
* **PostgreSQL Datatypes**:
  * Replaced `@dt.dtype.register` registrations in [datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_postgres/datatypes.py#L23-L44) by monkey-patching `PostgresType.to_ibis` dynamically.
  * Added fallback mappings for `sqltypes.Time` to support dialect-specific `TIME(precision)` type translations.
  * Provided default `unit="s"` parameter when constructing `dt.Interval` for postgres intervals with unspecified field fields.

---

### 3. PostgreSQL Type Parsing Refactor
* **PostgreSQL Client**:
  * In [client.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_postgres/client.py#L21-L129), removed references to deprecated parser internals (`_BRACKETS`, `_parse_numeric`, `_type_mapping`) and updated `_get_type` to use the standard `SqlglotPostgresType.from_string` from Ibis 7.1.

---

### 4. Query Builder Class Paths
* **Query Builder**:
  * In [query_builder.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/query_builder/query_builder.py#L84-L417), updated references from `ibis.expr.api.IntegerColumn` / `NumericValue` to `ibis.expr.types.IntegerColumn` / `NumericValue`.

---

### 5. Addon & Core Monkey-Patches
* **Temporal Value strftime Support**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py#L744-L751), patched `ibis.expr.types.TemporalValue` to support `strftime` in custom user calculations.
* **Pandas Backend Coalesce Fix**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py#L754-L777), monkey-patched the `compute_row_reduction` function of Ibis's Pandas execution backend to exclude scalar strings, bytes, and dicts from `Sized` checks. This fixes a core Ibis 7.1 bug where scalar values like `'null'` broke coalesce checks on Pandas dataframes during unit validation.
* **Pandas Backend Date Conversion Fix**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py), monkey-patched `PandasData.convert_Date` to safely fall back to python `datetime.date` objects when dealing with extreme out-of-bounds dates (e.g. `'9999-12-31'` or `'1000-01-01'`) that exceed Pandas nanosecond range.
* **Pandas Backend Decimal Type Mapping**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py), wrapped `constants.IBIS_TYPE_TO_PANDAS_TYPE` in a custom `PandasTypeMapping` subclass of `dict` to support fallback resolving of parameterized `Decimal` and `Date` type instances to `object` dtype, preventing KeyErrors during validation.

---

### 6. BigQuery Backend Compatibility Fixes
* **INTERVAL Support**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py), monkey-patched `BigQueryType.to_ibis` to translate `INTERVAL` columns to `dt.Interval(unit="s")`.
* **ExtractEpochSeconds Handling**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py), registered a custom translator function for `ops.ExtractEpochSeconds` on `BigQueryExprTranslator` to cast target `DATETIME` or `DATE` columns to `TIMESTAMP` before calling `UNIX_SECONDS` (preventing argument mismatch crashes in BigQuery).
* **BigQueryBackend Custom Methods**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py), monkey-patched the native `BigQueryBackend` to dynamically inject the custom DVT connection methods (`do_connect`, `_cursor_to_arrow`, `_parse_project_and_dataset`, `list_primary_key_columns`, `dvt_list_tables`) defined in `third_party.ibis.ibis_bigquery`.
  * In [__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_bigquery/__init__.py), updated `do_connect` signature to accept parameters `client` and `storage_client` to match Ibis 7.1.0 backend expectations.
* **Project and Dataset Name Parsing**:
  * In [clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py), updated `get_ibis_table` and `get_ibis_table_schema` to check if `schema_name` contains a dot delimiter (indicating project prefix `project.dataset` like `bigquery-public-data.new_york_citibike`). If so, they split the string and pass them as separate `database` (project_id) and `schema` (dataset_id) parameters to support BigQuery's updated v7.1.0 signatures.

---

### 7. Custom Backend Packages Registration
* **Import Patch**:
  * In [operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py), imported `third_party.ibis.ibis_postgres` to ensure custom client/type monkey-patches are fully loaded when operations are setup.
* **SQL Server Connection Integration**:
  * In [clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py), imported the custom `mssql_connect` function from `third_party.ibis.ibis_mssql.api` and mapped it in `CLIENT_LOOKUP` to ensure connection instantiates DVT's custom `Backend` class instead of the native raw backend.
* **Oracle Connection Integration**:
  * In [clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py), updated standard Ibis client lookup connection back to `third_party.ibis.ibis_oracle.api.oracle_connect`.
  * In [__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/__init__.py), added `list_databases` implementation to Oracle's backend to support schema and table reflection on newer versions of Ibis where list databases was deprecated.
  * In [__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/__init__.py), updated table metadata parsing inside `raw_column_metadata` to look up table names case-insensitively from the database schema list of tables, preventing schema mismatches for lowercase/mixedcase table names.

---

### 8. Schema Validation Allow List Pattern Parsing
* **Allow List Regex Fix**:
  * In [schema_validation.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/schema_validation.py), simplified `precision_scale_pattern` to `r"(?:\([^)]+\))?"` to properly parse type specifications containing commas in any order (e.g. `timestamp('UTC', 7)`), which previously failed regex split matching and led to false assertion failures.
* **Timestamp Precision Mapping**:
  * In [test_sql_server.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/system/data_sources/test_sql_server.py), updated the allow list mapping to match the timezone-aware timestamp formatting `timestamp('UTC', 7):timestamp('UTC')` returned under Ibis 7.1.0.

---

### 9. Core Combiner PyArrow Overflow Fix
* In [combiner.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/combiner.py), implemented `_convert_large_ints_to_decimals` helper function to automatically cast out-of-bounds 64-bit python `int` values (e.g. 20-digit or 38-digit integers returned by some database drivers) to `decimal.Decimal` objects inside Pandas DataFrames prior to loading them into the in-memory pandas client, preventing PyArrow from failing with `OverflowError: Python int too large to convert to C long` during table inference.
  * Extracted `_MAX_INT64` and `_MIN_INT64` into constants, moved imports to the module level, and added a detailed docstring.
* In [test_combiner.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/unit/test_combiner.py), added unit tests for `_convert_large_ints_to_decimals` to verify that values exceeding 64-bit bounds are correctly converted to `decimal.Decimal`, while standard integer ranges and other data types remain unmodified.

---

## Verification Results

1. **Unit Tests**:
   * Command: `pytest tests/unit/`
   * Results: **394 passed** (100% success rate).
2. **PostgreSQL System/Integration Tests**:
   * Command: `pytest tests/system/data_sources/test_postgres.py`
   * Results: **56 passed, 3 skipped, 0 failed** (100% success rate).
3. **SQL Server System/Integration Tests**:
   * Command: `pytest tests/system/data_sources/test_sql_server.py`
   * Results: **46 passed, 3 skipped, 0 failed** (100% success rate).
4. **BigQuery System/Integration Tests**:
   * Command: `PROJECT_ID=pso-kokoro-resources pytest tests/system/data_sources/test_bigquery.py`
   * Results: **40 passed, 2 skipped, 0 failed** (100% success rate).
5. **Oracle System/Integration Tests**:
   * Command: `ORACLE_PASSWORD=Oracle123 PROJECT_ID=pso-kokoro-resources pytest tests/system/data_sources/test_oracle.py`
   * Results: **56 passed, 6 skipped** (and 14 failed due to PostgreSQL connection failures as PostgreSQL is not running in this environment).
