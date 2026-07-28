# Architectural Review & Refactoring Report: Ibis Backend Modernization

## Executive Summary

This report provides a comprehensive architectural review of the backend-specific extensions and custom operations within the Data Validation Tool (DVT), addressing the design challenges highlighted in [Issue 1771](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1771).

### Current Challenges
1. **Divergent Extension Patterns (Monkey-Patching vs. Subclassing)**: Some backends (e.g., [PostgreSQL](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_postgres), [Snowflake](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_snowflake), [MySQL](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql), [BigQuery](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_bigquery), [Impala](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_impala)) mutate native Ibis backend classes dynamically at import time. Other backends (e.g., [Oracle](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_oracle), [MSSQL](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mssql), [DB2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_db2), [DB2 z/OS](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_db2_zos), [Sybase](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_sybase), [Teradata](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_teradata), [Spanner](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_cloud_spanner), [Redshift](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_redshift)) inherit from `BaseAlchemyBackend` or `BaseSQLBackend`.
2. **Scattered and Leaked Translator Overrides**: Even when subclassed backends exist in `third_party/ibis/ibis_<backend>/`, additional dialect-specific compiler rules are injected centrally from [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py). For example, MySQL compiler settings are set in [compiler.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql/compiler.py#L40), while [SnowflakeExprTranslator](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L488-L498) and [OracleExprTranslator](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L406-L414) registrations are partially injected from `operations.py`.
3. **Implicit Side-Effects & Import Fragility**: Functionality depends on the order of module imports. If `third_party.ibis.ibis_addon.operations` is not imported before a backend is used, critical patches (such as `BigQueryBackend.do_connect` or `PostgresType.to_ibis`) are missing.
4. **Upgrade Friction**: Monkey-patching internal methods and registries breaks easily during minor and major Ibis upgrades (e.g., the transition from Ibis 7.1.0 to 8.x/9.x SQLGlot compiler architecture).

---

## The Recommended Approach: Encapsulated Subclassing

### Recommended Architecture: The "Encapsulated Backend Package" Pattern
Standardize **all 14 backends** into self-contained, first-class backend packages located under `third_party/ibis/ibis_<backend>/`. Every backend must follow a uniform layout:

```
third_party/ibis/ibis_<backend>/
├── __init__.py    # Exports the custom Backend class (subclassing native or BaseAlchemyBackend)
├── api.py         # Exports <backend>_connect(...) connection factory function
├── compiler.py    # Defines <Backend>Compiler and <Backend>ExprTranslator with full _registry
├── datatypes.py   # Defines backend-specific type mappings (e.g., <Backend>Type(AlchemyType))
└── registry.py    # Contains SQL operation translation functions specific to this backend
```

### Justification
1. **High Cohesion & Isolation**: All logic for a given dialect (e.g., Oracle, Snowflake, BigQuery) resides exclusively within its own directory. Modifying or debugging Snowflake never risks unintended side-effects on PostgreSQL or MySQL.
2. **Elimination of Global Monkey-Patching**: Replacing runtime monkey-patches with explicit subclassing ensures that Ibis native classes remain clean. This prevents global state pollution and avoids import-order race conditions.
3. **Explicit Connection Dispatching**: [clients.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py) becomes a clean, declarative registry where every backend maps directly to its own `<backend>_connect` factory function in `third_party.ibis.ibis_<backend>.api`.
4. **Seamless Upgradability**: As Ibis evolves (e.g., deprecating `AlchemyCompiler` in favor of `SQLGlotCompiler` in Ibis 9.x), each backend's `compiler.py` and `Backend` class can be refactored or swapped out independently without touching a central monolithic operations file.
5. **Clear Role for `ibis_addon`**: [ibis_addon](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon) is repurposed to contain **only** cross-backend AST expression definitions ([ops.Value](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L157), `RawSQL`, `ToChar`, `BinaryLength`) and shared utility decorators (such as [cache_generator_results](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py#L206)).

---

## Architectural Comparison

| Dimension | Current State | Target Architecture (Encapsulated Subclassing) |
| :--- | :--- | :--- |
| **Backend Definition** | Mixed: 6 monkey-patched native backends, 8 custom subclasses | **100% Uniform**: All 14 backends subclass and encapsulate their custom behavior |
| **Translator Overrides** | Scattered between `ibis_<backend>/` and central `ibis_addon/operations.py` | **100% Localized**: All translator registries live in `ibis_<backend>/compiler.py` and `registry.py` |
| **Import Side-Effects** | Heavy: importing `operations.py` mutates global Ibis internals | **Zero**: No global monkey-patching; modules only define explicit subclasses |
| **Connection Dispatch in `clients.py`** | Mixed: calls native `ibis.<backend>.connect`, local helper functions, or third-party wrappers | **Standardized**: `CLIENT_LOOKUP` delegates to `third_party.ibis.ibis_<backend>.api.<backend>_connect` |
| **Testing Isolation** | Requires importing `operations.py` to test compiler matrix | Compilers can be instantiated and tested directly in isolation |

---

## Detailed Catalog of Required Changes

This section catalogs every required modification across all modules in the codebase.

### 1. Refactoring Core Addon Modules

#### [third_party/ibis/ibis_addon/operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py)
* **Goal**: Strip all backend-specific translator registries and global class monkey-patches.
* **Changes to Make**:
  - [ ] **Remove backend-specific translator registry assignments** (lines 381–508):
    - Remove `BigQueryExprTranslator._registry[...]`
    - Remove `ImpalaExprTranslator._registry[...]`
    - Remove `OracleExprTranslator._registry[...]`
    - Remove `PostgreSQLExprTranslator._registry[...]`
    - Remove `MsSqlExprTranslator._registry[...]`
    - Remove `MySQLExprTranslator._registry[...]`
    - Remove `RedShiftExprTranslator._registry[...]`
    - Remove `Db2ExprTranslator._registry[...]` and `Db2zOSExprTranslator._registry[...]`
    - Remove `SpannerExprTranslator._registry[...]`
    - Remove `TeradataExprTranslator._registry[...]`
    - Remove `SnowflakeExprTranslator._registry[...]`
    - Remove `SybaseExprTranslator._registry[...]`
  - [ ] **Relocate backend-specific helper functions**:
    - Move `bigquery_cast_from_binary_generate` & `extract_epoch_seconds` -> [ibis_bigquery/registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_bigquery/registry.py)
    - Move `sa_format_hashbytes_mysql`, `strftime_mysql`, `sa_cast_mysql` -> [ibis_mysql/registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql)
    - Move `sa_format_hashbytes_redshift` -> [ibis_redshift/registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_redshift)
    - Move `format_hashbytes_teradata` -> [ibis_teradata/registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_teradata/registry.py)
    - Move `sa_format_to_char`, `sa_format_binary_length`, `sa_format_random` -> shared utility or base Alchemy translator
  - [ ] **Remove global monkey-patches**:
    - Remove `BigQueryBackend.*` monkey-patches (lines 590–601).
    - Remove `BigQueryType.to_ibis` monkey-patch (lines 536–549).
    - Remove `SqlglotMySQLType` monkey-patches (lines 604–620).
    - Remove `BaseAlchemyBackend.dvt_list_tables` injection (line 379) (implement in custom `Backend` base class or individual backends).
    - Move Pandas execution patches (`compute_row_reduction`, `convert_Date`, `PandasTypeMapping`) to a dedicated [ibis_pandas](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis) initialization helper.
  - [ ] **Keep only cross-backend AST definitions**:
    - Retain class definitions for `BinaryLength`, `PaddedCharLength`, `ToChar`, `RawSQL`.
    - Retain expression method bindings on `BinaryValue.byte_length`, `StringValue.padded_char_length`, `NumericValue.to_char`, etc.
    - Retain default fallback mappings on `AlchemyExprTranslator._registry` and `ExprTranslator._registry`.

#### [third_party/ibis/ibis_addon/api.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py)
* **Goal**: Maintain shared helper utilities without dialect coupling.
* **Changes to Make**:
  - [ ] Retain [cast](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py#L52), [force_cast](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py#L100), and [cache_generator_results](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py#L206).
  - [ ] Move [dvt_handle_failed_column_type_inference](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py#L124) and [db2_type_string_length](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py#L158) into a common SQL alchemy backend mixin or the respective DB2/Oracle modules.

---

### 2. Standardizing Connection Dispatch in `clients.py`

#### [data_validation/clients.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py)
* **Goal**: Unify `CLIENT_LOOKUP` so all data sources route through explicit `<backend>_connect` entry points.
* **Changes to Make**:
  - [ ] Refactor [CLIENT_LOOKUP](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L498-L513) to uniformly map all source types to functions imported from `third_party.ibis.ibis_<backend>.api`:
    ```python
    CLIENT_LOOKUP = {
        consts.SOURCE_TYPE_BIGQUERY: bigquery_connect,
        consts.SOURCE_TYPE_IMPALA: impala_connect,
        consts.SOURCE_TYPE_MYSQL: mysql_connect,
        consts.SOURCE_TYPE_ORACLE: oracle_connect,
        consts.SOURCE_TYPE_FILESYSTEM: get_pandas_client,
        consts.SOURCE_TYPE_POSTGRES: postgres_connect,
        consts.SOURCE_TYPE_REDSHIFT: redshift_connect,
        consts.SOURCE_TYPE_TERADATA: teradata_connect,
        consts.SOURCE_TYPE_MSSQL: mssql_connect,
        consts.SOURCE_TYPE_SNOWFLAKE: snowflake_connect,
        consts.SOURCE_TYPE_SPANNER: spanner_connect,
        consts.SOURCE_TYPE_SYBASE: sybase_connect,
        consts.SOURCE_TYPE_DB2: db2_connect,
        consts.SOURCE_TYPE_DB2_ZOS: db2_zos_connect,
    }
    ```
  - [ ] Remove inline ad-hoc client connection wrappers like [impala_connect](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L489-L495) in `clients.py` and delegate connection parameter preparation to `third_party/ibis/ibis_impala/api.py`.
  - [ ] Update [get_bigquery_client](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L160-L195) to call `third_party.ibis.ibis_bigquery.api.bigquery_connect` instead of native `ibis.bigquery.connect`.

---

### 3. Backend-by-Backend Remediation

#### A. [BigQuery (third_party/ibis/ibis_bigquery)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_bigquery)
* **Current Flaw**: Subclasses `Backend(BigQueryBackend)` in [__init__.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_bigquery/__init__.py#L36), but is bypassed in `clients.py`. Instead, `operations.py` monkey-patches the native `BigQueryBackend` and `BigQueryType`.
* **Action Items**:
  - [ ] In `ibis_bigquery/compiler.py`: Create `BigQueryExprTranslator(ibis.backends.bigquery.compiler.BigQueryExprTranslator)` and `BigQueryCompiler(ibis.backends.bigquery.compiler.BigQueryCompiler)`. Register `HashBytes`, `RawSQL`, `Strftime`, `BinaryLength`, `ExtractEpochSeconds` directly on `BigQueryExprTranslator._registry`.
  - [ ] In `ibis_bigquery/datatypes.py`: Create `BigQueryType` subclass or mapping helper that handles `INTERVAL` -> `dt.Interval(unit="s")` without monkey-patching `ibis.backends.bigquery.datatypes.BigQueryType`.
  - [ ] In `ibis_bigquery/__init__.py`: Set `Backend.compiler = BigQueryCompiler`.
  - [ ] In `ibis_bigquery/api.py`: Ensure `bigquery_connect` accepts all DVT arguments (`client`, `storage_client`, etc.) and returns `Backend()`.

#### B. [Snowflake (third_party/ibis/ibis_snowflake)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_snowflake)
* **Current Flaw**: [__init__.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_snowflake/__init__.py) is empty. [datatypes.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_snowflake/datatypes.py) monkey-patches `SnowflakeType.to_ibis`, `SnowflakeBackend._metadata`, and `SnowflakeBackend.list_primary_key_columns`. [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L488-L498) updates `SnowflakeExprTranslator._registry`.
* **Action Items**:
  - [ ] In `ibis_snowflake/__init__.py`: Define `Backend(ibis.backends.snowflake.Backend)` containing the custom `_metadata` and `list_primary_key_columns` methods.
  - [ ] In `ibis_snowflake/compiler.py`: Define `SnowflakeExprTranslator` and `SnowflakeCompiler`. Initialize its `_registry` with operations from [registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_snowflake/registry.py) (`cast_snowflake`, `format_hashbytes_snowflake`, `epoch_time_snowflake`, etc.).
  - [ ] In `ibis_snowflake/datatypes.py`: Subclass `SnowflakeType` to handle `VARBINARY` and numeric precision mapping cleanly without monkey-patching native `SnowflakeType.to_ibis`.
  - [ ] In `ibis_snowflake/api.py`: Update `snowflake_connect` to instantiate and return `ibis_snowflake.Backend`.

#### C. [MySQL (third_party/ibis/ibis_mysql)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql)
* **Current Flaw**: [__init__.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql/__init__.py) monkey-patches `MySQLBackend.list_primary_key_columns`. [compiler.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql/compiler.py) monkey-patches `MySqlBackend.compiler = MySQLCompilerNew`. [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L447-L451) updates `MySQLExprTranslator._registry` and monkey-patches `SqlglotMySQLType`.
* **Action Items**:
  - [ ] In `ibis_mysql/__init__.py`: Define `Backend(ibis.backends.mysql.Backend)` with `compiler = MySQLCompilerNew` and `list_primary_key_columns`.
  - [ ] In `ibis_mysql/compiler.py`: Define `MySQLExprTranslator(ibis.backends.mysql.compiler.MySQLExprTranslator)` and register `ops.Cast`, `RawSQL`, `ops.HashBytes`, `ops.Strftime`, `BinaryLength`.
  - [ ] In `ibis_mysql/api.py`: Implement `mysql_connect(...)` returning `ibis_mysql.Backend()`.

#### D. [PostgreSQL (third_party/ibis/ibis_postgres)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_postgres)
* **Current Flaw**: [client.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_postgres/client.py#L197-L202) monkey-patches `PostgresBackend` (`_metadata`, `list_databases`, `do_connect`, `list_primary_key_columns`, `raw_column_metadata`, `is_char_type_padded`). [datatypes.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_postgres/datatypes.py#L44) monkey-patches `PostgresType.to_ibis`. [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L416-L428) updates `PostgreSQLExprTranslator._registry`.
* **Action Items**:
  - [ ] In `ibis_postgres/__init__.py`: Define `Backend(ibis.backends.postgres.Backend)` encapsulating all custom methods from `client.py`.
  - [ ] In `ibis_postgres/compiler.py`: Define `PostgreSQLExprTranslator` and `PostgreSQLCompiler` populated with operation mappings from [registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_postgres/registry.py).
  - [ ] In `ibis_postgres/datatypes.py`: Subclass `PostgresType` or use compiler type-mapper integration instead of overwriting `PostgresType.to_ibis`.
  - [ ] In `ibis_postgres/api.py`: Implement `postgres_connect(...)` returning `ibis_postgres.Backend()`.

#### E. [Microsoft SQL Server (third_party/ibis/ibis_mssql)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mssql)
* **Current Flaw**: Subclasses `Backend(BaseAlchemyBackend)` in [__init__.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mssql/__init__.py#L30), but relies on [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L431-L445) to mutate native `MsSqlExprTranslator._registry`.
* **Action Items**:
  - [ ] In `ibis_mssql/compiler.py`: Create `MsSqlExprTranslator(ibis.backends.mssql.compiler.MsSqlExprTranslator)` and `MsSqlCompiler`. Populate `_registry` with all translations from [registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mssql/registry.py).
  - [ ] In `ibis_mssql/__init__.py`: Set `Backend.compiler = MsSqlCompiler`.

#### F. [Oracle (third_party/ibis/ibis_oracle)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_oracle)
* **Current Flaw**: Well-subclassed (`Backend`, `OracleCompiler`, `OracleExprTranslator`, `OracleType`), but [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L406-L414) redundantly re-registers operations (`RawSQL`, `HashBytes`, `ToChar`, `BinaryLength`, `RStrip`, `PaddedCharLength`) on `OracleExprTranslator._registry`.
* **Action Items**:
  - [ ] In `ibis_oracle/registry.py`: Ensure all operations are present in `operation_registry`.
  - [ ] Remove `if OracleExprTranslator:` block from `operations.py`.

#### G. [Impala (third_party/ibis/ibis_impala)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_impala)
* **Current Flaw**: [api.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_impala/api.py#L243-L250) monkey-patches `ImpalaBackend` methods and global `udf.parse_type`. [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L399-L404) updates `ImpalaExprTranslator._registry`. [clients.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L489) defines a duplicate `impala_connect`.
* **Action Items**:
  - [ ] In `ibis_impala/__init__.py`: Define `Backend(ibis.backends.impala.Backend)` with overridden `get_schema`, `_get_schema_using_query`, `do_connect`, `list_primary_key_columns`, `dvt_list_tables`.
  - [ ] In `ibis_impala/compiler.py`: Define `ImpalaExprTranslator` and `ImpalaCompiler`.
  - [ ] In `ibis_impala/api.py`: Cleanly export `impala_connect(...)` and remove global monkey-patches.

#### H. [IBM Db2 & Db2 z/OS (third_party/ibis/ibis_db2, ibis_db2_zos)](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_db2)
* **Current Flaw**: Subclassed backends, but [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L460-L473) reaches into `Db2ExprTranslator._registry` and `Db2zOSExprTranslator._registry` to assign `RawSQL`, `BinaryLength`, `RStrip`, `PaddedCharLength`.
* **Action Items**:
  - [ ] Move `RawSQL`, `BinaryLength`, `RStrip`, `PaddedCharLength` into [ibis_db2/registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_db2/registry.py) and [ibis_db2_zos/registry.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_db2_zos/registry.py).
  - [ ] Remove `if Db2ExprTranslator:` and `if Db2zOSExprTranslator:` blocks from `operations.py`.

#### I. [SAP Sybase, Teradata, Cloud Spanner, Redshift](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_sybase)
* **Current Flaw**: All are subclassed backends whose compiler registries are partially mutated from [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L453-L508).
* **Action Items**:
  - [ ] In each backend's `registry.py` and `compiler.py`, ensure all dialect operations (`RawSQL`, `HashBytes`, `BinaryLength`, `PaddedCharLength`) are self-contained.
  - [ ] Remove `RedShiftExprTranslator`, `SpannerExprTranslator`, `TeradataExprTranslator`, and `SybaseExprTranslator` blocks from `operations.py`.

#### J. [Pandas / Local Filesystem](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L197)
* **Current Flaw**: [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L510-L586) monkey-patches `compute_row_reduction`, `convert_Date`, and `IBIS_TYPE_TO_PANDAS_TYPE`.
* **Action Items**:
  - [ ] Create `third_party/ibis/ibis_pandas/` (or encapsulate in `data_validation/clients.py:get_pandas_client`) to apply execution patches when initializing the Pandas backend client.

---

## Phased Implementation Checklist

Use this actionable checklist to execute the refactoring in structured phases.

### Phase 1: Core Addon & Base Isolation
- [ ] Refactor [operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py): Remove all 13 backend-specific `_registry` blocks.
- [ ] Retain AST operations (`BinaryLength`, `PaddedCharLength`, `ToChar`, `RawSQL`) and generic `AlchemyExprTranslator._registry` fallback mappings in `operations.py`.
- [ ] Move `cache_generator_results` and `force_cast` into dedicated utility locations or maintain cleanly in [api.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/api.py).

### Phase 2: Refactor Native Wrapped Backends
- [ ] **BigQuery**: Create `ibis_bigquery/compiler.py`, update `Backend(BigQueryBackend)` in `__init__.py`, and route `get_bigquery_client` in `clients.py` through `ibis_bigquery.api.bigquery_connect`.
- [ ] **Snowflake**: Define `Backend` in `ibis_snowflake/__init__.py`, create `compiler.py`, remove monkey-patches from `datatypes.py`, and update `snowflake_connect` in `api.py`.
- [ ] **MySQL**: Define `Backend` in `ibis_mysql/__init__.py`, create `compiler.py`, implement `mysql_connect` in `api.py`, and remove monkey-patches from `operations.py`.
- [ ] **PostgreSQL**: Define `Backend` in `ibis_postgres/__init__.py`, create `compiler.py`, implement `postgres_connect` in `api.py`, and remove monkey-patches from `client.py` and `datatypes.py`.
- [ ] **Impala**: Define `Backend` in `ibis_impala/__init__.py`, create `compiler.py`, export `impala_connect` in `api.py`, and eliminate monkey-patches.

### Phase 3: Clean Subclassed Backends
- [ ] **Oracle**: Ensure all operations are in `ibis_oracle/registry.py` and remove Oracle overrides from `operations.py`.
- [ ] **MSSQL**: Create `ibis_mssql/compiler.py`, attach to `Backend.compiler`, and remove MSSQL overrides from `operations.py`.
- [ ] **Db2 & Db2 z/OS**: Move operation registrations into `ibis_db2/registry.py` and `ibis_db2_zos/registry.py`.
- [ ] **Sybase, Teradata, Spanner, Redshift**: Ensure all operation registrations are completely self-contained in their respective `registry.py` / `compiler.py` files.

### Phase 4: Standardize Client Dispatching
- [ ] Update `CLIENT_LOOKUP` in [data_validation/clients.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L498-L513) to uniformly map every source type to its corresponding `third_party.ibis.ibis_<backend>.api.<backend>_connect` function.
- [ ] Remove unused backend imports and ad-hoc connection functions from `clients.py`.

### Phase 5: Verification & Test Suite
- [ ] Run compiler matrix unit tests: `pytest tests/unit/ibis_addon/test_compiler_matrix.py`.
- [ ] Run all unit tests: `pytest tests/unit/`.
- [ ] Run backend-specific system/integration test suites (e.g., `test_postgres.py`, `test_sql_server.py`, `test_bigquery.py`, `test_snowflake.py`).
