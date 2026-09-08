# DVT Migration to Ibis 7.1.0 Walkthrough

This walkthrough documents the changes made to successfully migrate the Data Validation Tool (DVT) codebase to **Ibis 7.1.0**, resolving all unit test failures.

---

## Summary of Changes

### 1. Custom Backend Operations & Rewrites
* **Oracle Registry**:
  * In [registry.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/registry.py#L470-L482), mapped `ops.Coalesce` instead of `ops.IfNull` (which was removed in Ibis 7.x) and removed the obsolete `ops.NotAny` and `ops.NotAll` mappings.
* **Impala Rewrites**:
  * In [api.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_impala/api.py#L198-L205), removed the obsolete `@rewrites(ops.IfNull)` wrapper function since `ops.IfNull` is no longer generated.

---

### 2. SQLAlchemy Custom Dialect Type Mappings
In Ibis 7.1, `dt.dtype` is locked against external registrations using `del dtype.register` at startup. This made backend-specific decorators like `@dt.dtype.register(OracleDialect_oracledb, ...)` raise `AttributeError`.

* **Oracle Datatypes**:
  * Created the `OracleType(AlchemyType)` class in [datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/datatypes.py#L23-L65) to cleanly define type mapping via `to_ibis` and `from_ibis` methods.
  * In [compiler.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_oracle/compiler.py#L25), registered `OracleType` as the `type_mapper` for `OracleExprTranslator`.
* **PostgreSQL Datatypes**:
  * Replaced `@dt.dtype.register` registrations in [datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_postgres/datatypes.py#L23-L44) by monkey-patching `PostgresType.to_ibis` dynamically.

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

---

## Verification Results
Ran `pytest tests/unit/` and verified that all **394 tests passed successfully**!
