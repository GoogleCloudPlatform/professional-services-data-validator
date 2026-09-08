# Code Review & Refactoring Report: Review Pass on Ibis 7.1 Modernization

## Executive Summary

This report provides a detailed technical review of the five code items specified in [instructions_review_pass.md](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/docs/internal/design_docs/migration7.1/instructions_review_pass.md). Each review evaluates correctness, equivalence, readability, and compatibility with **Ibis 7.1.0** and future Ibis releases.

As requested, **no code changes have been made** in this pass. The report is structured with clear before/after code diffs and an actionable implementation checklist to make executing the changes straightforward.

---

## Item 1: `RawSQL` Definition in `develop` vs. `ibis-7.1-modernization`

### Context & Comparison
* **`develop` branch**:
  ```python
  class RawSQL(ops.Comparison):
      pass
  ```
* **`ibis-7.1-modernization` branch** ([operations.py:L176-L178](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L176-L178)):
  ```python
  class RawSQL(ops.Comparison):
      left: ops.Value[dt.String]
      right: ops.Value[dt.String]
  ```

### Analysis & Technical Evaluation
1. **Base Class Inheritance**: In Ibis 7.1.0, `ops.Comparison` subclasses `ops.Binary` (`ibis.expr.operations.core.Binary`), which already declares the field type annotations:
   ```python
   left: Value
   right: Value
   ```
2. **Equivalence**: Both definitions result in the exact same runtime AST node structure. The translator functions ([format_raw_sql](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L248) and [sa_format_raw_sql](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L253)) access `op.args` directly (`rand_col, raw_sql = op.args`), so the explicit string type annotations do not alter compilation.
3. **Over-Constraining Types**: Explicitly typing `left: ops.Value[dt.String]` restricts the left operand strictly to string expressions, whereas `develop` allows comparing or wrapping arbitrary column expressions.

### Opinion & Recommendation
**Strongly Agree with `develop`**: The definition in the `develop` branch (`class RawSQL(ops.Comparison): pass`) is cleaner, less verbose, and leverages Ibis's built-in `ops.Binary` field definitions without redundant re-declarations.

#### Suggested Change
```diff
--- a/third_party/ibis/ibis_addon/operations.py
+++ b/third_party/ibis/ibis_addon/operations.py
@@ -174,8 +174,7 @@ class ToChar(ops.Value):
 
-class RawSQL(ops.Comparison):
-    left: ops.Value[dt.String]
-    right: ops.Value[dt.String]
+class RawSQL(ops.Comparison):
+    pass
```

---

## Item 2: Backend-Specific Imports and Try/Catch Blocks in `operations.py` & Dedicated Pandas Backend

### Context
[operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py) contains 5 backend-specific try/catch blocks at the bottom of the file (lines 510–620):
1. **Lines 510–532**: Monkey-patches `ibis.backends.pandas.execution.generic.compute_row_reduction` to fix handling of scalar strings/bytes/dicts.
2. **Lines 535–549**: Monkey-patches `ibis.backends.bigquery.datatypes.BigQueryType.to_ibis` for `INTERVAL` types.
3. **Lines 552–587**: Monkey-patches `ibis.formats.pandas.PandasData.convert_Date` to handle out-of-bounds dates.
4. **Lines 590–601**: Monkey-patches `ibis.backends.bigquery.Backend` methods (`do_connect`, `_cursor_to_arrow`, `_parse_project_and_dataset`, `list_primary_key_columns`, `dvt_list_tables`).
5. **Lines 604–620**: Monkey-patches `ibis.backends.mysql.datatypes.SqlglotMySQLType` (`_from_sqlglot_DATETIME` and `_TIMESTAMP`).

### Analysis & Technical Evaluation
* **Encapsulation Violation**: `operations.py` in `ibis_addon` is intended for common, cross-backend operation definitions. Injecting global runtime monkey-patches into native Ibis classes upon importing `operations.py` creates hidden side-effects and import-order dependencies.
* **Backend Relocation**:
  * The BigQuery patches belong in [third_party/ibis/ibis_bigquery/](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_bigquery).
  * The MySQL patches belong in [third_party/ibis/ibis_mysql/](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_mysql).
* **Dedicated Pandas Backend (`third_party/ibis/ibis_pandas/`)**:
  * Currently, Pandas patches ([compute_row_reduction](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L518), [convert_Date](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L560), and [PandasTypeMapping](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L102-L115)) are scattered in `operations.py`.
  * Creating a dedicated `third_party/ibis/ibis_pandas/` package brings Pandas/FileSystem into 100% alignment with the rest of DVT's backend architecture.

### Opinion & Recommendation
**Strongly Agree**: Moving all 5 blocks to their respective backends and creating `third_party/ibis/ibis_pandas/` is the ideal architectural decision. It completely decouples `ibis_addon` from individual backend quirks.

#### Proposed Directory Structure for Pandas
```
third_party/ibis/ibis_pandas/
├── __init__.py      # Imports execution patches
├── api.py           # Defines get_pandas_client() or pandas_connect()
└── execution.py     # Contains compute_row_reduction, convert_Date, and PandasTypeMapping
```

---

## Item 3: Impala UDF Lines in `clients.py` vs. `third_party/ibis/ibis_impala/api.py`

### Context & Comparison
* **[data_validation/clients.py:L27-L32](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L27-L32)**:
  ```python
  try:
      import ibis.backends.impala.udf as impala_udf

      impala_udf._impala_to_ibis_type["binary"] = "binary"
  except ImportError:
      pass
  ```
* **[third_party/ibis/ibis_impala/api.py:L76-L80, L243](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_impala/api.py#L76-L80)**:
  ```python
  def parse_type(t):
      """Returns the Ibis datatype from source type."""
      t = t.lower()
      if t in udf._impala_to_ibis_type:
          return udf._impala_to_ibis_type[t]
      ...

  udf.parse_type = parse_type
  ```

### Analysis & Technical Evaluation
1. **Modularity**: `data_validation/clients.py` is a top-level client factory. Mutating `impala_udf._impala_to_ibis_type` at the top level of `clients.py` introduces global side-effects during initial module import.
2. **Redundancy**: `third_party/ibis/ibis_impala/api.py` is the dedicated customization module for Impala. It already monkey-patches `udf.parse_type`.
3. **Correct Home**: If Impala UDF type translation requires `"binary": "binary"`, that dictionary modification belongs inside `third_party/ibis/ibis_impala/api.py` where all other Impala UDF and connection customizations live.

### Opinion & Recommendation
**Agree**: The 6 lines in `clients.py` are unnecessary there and should be removed from `clients.py` and consolidated into `third_party/ibis/ibis_impala/api.py`.

#### Suggested Change
```diff
--- a/data_validation/clients.py
+++ b/data_validation/clients.py
@@ -24,12 +24,6 @@ from google.api_core import client_options
 import ibis
 
-try:
-    import ibis.backends.impala.udf as impala_udf
-
-    impala_udf._impala_to_ibis_type["binary"] = "binary"
-except ImportError:
-    pass
 import pandas

--- a/third_party/ibis/ibis_impala/api.py
+++ b/third_party/ibis/ibis_impala/api.py
@@ -240,6 +240,7 @@ def _dvt_list_tables(self, like=None, database=None):
     return self.list_tables(like=like, database=database)
 
+udf._impala_to_ibis_type["binary"] = "binary"
 udf.parse_type = parse_type
```

---

## Item 4: `list_schemas` in `clients.py` and Ibis 7.1 Terminology

### Context & Question
The review question asks whether `list_schemas` in [clients.py:L334-L349](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L334-L349) should remove references to the `list_schemas` attribute under the assumption that Ibis 7.1 replaced `schema` with `catalog` and `database`.

### Verification against Ibis 7.1.0 Codebase
We inspected the active Ibis 7.1.0 codebase and docstrings across backends (BigQuery, PostgreSQL, MySQL, MSSQL):

1. **`list_schemas()` is the Modern Ibis 7.1+ Standard**:
   * In Ibis 5.x and earlier, backends provided `list_databases()`.
   * Starting in **Ibis 6.1.0**, Ibis standardized on `list_schemas()` and explicitly **deprecated** `list_databases()`.
   * The docstring for `BigQueryBackend.list_databases` in Ibis 7.1.0 states:
     > `DEPRECATED: Backend.list_databases is deprecated as of v6.1.0, removed in v8.0.0; use list_schemas()`
2. **Runtime Deprecation Warnings**:
   * Calling `client.list_databases()` on Ibis 7.1 emits a `FutureWarning`:
     `FutureWarning: Backend.list_databases is deprecated as of v6.1.0, removed in v8.0.0; use list_schemas()`

### Analysis of `clients.py:list_schemas(client)`
The current implementation in `ibis-7.1-modernization`:
```python
def list_schemas(client):
    """Return a list of schemas in the DB."""
    if hasattr(client, "list_schemas"):
        try:
            return client.list_schemas()
        except NotImplementedError:
            pass

    if hasattr(client, "list_databases"):
        try:
            return client.list_databases()
        except NotImplementedError:
            return [None]
    else:
        return [None]
```

### Opinion & Recommendation
**Do NOT remove `list_schemas`**: Removing `hasattr(client, "list_schemas")` would force DVT to call deprecated `list_databases()` methods, causing `FutureWarning` logs in Ibis 7.1 and fatal `AttributeError` failures when upgrading to Ibis 8.0/9.0 where `list_databases` is removed.

The existing implementation in `clients.py` (which checks `list_schemas` first, and falls back to `list_databases` only for legacy/custom backends) is **correct, future-proof, and should be kept as-is**.

---

## Item 5: `impala_connect` in `clients.py` vs. `third_party/ibis/ibis_impala/api.py` & `unique.column.names=false`

### Context & Question
In [clients.py:L489-L495](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L489-L495):
```python
def impala_connect(*args, **kwargs):
    client = ibis.impala.connect(*args, **kwargs)
    try:
        client.raw_sql("set hive.resultset.use.unique.column.names=false")
    except Exception:
        pass
    return client
```
The review asks to verify why `set hive.resultset.use.unique.column.names=false` was introduced, and whether `impala_connect` in `clients.py` is a duplicate of [ibis_impala/api.py:L252](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_impala/api.py#L252).

### Technical Verification & Git History
1. **Git Origin**: This was introduced in commit `5b8359e` (*"Resolved FutureWarnings and support FileSystem and Hive backends"*).
2. **Why `hive.resultset.use.unique.column.names=false` is Necessary**:
   * In Hive and Impala (via HiveServer2/Thrift), query results by default prefix column headers with table names (e.g. `table_name.col_name` instead of `col_name`).
   * In Ibis 7.1.0, schema validation and DataFrame column lookups require exact column name matches. Without disabling unique column names in Hive, queries return qualified column names, breaking table validation comparisons against other databases.
   * Running `set hive.resultset.use.unique.column.names=false` upon connection forces Hive/Impala to return plain, unqualified column names (`col_name`).
3. **Duplication Problem**:
   * `data_validation/clients.py` defines its own local `impala_connect` function.
   * `third_party/ibis/ibis_impala/api.py` also defines `impala_connect(...)`.
   * This splits connection configuration across two files and leaves `ibis_impala/api.py` out of sync with `clients.py`.

### Opinion & Recommendation
**Verified**: The `unique.column.names=false` SQL setting is valid and required for Hive/Impala compatibility.
However, defining `impala_connect` inline in `clients.py` is redundant. The setting should be moved inside `third_party/ibis/ibis_impala/api.py:impala_connect`, and `clients.py` should import and use `third_party.ibis.ibis_impala.api.impala_connect`.

#### Suggested Change
```diff
--- a/third_party/ibis/ibis_impala/api.py
+++ b/third_party/ibis/ibis_impala/api.py
@@ -282,4 +282,8 @@ def impala_connect(
         http_path=http_path,
     )
+    try:
+        backend.raw_sql("set hive.resultset.use.unique.column.names=false")
+    except Exception:
+        pass
     return backend

--- a/data_validation/clients.py
+++ b/data_validation/clients.py
@@ -37,4 +37,5 @@ from data_validation.secret_manager import SecretManagerBuilder
 
 from third_party.ibis.ibis_cloud_spanner.api import spanner_connect
+from third_party.ibis.ibis_impala.api import impala_connect
 from third_party.ibis.ibis_redshift.api import redshift_connect
@@ -487,12 +488,4 @@ def get_max_in_list_size(client, in_list_over_expressions=False):
 
-def impala_connect(*args, **kwargs):
-    client = ibis.impala.connect(*args, **kwargs)
-    try:
-        client.raw_sql("set hive.resultset.use.unique.column.names=false")
-    except Exception:
-        pass
-    return client
-
-
 CLIENT_LOOKUP = {
```

---

## Actionable Implementation Checklist

Use this checklist when executing the refactoring changes:

### Item 1: Simplify `RawSQL`
- [ ] In [third_party/ibis/ibis_addon/operations.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L176), revert `class RawSQL(ops.Comparison):` to use `pass` instead of explicit `left`/`right` annotations.

### Item 2: Relocate Backend-Specific Try/Catch Blocks & Create `ibis_pandas`
- [ ] Move BigQuery `INTERVAL` patch ([L535-L549](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L535-L549)) to `third_party/ibis/ibis_bigquery/datatypes.py`.
- [ ] Move BigQuery backend method injections ([L590-L601](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L590-L601)) to `third_party/ibis/ibis_bigquery/__init__.py`.
- [ ] Move MySQL `SqlglotMySQLType` patch ([L604-L620](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/third_party/ibis/ibis_addon/operations.py#L604-L620)) to `third_party/ibis/ibis_mysql/datatypes.py`.
- [ ] Create `third_party/ibis/ibis_pandas/` package and move Pandas execution patches (`compute_row_reduction`, `convert_Date`, and `PandasTypeMapping`) there.

### Item 3: Clean Impala UDF Imports in `clients.py`
- [ ] Remove `impala_udf._impala_to_ibis_type["binary"] = "binary"` from [data_validation/clients.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L27-L32).
- [ ] Add `udf._impala_to_ibis_type["binary"] = "binary"` inside `third_party/ibis/ibis_impala/api.py`.

### Item 4: Retain `list_schemas` in `clients.py`
- [ ] Keep [clients.py:list_schemas](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L334-L349) unchanged as `list_schemas()` is the active Ibis 7.1+ standard.

### Item 5: Consolidate `impala_connect`
- [ ] Add `client.raw_sql("set hive.resultset.use.unique.column.names=false")` into `third_party/ibis/ibis_impala/api.py:impala_connect`.
- [ ] Remove duplicate `impala_connect` definition from [data_validation/clients.py](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/19a2827e14053773288690aabb1b14acf0252911/data_validation/clients.py#L489-L495).
- [ ] Import `impala_connect` from `third_party.ibis.ibis_impala.api` in `clients.py`.
