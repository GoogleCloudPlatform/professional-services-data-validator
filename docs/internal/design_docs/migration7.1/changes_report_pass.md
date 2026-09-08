# Source Code Changes Report: Review Pass on Ibis 7.1 Modernization

## Executive Summary

This report documents the source code changes executed in accordance with the recommendations outlined in [review_report_pass.md](file:///usr/local/google/home/mudupalli/professional-services-data-validator/docs/internal/design_docs/migration7.1/review_report_pass.md), incorporating the user-requested exception for **Item #4** (prioritizing the `client.list_databases()` method provided by Ibis 7.1).

All modifications have been implemented and verified against the test suite (`pytest tests/unit/`), passing **443 unit tests** (with 6 skipped tests).

---

## Detailed Summary of Changes

### Item 1: `RawSQL` Definition Simplification
* **File Modified**: [third_party/ibis/ibis_addon/operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py#L176-L177)
* **Rationale**: Reverted `RawSQL` definition from explicit string type annotations (`left: ops.Value[dt.String]`, `right: ops.Value[dt.String]`) back to `pass`. In Ibis 7.1.0, `ops.Comparison` inherits from `ops.Binary`, which already defines `left: Value` and `right: Value`. Removing the explicit annotations eliminates unnecessary type over-constraints while preserving full AST compiler compatibility.
* **Diff**:
```diff
--- a/third_party/ibis/ibis_addon/operations.py
+++ b/third_party/ibis/ibis_addon/operations.py
@@ -174,8 +174,7 @@ class ToChar(ops.Value):
 
 
 class RawSQL(ops.Comparison):
-    left: ops.Value[dt.String]
-    right: ops.Value[dt.String]
+    pass
```

---

### Item 2: Backend Try/Catch Block Relocation & Dedicated `ibis_pandas` Package
* **Files Modified/Created**:
  * [third_party/ibis/ibis_addon/operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py#L505-L508)
  * [third_party/ibis/ibis_bigquery/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_bigquery/__init__.py#L152-L180)
  * [third_party/ibis/ibis_mysql/datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_mysql/datatypes.py) *(New)*
  * [third_party/ibis/ibis_mysql/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_mysql/__init__.py#L15-L17)
  * [third_party/ibis/ibis_pandas/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_pandas/__init__.py) *(New)*
  * [third_party/ibis/ibis_pandas/execution.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_pandas/execution.py) *(New)*
* **Rationale**: Removed 5 backend-specific monkey-patch blocks from the bottom of `operations.py` and relocated them to their respective backend packages:
  1. **BigQuery `INTERVAL` & Backend methods**: Relocated `BigQueryType.to_ibis` monkey-patch and `BigQueryBackend` method attachments into `third_party/ibis/ibis_bigquery/__init__.py`.
  2. **MySQL Datatype Patches**: Created `third_party/ibis/ibis_mysql/datatypes.py` containing `SqlglotMySQLType._from_sqlglot_DATETIME` and `_TIMESTAMP` patches and imported it in `third_party/ibis/ibis_mysql/__init__.py`.
  3. **Dedicated Pandas Backend Package**: Created `third_party/ibis/ibis_pandas/` (`__init__.py` and `execution.py`) containing `PandasTypeMapping`, `compute_row_reduction`, and `convert_Date` patches, and imported `third_party.ibis.ibis_pandas` in `operations.py`.

---

### Item 3: Relocating Impala UDF Type Mapping
* **Files Modified**:
  * [data_validation/clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py#L27-L32)
  * [third_party/ibis/ibis_impala/api.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_impala/api.py#L243)
* **Rationale**: Removed the top-level `impala_udf._impala_to_ibis_type["binary"] = "binary"` try/catch block from `clients.py` and consolidated it inside `third_party/ibis/ibis_impala/api.py` alongside other Impala UDF customizations (`udf.parse_type`).
* **Diff**:
```diff
--- a/data_validation/clients.py
+++ b/data_validation/clients.py
@@ -27,6 +27,0 @@
-try:
-    import ibis.backends.impala.udf as impala_udf
-
-    impala_udf._impala_to_ibis_type["binary"] = "binary"
-except ImportError:
-    pass

--- a/third_party/ibis/ibis_impala/api.py
+++ b/third_party/ibis/ibis_impala/api.py
@@ -242,2 +242,3 @@
+udf._impala_to_ibis_type["binary"] = "binary"
 udf.parse_type = parse_type
```

---

### Item 4: `list_databases(client)` Calling `client.list_databases()`
* **File Modified**: [data_validation/clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py#L328-L347)
* **Rationale**: Per the user requirement, `list_databases(client)` checks for `hasattr(client, "list_databases")` first and invokes `client.list_databases()`, falling back to `client.list_schemas()` if `list_databases` is not present or raises `NotImplementedError`.
* **Diff**:
```diff
--- a/data_validation/clients.py
+++ b/data_validation/clients.py
@@ -328,12 +328,12 @@ def list_databases(client):
     """Return a list of databases in the DB.
        In version 7.1, Ibis adopted a uniform way of referring
        to a collection of tables as a database, irrespective of the terminology used by the specific backend.
        Here we want the collection of tables that may be used for validation, hence the changing
        the function name to list_databases()."""
+    if hasattr(client, "list_databases"):
+        try:
+            return client.list_databases()
+        except NotImplementedError:
+            pass
+
     if hasattr(client, "list_schemas"):
         try:
             return client.list_schemas()
         except NotImplementedError:
-            pass
-
-    if hasattr(client, "list_databases"):
-        try:
-            return client.list_databases()
-        except NotImplementedError:
             return [None]
     else:
         return [None]
```

---

### Item 5: Consolidating `impala_connect` and `unique.column.names=false`
* **Files Modified**:
  * [third_party/ibis/ibis_impala/api.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_impala/api.py#L282-L287)
  * [data_validation/clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py#L37,L488-L498)
* **Rationale**: Moved the `set hive.resultset.use.unique.column.names=false` raw SQL initialization directly into `impala_connect` within `third_party/ibis/ibis_impala/api.py`. Removed the duplicate inline `impala_connect` function definition in `data_validation/clients.py`, importing `impala_connect` directly from `third_party.ibis.ibis_impala.api`.
* **Diff**:
```diff
--- a/third_party/ibis/ibis_impala/api.py
+++ b/third_party/ibis/ibis_impala/api.py
@@ -282,2 +283,6 @@ def impala_connect(
     )
+    try:
+        backend.raw_sql("set hive.resultset.use.unique.column.names=false")
+    except Exception:
+        pass
     return backend

--- a/data_validation/clients.py
+++ b/data_validation/clients.py
@@ -37,2 +37,3 @@ from data_validation.secret_manager import SecretManagerBuilder
 from third_party.ibis.ibis_cloud_spanner.api import spanner_connect
+from third_party.ibis.ibis_impala.api import impala_connect
 from third_party.ibis.ibis_redshift.api import redshift_connect
@@ -487,9 +488,0 @@ def get_max_in_list_size(client, in_list_over_expressions=False):
-def impala_connect(*args, **kwargs):
-    client = ibis.impala.connect(*args, **kwargs)
-    try:
-        client.raw_sql("set hive.resultset.use.unique.column.names=false")
-    except Exception:
-        pass
-    return client
-
```

---

## File Change Matrix

| File Path | Status | Description |
| :--- | :--- | :--- |
| [third_party/ibis/ibis_addon/operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py) | **Modified** | Simplified `RawSQL` to `pass`; removed 5 backend-specific try/catch blocks; imported `third_party.ibis.ibis_pandas`. |
| [third_party/ibis/ibis_bigquery/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_bigquery/__init__.py) | **Modified** | Relocated BigQuery `INTERVAL` datatype patch and `BigQueryBackend` method attachments. |
| [third_party/ibis/ibis_mysql/datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_mysql/datatypes.py) | **Created** | Created module encapsulating `SqlglotMySQLType` datetime/timestamp patches. |
| [third_party/ibis/ibis_mysql/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_mysql/__init__.py) | **Modified** | Imported `third_party.ibis.ibis_mysql.datatypes`. |
| [third_party/ibis/ibis_pandas/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_pandas/__init__.py) | **Created** | Package initialization for Pandas execution patches. |
| [third_party/ibis/ibis_pandas/execution.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_pandas/execution.py) | **Created** | Defines `PandasTypeMapping`, `compute_row_reduction`, and `convert_Date` patches. |
| [third_party/ibis/ibis_impala/api.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_impala/api.py) | **Modified** | Added `udf._impala_to_ibis_type["binary"] = "binary"` and `unique.column.names=false` setting to `impala_connect`. |
| [data_validation/clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py) | **Modified** | Removed top-level Impala UDF import; prioritized `client.list_databases()` in `list_databases(client)`; removed duplicate `impala_connect` and imported it from `third_party.ibis.ibis_impala.api`. |

---

## Test Verification

* **Unit Test Suite**: `pytest tests/unit/`
* **Result**: **443 passed**, 6 skipped in ~27 seconds.
