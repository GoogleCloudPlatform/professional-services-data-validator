# Pandas to DuckDB Migration & Integration Fixes

We have successfully migrated the local/in-memory file validation backend from Pandas to DuckDB and resolved all test suite failures (both unit tests and Postgres integration tests).

## Summary of Changes

### 1. Discrepancies in Table Listing Schema Names
- **Issue**: For DuckDB, calling `.inspector.get_table_names(schema="memory")` returned `[]` because `"memory"` is the catalog name. DuckDB's default schema name containing user tables is `"main"`.
- **Solution**: Excluded `"pandas"` and `"duckdb"` backends from using the custom monkeypatched `dvt_list_tables` in [data_validation/clients.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/clients.py#L326), restoring default backend inspector listings.

### 2. ExtractEpochSeconds Overflow & Int32 Casts
- **Issue**: Ibis registers the output datatype of `ops.ExtractEpochSeconds` as `int32` by default. For extreme dates (e.g. `0001-01-01` and `9999-12-31`), extracting the epoch seconds resulted in overflows, raising PyArrow/DuckDB conversion exceptions.
- **Solution**: Patched `ops.ExtractEpochSeconds.dtype = dt.int64` globally and registered a custom translation function `duckdb_sa_epoch_seconds` casting epoch values to `BIGINT` in [third_party/ibis/ibis_addon/operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py#L950-L962).

### 3. Binary-to-String Casting
- **Issue**: The SQLAlchemy Postgres dialect used by `duckdb_engine` translates binary to string casts to Postgres-specific `encode(column, 'escape')`, which is not supported by DuckDB (DuckDB only supports `encode(VARCHAR) -> BLOB` without additional arguments).
- **Solution**: Implemented a custom `duckdb_sa_cast` translator function inside [third_party/ibis/ibis_addon/operations.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_addon/operations.py#L964-L974) to translate binary-to-string casts using `lower(hex(arg))`.

### 4. Nullable Column Type Inference Discrepancies
- **Issue**: When registering a Pandas DataFrame with all-null/`None` columns, DuckDB's Python binder defaults the column types to `INTEGER`. If Ibis compiled a query believing the column was `string` (based on the memtable's sanitized schema), no cast was generated, resulting in `Conversion Error: Could not convert string 'nan' to INT32`.
- **Solution**: Replaced `client.create_table(name, memtable)` with explicit table creation using SQLAlchemy compiled DDL from the sanitized Ibis schema inside [data_validation/combiner.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/combiner.py#L176-L233). Columns are created with correct explicit types before appending the DataFrame rows using `df.to_sql(name, client.con, if_exists="append")`.

## Test Results

1. **Unit Tests**: All 400 unit tests are passing.
2. **System Integration Tests (Postgres)**: All 56 integration tests are passing.
   - Command: `PROJECT_ID=pso-kokoro-resources POSTGRES_PASSWORD=password PYTHONPATH=. pytest tests/system/data_sources/test_postgres.py`
   - Result: `56 passed, 3 skipped, 268 warnings`

## File Diff Summary

### data_validation/clients.py
```diff
@@ -324,5 +324,5 @@
 
     # Exclude pandas and duckdb from dvt_list_tables monkeypatch
-    if client.name in ["pandas"]:
+    if client.name in ["pandas", "duckdb"]:
         return client
```

### third_party/ibis/ibis_addon/operations.py
```diff
@@ -948,2 +948,27 @@
 
+# Patch ExtractEpochSeconds return type to int64 to prevent overflows for tricky dates
+ops.ExtractEpochSeconds.dtype = dt.int64
+
+# Register DuckDB translation of ExtractEpochSeconds to use EPOCH casted to BIGINT
+try:
+    from ibis.backends.duckdb.compiler import DuckDBSQLExprTranslator
+    def duckdb_sa_epoch_seconds(translator, op):
+        arg = translator.translate(op.arg)
+        return sa.cast(sa.func.epoch(arg), sa.BIGINT)
+
+    DuckDBSQLExprTranslator._registry[ops.ExtractEpochSeconds] = duckdb_sa_epoch_seconds
+
+    def duckdb_sa_cast(t, op):
+        arg = op.arg
+        typ = op.to
+        arg_dtype = arg.dtype
+
+        sa_arg = t.translate(arg)
+        if arg_dtype.is_binary() and typ.is_string():
+            return sa.func.lower(sa.func.hex(sa_arg))
+
+        return sa_fixed_cast(t, op)
+
+    DuckDBSQLExprTranslator._registry[ops.Cast] = duckdb_sa_cast
+except Exception:
+    pass
```

### data_validation/combiner.py
```diff
@@ -173,3 +173,36 @@
 
+def _create_and_populate_table(client, name: str, df: "DataFrame", schema: "ibis.Schema"):
+    import sqlalchemy as sa
+    # Map Ibis types to SQLAlchemy types
+    def get_sqla_type(dtype):
+        if dtype.is_integer():
+            return sa.BIGINT
+        elif dtype.is_string():
+            return sa.VARCHAR
+        elif dtype.is_floating():
+            return sa.DOUBLE
+        elif dtype.is_boolean():
+            return sa.BOOLEAN
+        elif dtype.is_timestamp():
+            return sa.TIMESTAMP
+        elif dtype.is_date():
+            return sa.DATE
+        elif dtype.is_binary():
+            return sa.BLOB
+        else:
+            return sa.VARCHAR
+
+    metadata = sa.MetaData()
+    columns = []
+    for col_name, dtype in schema.items():
+        columns.append(sa.Column(col_name, get_sqla_type(dtype), quote=True))
+
+    table = sa.Table(name, metadata, *columns)
+    metadata.create_all(client.con)
+
+    # Insert rows using Pandas to_sql with if_exists="append"
+    df.to_sql(name, client.con, if_exists="append", index=False, chunksize=10000)
+
+
 def _generate_report_slice(
...
@@ -199,15 +232,12 @@
             schema as the report table.
     """
-    source_mt = ibis.memtable(source_df, schema=_sanitize_schema(ibis.memtable(source_df).schema()))
-    target_mt = ibis.memtable(target_df, schema=_sanitize_schema(ibis.memtable(target_df).schema()))
+    source_schema = _sanitize_schema(ibis.memtable(source_df).schema())
+    target_schema = _sanitize_schema(ibis.memtable(target_df).schema())
 
     client = ibis.duckdb.connect()
-    client.create_table(consts.RESULT_TYPE_SOURCE, source_mt)
-    client.create_table(consts.RESULT_TYPE_TARGET, target_mt)
+    _create_and_populate_table(client, consts.RESULT_TYPE_SOURCE, source_df, source_schema)
+    _create_and_populate_table(client, consts.RESULT_TYPE_TARGET, target_df, target_schema)
     source = client.table(consts.RESULT_TYPE_SOURCE)
     target = client.table(consts.RESULT_TYPE_TARGET)
```
