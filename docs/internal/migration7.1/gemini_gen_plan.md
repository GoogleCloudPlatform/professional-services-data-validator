# DVT Migration Plan: Upgrading to Ibis 7.1.0 (Safer Intermediate Modernization)

## Executive Summary
This document outlines the modernization plan for upgrading the Data Validation Tool (DVT) from **Ibis 5.1.0** to **Ibis 7.1.0**. 

While a jump straight to **Ibis 9.0.0** was scaffolded in other work, it introduces extensive breaking changes due to a full rewrite of the compilation engine to **SQLGlot** and the removal of the older **Pandas interface** in favor of **DuckDB**.

**Ibis 7.1.0** represents a highly strategic, safer "sweet spot" for DVT's modernization:
1. **Retains Pandas Interface**: It preserves the legacy Pandas client interface which DVT uses extensively for local filesystem and memory validations.
2. **Maintains Backend Compilers**: It retains the classic `Compiler` and `ExprTranslator` class hierarchy in `ibis.backends.base.sql`, meaning DVT's custom backends (Teradata, DB2, DB2 z/OS, Sybase, Spanner) **do not require a full SQLGlot compiler rewrite** and can be updated with minimal modifications.
3. **Upgrades Supported Backends**: We retire DVT's custom copies/wrappers of native backends (BigQuery, Postgres, Impala, MSSQL, MySQL, Oracle, Snowflake) and migrate to Ibis 7.1.0's fully mature, native connection interfaces directly.

---

## Architecture Comparison: 5.1.0 vs 7.1.0 vs 9.0.0

| Feature / Architecture | Ibis 5.1.0 (Current) | Ibis 7.1.0 (Target) | Ibis 9.0.0 (Alternative) |
| :--- | :--- | :--- | :--- |
| **Pandas Interface** | Local DataFrames | Local DataFrames | Fully replaced by DuckDB |
| **Compiler Engine** | SQL Alchemy & Base SQL | SQL Alchemy & Base SQL | SQLGlot Compiler |
| **Native SQL Backends** | Partials in `third_party` | Fully Natively Supported | Fully Natively Supported |
| **Custom Compilers** | Standard `ExprTranslator` | Standard `ExprTranslator` | Rewrite `visit_<Operation>` |
| **Upgrade Risk** | None (Status Quo) | **Low / Medium** | **Extremely High** |

---

## Step-by-Step Migration Strategy

### 1. Dependency Alignment (`setup.py`)
Update `setup.py` to specify `ibis-framework==7.1.0`. The Python environment requirements are updated to Python 3.10+. Python 3.11 is ideal and fully compatible with all dependencies, including `numpy 1.x` and `pandas 2.x` prebuilt wheels.

### 2. Retire Obsolete Custom Backend Copies
In DVT's legacy setup, custom directories for native backends were kept in `third_party/ibis/` because of earlier limitations. Under Ibis 7.1.0, we leverage the native connection methods in the `CLIENT_LOOKUP` map inside `data_validation/clients.py`:
- **BigQuery**: Uses `ibis.bigquery.connect`
- **Impala**: Uses `ibis.impala.connect`
- **MySQL**: Uses `ibis.mysql.connect`
- **Postgres**: Uses `ibis.postgres.connect`
- **MSSQL**: Uses lazy/dynamic `ibis.mssql.connect`
- **Oracle**: Uses lazy/dynamic `ibis.oracle.connect`
- **Snowflake**: Uses lazy/dynamic `ibis.snowflake.connect`

*Note: Dynamic wrappers are added to prevent import-time `PackageNotFoundError` exceptions when specific database drivers are not present in the runtime environment.*

### 3. Consolidate Custom Dialect Extensions in `operations.py`
DVT extends SQL dialects to add custom operations (`ops.HashBytes`, `RawSQL`, `ToChar`, `PaddedCharLength`, `BinaryLength`). Rather than referencing registry modules inside retired `third_party/ibis/*` directories, we define these helper functions directly inside `third_party/ibis/ibis_addon/operations.py` and register them on the native Ibis translator classes.

### 4. Update Registry Mappings for Internal Ibis Renames
Ibis 7.1.0 introduced minor internal API cleanups:
- `ops.IfNull` was replaced by the more generic `ops.Coalesce` (which takes a variable-length tuple of arguments instead of two scalar fields).
- `ops.NotAny`, `ops.NotAll`, `ops.CumulativeAll`, and `ops.CumulativeAny` are removed from operations as Ibis now translates them into standard windowed `All`/`Any` nodes automatically.
- `to_sqla_type` was refactored to `AlchemyType.from_ibis` inside `ibis.backends.base.sql.alchemy.datatypes`.

We updated the registry files for the custom backends (**Teradata**, **DB2**, **DB2 z/OS**, and **Sybase**) to reflect these changes.

---

## Code Modifications (PR Summary)

### Setup & Client Connection Updates
`setup.py` was updated to pin `ibis-framework==7.1.0`.

`data_validation/clients.py` imports `spanner_connect` and `redshift_connect` from `third_party` but leverages standard native backends for all other databases. Safe dynamic loaders are added:
```python
def oracle_connect(*args, **kwargs):
    try:
        return ibis.oracle.connect(*args, **kwargs)
    except ImportError:
        raise Exception("pip install oracledb")

def snowflake_connect(*args, **kwargs):
    try:
        return ibis.snowflake.connect(*args, **kwargs)
    except ImportError:
        raise Exception("pip install snowflake-connector-python && pip install snowflake-sqlalchemy")
```

### Custom Operator Modernization
`third_party/ibis/ibis_addon/operations.py` was modernized to use class-level type annotations instead of obsolete `rlz.one_of` rules, aligning with Ibis 7.x's new pattern:
```python
class BinaryLength(ops.Value):
    arg: ops.Value[dt.Binary | dt.String]
    dtype = dt.int32
    shape = rlz.shape_like("arg")

class PaddedCharLength(ops.Value):
    arg: ops.Value[dt.String]
    dtype = dt.int32
    shape = rlz.shape_like("arg")

class ToChar(ops.Value):
    arg: ops.Value[dt.Decimal | dt.Float64 | dt.Date | dt.Time | dt.Timestamp]
    fmt: ops.Value[dt.String]
    dtype = dt.string
    shape = rlz.shape_like("arg")

class RawSQL(ops.Comparison):
    left: ops.Value[dt.String]
    right: ops.Value[dt.String]
```

### Database Dialects Registration
All translator overrides are registered directly onto the native Ibis classes:
```python
BigQueryExprTranslator._registry[ops.HashBytes] = bigquery_format_hashbytes
ImpalaExprTranslator._registry[ops.Coalesce] = impala_sa_ifnull
PostgreSQLExprTranslator._registry[ops.Cast] = postgres_sa_cast
MsSqlExprTranslator._registry[ops.Coalesce] = sa_fixed_arity(sa.func.isnull, 2)
```

---

## Verification and Results
All verification tests were executed successfully using **Python 3.11.9** with dependencies loaded cleanly from `./venv_new_libs`:

1. **Syntax and Schema Compilation**: Verified that all custom operators compile correctly into standard Ibis nodes.
2. **Operations Module Import**: Verified that `third_party.ibis.ibis_addon.operations` imports 100% cleanly under Ibis 7.1.0.
3. **DVT Entrypoint & Core Clients**: Verified that `data_validation.clients` and DVT's CLI entrypoint (`data_validation.__main__`) import cleanly without any warnings or `AttributeError`s.

### Verification Commands
```bash
# Verify Ibis Addon Operations
python3.11 scratch/test_operations_import.py
# Output: Imported Ibis version: 7.1.0 | Successfully imported third_party.ibis.ibis_addon.operations!

# Verify Clients Registry
python3.11 scratch/test_clients_import.py
# Output: Imported Ibis version: 7.1.0 | Successfully imported data_validation.clients!

# Verify DVT Main entry point
python3.11 scratch/test_dvt_import.py
# Output: Imported Ibis version: 7.1.0 | Successfully imported data_validation.__main__ and its main entry point!
```

---

## Next Steps
1. **Review Implementation Changes**: The changes have been applied to the `ibis-7.1-modernization` branch.
2. **CI Pipeline Integration**: Run full integration test suites for specialized targets (like Oracle, DB2, Snowflake) in test containers to validate runtime execution of translated queries.
3. **Merge to Mainline**: Merge the `ibis-7.1-modernization` branch into `develop`.

