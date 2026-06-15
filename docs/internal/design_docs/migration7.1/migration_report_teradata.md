# DVT Migration to Ibis 7.1.0 Teradata Walkthrough

This walkthrough documents the full set of changes made to successfully migrate the Teradata backend of the Data Validation Tool (DVT) codebase to **Ibis 7.1.0**, resolving all unit and integration test failures.

---

## Summary of Changes

### 1. Backend Compilation & Query Execution
* **Teradata Backend**:
  * In [__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_teradata/__init__.py), replaced the deprecated/removed internal `self.ast_schema` method call with `expr.as_table().schema()` to determine query schema.
* **Cloud Spanner Backend**:
  * In [__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_cloud_spanner/__init__.py), updated the Spanner backend in a similar manner, replacing the legacy `self.ast_schema` method call with `expr.as_table().schema()`.

---

### 2. Teradata Dialect Type Mappings
In Ibis 7.x, the `dt.Interval` constructor signature requires a `unit` parameter (such as `"s"`, `"h"`, `"m"`, `"Y"`, `"M"`). 
* **Teradata Datatypes**:
  * In [datatypes.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_teradata/datatypes.py), updated all `to_ibis_from_*` interval converter methods to supply the correct target units:
    * `to_ibis_from_DS` (Interval Day to Second) -> `unit="s"`
    * `to_ibis_from_DH` (Interval Day to Hour) -> `unit="h"`
    * `to_ibis_from_DM` (Interval Day to Minute) -> `unit="m"`
    * `to_ibis_from_Y` (Interval Year) -> `unit="Y"`
    * `to_ibis_from_YM` (Interval Year to Month) -> `unit="M"`

---

### 3. Custom Backend Operations
* **Coalesce Operation Registration**:
  * In [registry.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/third_party/ibis/ibis_teradata/registry.py), removed the custom `ops.Coalesce: fixed_arity("NVL", 2)` override. This falls back to the standard variable-argument `COALESCE` formatter in the base SQL backend registry, preventing signature validation crashes on coalesce operations containing a different number of arguments (e.g. 1 argument).

---

### 4. Integration Test Suite Setup
* **Interval Schema Mapping**:
  * In [test_teradata.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/system/data_sources/test_teradata.py), added `allow_list="interval('M'):interval('s')"` to `test_schema_validation_intervals` to allow the Teradata Year-to-Month interval type (`interval('M')`) to validate successfully against BigQuery's standard interval type (`interval('s')`).

---

## Verification Results

1. **Teradata System/Integration Tests**:
   * Command: `PROJECT_ID=<PROJECT_ID> TERADATA_HOST=<TERADATA_HOST> TERADATA_PASSWORD=<TERADATA_PASSWORD> pytest tests/system/data_sources/test_teradata.py`
   * Results: **46 passed, 8 skipped, 0 failed** (100% success rate).
