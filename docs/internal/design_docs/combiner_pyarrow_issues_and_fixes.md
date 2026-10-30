# Combiner PyArrow Conversion: Issues and Fixes

## Background

Branch `issue-1702-using-duckdb` replaces the Ibis/Pandas based combiner with one that works natively
on PyArrow tables (see [combiner_review.md](combiner_review.md)). Commit `2d4282c7` rewrote
[combiner.py](../../../data_validation/combiner.py) around a new `join_and_unpivot()` function. Testing
that version against BigQuery and Oracle system tests turned up a number of problems, both in the
combiner and in how results are fetched into PyArrow.

This document records each problem investigated, its root cause, and the fix applied (or the
proposed fix for problems still open).

### Data flow

Validation query results no longer pass through Pandas before the combiner:

| Backend | Path into PyArrow | Final cast |
|---|---|---|
| BigQuery | Ibis `BigQueryBackend.to_pyarrow` → `QueryJob.result().to_arrow()` (Storage API/REST), Arrow native throughout | `PyArrowData.convert_table` → `table.cast(ibis_schema)` (safe cast) |
| SQLAlchemy backends (Oracle, PostgreSQL, SQL Server, ...) | DB-API cursor `fetchmany()` → Python tuples → DVT patched `_sql_to_pyarrow_batches` builds each column with `pa.array(col)` | DVT patched `_base_to_pyarrow` casts to the Ibis schema with fallbacks |

Both patched functions live in [operations.py](../../../third_party/ibis/ibis_addon/operations.py).
Pandas is only used at the end of `generate_report()` to convert the finished report table.

### Commits

| Commit | Summary |
|---|---|
| `bb103562` | fix: Remove the Pandas overflow workaround of cast to string (problems 1, 2, 6) |
| `177dec40` | fix: Use decimal256 for numeric joins in combiner (problems 3, 4, 5, 7) |
| `057f73d5` | fix: Align numeric join key types in combiner and restore integer decimals Oracle returns as floats (problems 8, 9) |

## Summary

| # | Problem | Symptom | Status |
|---|---|---|---|
| 1 | String cast aggregates reach the combiner | `ValueError: Aggregate value of sum__col_int8 must be numeric` | Fixed |
| 2 | Comparing high precision values via float64 | False positive success (issue-1007) | Fixed |
| 3 | Arrow does not promote decimal128 to decimal256 | `ArrowInvalid: Decimal precision out of range [1, 38]: 45` | Fixed |
| 4 | Exact comparison applied to `avg`/`std` | False failure on Oracle `std` vs BigQuery `std` | Fixed |
| 5 | Unsafe decimal cast in `_base_to_pyarrow` | Silently corrupted Oracle `SUM(NUMBER(38))` | Fixed |
| 6 | NULL group-by keys do not join | `assert 1612 == 1611` | Fixed |
| 7 | `__dvt_fake_pk` length / more than one row | `ArrowInvalid: Added column's length must match table's length` | Fixed |
| 8 | Oracle integer keys returned as floats | `ArrowInvalid: Incompatible data types for corresponding join field keys` | Fixed |
| 9 | Join keys with different numeric types | Same as 8, `decimal128(5, 0)` vs `int64` | Fixed |
| 10 | NULL in both source and target marked as fail | Row/column validations fail on NULL/NaN values | **Open (parked)** |
| 11 | Floating point noise in `std`/`avg` | Oracle `7.07e-13` vs BigQuery `0.0` fails | **Open (parked)** |
| 12 | String aggregates rejected by the combiner | `ValueError: Aggregate value of min__cast_uuid_string__col_uuid must be numeric` | **Open** |
| 13 | Spanner backend cannot fetch results into PyArrow | `AttributeError: 'SpannerCursor' object has no attribute 'fetchmany'` | **Open** |

## System test results

| Suite | Result | Failures |
|---|---|---|
| BigQuery (`test_bigquery.py`) | All pass except 1 | #10 |
| Oracle column validations (`test_oracle.py -k "column_validation and not postgres"`) | 14 passed, 2 skipped, 2 failed | #10, #11 |
| Oracle to PostgreSQL (`test_oracle.py -k postgres`) | 9 passed, 1 skipped, 8 failed | #10 (6 tests), #12 (2 tests) |
| PostgreSQL (`test_postgres.py --no-cloud-sql`) | 55 passed, 3 skipped, 4 failed | #10 (3 tests), #12 (1 test) |
| Spanner (`test_spanner.py`) | 5 passed, 14 failed | #13 (all 14 tests) |

Oracle to PostgreSQL failures in detail:

| Test | Problem |
|---|---|
| `test_column_validation_oracle_to_postgres` | #12, `max` of a UUID column is a string aggregate |
| `test_column_validation_uuid_oracle_to_postgres` | #12, `min` of a UUID primary key is a string aggregate |
| `test_column_validation_all_null_oracle_to_postgres` | #10, 101 validations where every aggregate is NULL on both sides |
| `test_row_validation_comp_fields_oracle_to_postgres` | #10, comparison fields for the all-NULL row |
| `test_custom_query_row_validation_blob_to_postgres` | #10, `col_blob` NULL on both sides |
| `test_custom_query_row_validation_clob_to_postgres` | #10, `col_clob` NULL on both sides |
| `test_custom_query_row_validation_blob_fail_to_postgres` | #10, `assert 3 == 2`: the 2 expected mismatches plus a NULL row |
| `test_custom_query_row_validation_clob_fail_to_postgres` | #10, `assert 6 == 4`: the 4 expected mismatches plus 2 NULL rows |

PostgreSQL failures in detail:

| Test | Problem |
|---|---|
| `test_column_validation_pg_types` | #12, `min`/`max` of a UUID column is a string aggregate |
| `test_column_validation_tricky_dates_to_bigquery` | #10, 18 validations for the all-NULL row are marked fail |
| `test_row_validation_comp_fields_reserved_words` | #10, 8 comparison fields NULL on both sides |
| `test_row_validation_comp_fields_tricky_dates_to_bigquery` | #10, 6 comparison fields NULL on both sides |

No PostgreSQL or Oracle to PostgreSQL failures were caused by the fixes in this document, and no
PostgreSQL YAML line count expectations needed updating for problem 1.

Every Spanner test that runs a validation query fails with problem 13 before the combiner is reached,
so Spanner currently gives no coverage of the combiner. Once 13 is fixed the Spanner suite should be
rerun, it may expose further combiner issues.

---

## 1. String cast aggregates reach the combiner

**Tests:** `test_column_validation_core_types`, `test_custom_query_validation_core_types` (BigQuery).

**Symptom:**

```text
ValueError: Aggregate value of sum__col_int8 must be numeric
ValueError: Aggregate value of count__col_dec_20 must be numeric
ValueError: Aggregate value of sum__epoch_seconds__cast_timestamp__col_date must be numeric
```

**Root cause:** `ConfigManager` added `cast: string` to some aggregates to stop values overflowing
int64/float64 in a Pandas DataFrame. The new combiner rejects string aggregates. There were two sources
of the cast in [config_manager.py](../../../data_validation/config_manager.py):

1. `_decimal_column_too_big_for_pandas()` (used by `build_config_column_aggregates()`): any `Int64`
   column for `sum` (all BigQuery integer types are `Int64`) and any `Decimal` with precision > 18
   (less a margin for `sum`) for `count`/`min`/`max`/`sum`. Introduced by PRs #918, #1395 and #1452.
2. `append_pre_agg_calc_field()`: `sum` of `epoch_seconds` for timestamp/date columns (issue-1391).

**Investigation:** An experiment with the cast removed showed BigQuery results arriving in PyArrow
exactly, with no Pandas involved:

| BigQuery type | Arrow type for sum/min/max |
|---|---|
| `INT64`/`INT`/`SMALLINT`/`TINYINT` | `int64` (BigQuery errors on `SUM` overflow, it cannot wrap silently) |
| `NUMERIC(p,s)` | `decimal128(38, 9)` |
| `BIGNUMERIC(p,s)` | `decimal256(76, 38)` |
| `FLOAT64` | `double` |

The cast was always applied to the *result* of the aggregate in SQL, so it never protected against
overflow inside the database engine either.

An initial idea of treating all `pa.string()` columns as numeric in the combiner was rejected.

**Fix (`bb103562`):**

- `build_config_column_aggregates()` only applies `cast: float64` for `std`/`avg`. Other aggregates are
  no longer cast to string.
- The `final_cast_type = "string"` logic for `sum(epoch_seconds)` was removed from
  `append_pre_agg_calc_field()`.
- Expected YAML line counts in [test_bigquery.py](../../../tests/system/data_sources/test_bigquery.py)
  were reduced by one per removed `cast: string` line:
  - `EXPECTED_NUM_YAML_LINES_WILDCARD`: 162 → 155
  - `EXPECTED_NUM_YAML_LINES_TIMESTAMP_SUM_AVG_BITXOR`: 54 → 53
  - `EXPECTED_NUM_YAML_LINES_BQ_DATETIME_SUM_AVG_BITXOR`: 54 → 53
- `_decimal_column_too_big_for_pandas()` was renamed to `_is_high_precision_decimal()`, dropping the
  now-unused `margin` parameter and `dt.Int64` branches. It is still used by
  `_key_column_needs_casting_to_string()` for primary key/group-by columns and for `std`/`avg` float64
  casts.

## 2. Comparing high precision values via float64

**Test:** `test_column_validation_large_decimals_to_bigquery_mismatch` (regression test for
issue-1007).

**Root cause:** Removing the string cast meant high precision values reached the numeric branch of
`join_and_unpivot()`, which casts both sides to `float64` before subtracting. float64 holds roughly
15-17 significant digits, so values that differ only beyond that compare equal:

| Source | Target | difference | status (before fix) |
|---|---|---|---|
| `4938271605061728382` | `4938271605061728390` | `0.0` | success ❌ |

This affects `int64` as well as decimals.

**Fix (`bb103562`):** When `validation.threshold` is 0 and both sides are integer or decimal
(`_is_exact_type()`), success is decided by an exact comparison of the native values. float64 is
still used for the `difference`/`pct_difference` report columns and for thresholds above 0. Dates and
timestamps qualify because they are already converted to `int64` epoch seconds.

Values that differ only beyond float64 precision will show `difference = 0.0` alongside a `fail`
status, because the report columns are float64.

A unit test `test_generate_report_with_high_precision_aggs` covers `decimal128`, `int64` and
`decimal256` values.

## 3. Arrow does not promote decimal128 to decimal256

**Test:** `test_column_validation_large_decimals_to_bigquery_mismatch` (Oracle to BigQuery).

**Symptom:**

```text
pyarrow.lib.ArrowInvalid: Decimal precision out of range [1, 38]: 45
```

**Root cause:** `pc.equal()` promotes both decimal inputs to a common type using
`scale = max(s1, s2)` and `precision = max(p1 - s1, p2 - s2) + scale`, but with PyArrow 14 it stays in
`decimal128` when both inputs are `decimal128`. Oracle returned `decimal128(38, 2)` and BigQuery
`decimal128(38, 9)`, which needs a precision of 36 + 9 = 45. Promotion to `decimal256` only happens
when one input is already `decimal256`.

**Fix (`177dec40`):** `_exact_equal()` in [combiner.py](../../../data_validation/combiner.py) widens any
`decimal128` input to `decimal256` with the same precision and scale before calling `pc.equal()`. This
is lossless and allows up to 76 digits. A fallback for common types needing more than 76 digits was
considered and deliberately not implemented.

Unit test `test_generate_report_with_mixed_exact_types` covers Oracle/BigQuery decimal mixes, `int64`
vs decimal and `decimal128` vs `decimal256`.

## 4. Exact comparison applied to avg/std

**Test:** `test_column_validation_core_types_to_bigquery` (Oracle to BigQuery).

**Root cause:** A regression introduced by fix 2. Oracle returned `std__col_dec_10_2` as
`decimal256(40, 40)` (`0.0953939201416945649...`) and BigQuery as `decimal128(38, 9)` (`0.095393920`).
Both are decimals so the exact comparison was used and failed. Standard deviation and average are
approximate by nature and engines return them with different precision.

**Fix (`177dec40`):** The exact comparison excludes `aggregation_type` `avg` and `std`, which use the
percentage difference instead. Unit test `test_generate_report_with_approximate_decimal_aggs`.

## 5. Unsafe decimal/integer cast in _base_to_pyarrow

**Test:** `test_column_validation_large_decimals_to_bigquery` (Oracle to BigQuery).

**Symptom:** Oracle `sum__col_dec_38` arrived as `477568460403916027209854607343450327.92` instead of
`61728394506172839450617283945061728390`.

**Root cause:** Ibis typed `SUM(NUMBER(38))` as `decimal128(38, 2)`, leaving 36 integer digits, while
the cursor value needed 38. The safe cast failed, and `_base_to_pyarrow()` fell back to
`pc.cast(..., safe=False)`, which silently overflowed:

```text
pa.array([Decimal("61728394506172839450617283945061728390")])  -> decimal128(38, 0)
.cast(decimal128(38, 2))                       -> ArrowInvalid: Rescaling Decimal128 value would cause data loss
pc.cast(..., decimal128(38, 2), safe=False)    -> 477568460403916027209854607343450327.92
```

Similarly, on PostgreSQL (`SUM(bigint) -> numeric`) and MySQL (`SUM(bigint) -> DECIMAL`), SQL widens
sums above $2^{63} - 1$ while Ibis statically types `t.col_int64.sum()` as `int64`; falling back to
`pc.cast(..., pa.int64(), safe=False)` silently wraps modulo $2^{64}$.

Previously both cases were hidden because the values were cast to string in SQL (problem 1).

**Fix (`177dec40` + follow-up):** In `_base_to_pyarrow()`, when the expected type is decimal or integer,
the cursor type is decimal or integer, and the safe cast fails (apart from `int -> decimal`, which uses
string conversion), the cursor's type is kept rather than falling back to `safe=False`. Unit test
`test_base_to_pyarrow_preserves_overflowing_numerics` in
[test_operations.py](../../../tests/unit/ibis_addon/test_operations.py).

## 6. NULL group-by keys do not join

**Test:** `test_grouped_count_validator` (BigQuery).

**Symptom:** `assert 1612 == 1611`.

**Root cause:** `bigquery-public-data.new_york_citibike.citibike_trips` grouped by
`CAST(starttime AS DATE)` has 1,610 dates plus one NULL group. PyArrow joins follow SQL semantics
where `NULL != NULL`, so the source and target NULL groups did not match and produced two rows. The
earlier combiner (`06f38723`) handled this with null-filled synthetic join keys, which commit
`2d4282c7` dropped.

**Fix (`bb103562`, later reworked):** `_null_safe_full_outer_join()` in [combiner.py](../../../data_validation/combiner.py). For each
key containing NULLs it joins on a stand-in key instead: the position of each value in the distinct
values from both tables (`pc.unique` + `pc.index_in(..., skip_nulls=False)`). NULL is one of those
distinct values so it matches itself. The original values, including NULLs, are restored after the
join. Keys without NULLs are joined as normal.

Unit test `test_generate_report_with_null_group_keys` covers a NULL group on both sides, a NULL group
on one side only, composite keys with a partial NULL and an entirely NULL key.

**Performance:** Benchmarked with 5M rows (PyArrow 14). Keys without NULLs are effectively free
(only `null_count` is checked). Keys with NULLs added roughly 20-40% to the join itself, which is
small relative to query execution.

**Caveat:** Because NULLs now match, many rows sharing a NULL key on both sides join many-to-many
(50k × 50k rows = 2.5 billion). This is inherent in NULL-matching semantics, matches the previous
combiner and is the same as duplicate non-NULL keys. It does not affect grouped column validations
(each group appears once per side) or row validations where other key columns keep rows distinct.

## 7. __dvt_fake_pk length / more than one row

**Test:** `test_generate_report_with_too_many_rows` (unit test).

**Symptom:**

```text
ArrowInvalid: Added column's length must match table's length. Expected length 2 but got length 1
```

**Root cause:** `__dvt_fake_pk` is new in this branch (commit `2d4282c7`). Plain column validations
without grouped columns join source and target on this constant key so they can share the grouped/row
code path. It was hard coded as a single row (`[[0]]`), which fails for 0 or 2+ rows.

On `develop` the Ibis combiner used `cross_join()` and the equivalent test asserted 16 rows with a
`TODO: how do we want to handle this going forward?`.

**Fix (`177dec40`):** The constant join key approach was kept, with:

- `pa.repeat(0, table.num_rows)` so the key matches each table's length, including 0 rows.
- A guard raising `ValueError("Expected at most one row per side for a column validation without
  grouped columns, ...")` when either side has more than one row. This resolves the old TODO.

`test_generate_report_with_too_many_rows` now expects the `ValueError`. A new
`test_generate_report_with_empty_target` checks that an empty target produces a single failed row.

## 8. Oracle integer keys returned as floats

**Test:** `test_column_validation_tricky_dates_to_bigquery` (Oracle to BigQuery, grouped by `id`).

**Symptom:**

```text
ArrowInvalid: Incompatible data types for corresponding join field keys: FieldRef.Name(id) of type double and FieldRef.Name(id) of type int64
```

**Root cause:** `id` is `NUMBER(5)` in Oracle. In this query (an epoch seconds subquery, with `id`
also aggregated while grouped on) Oracle describes the result column as an unconstrained `NUMBER`
(precision 0, scale -127):

```text
('ID', DB_TYPE_NUMBER, 127, None, 0, -127, False)
rows: [(1.0, 1, 1.0, ...)]
```

The driver therefore fetches `1.0`. `_base_to_pyarrow()` has a rule that keeps the cursor's float type
when Ibis expects a decimal (to preserve `avg`/`std` precision), so the key reached the combiner as
`double`. Pandas tolerated `1.0 == 1`, PyArrow joins do not.

**Fix (`057f73d5`):** New helper `_floats_exactly_fit_integer_decimal()` in
[operations.py](../../../third_party/ibis/ibis_addon/operations.py). The float-to-decimal casting rule is
skipped (i.e. the expected decimal type is used) when all of these are true:

- the expected type is a decimal with scale 0
- every value is a whole number
- every value is within ±2^53 (exactly representable in float64)
- the cast does not overflow the expected precision

The values are checked explicitly because a PyArrow float to decimal cast rounds (`1.5` → `2`)
rather than raising. `id`, `min__id` and `max__id` now arrive as `decimal128(5, 0)`. `sum__id` stays
float because Oracle gives no precision for it.

Unit test `test_floats_exactly_fit_integer_decimal` in
[test_operations.py](../../../tests/unit/ibis_addon/test_operations.py).

## 9. Join keys with different numeric types

**Test:** `test_column_validation_tricky_dates_to_bigquery`.

**Symptom:** After fix 8, `id of type decimal128(5, 0) and id of type int64`.

**Root cause:** Oracle returns `NUMBER(p)` keys as decimals and BigQuery `INT64` keys as `int64`. Any
Oracle to BigQuery grouped validation or row validation with such a key hits this.

**Fix (`057f73d5`):** New helper `_align_key_types()` in
[combiner.py](../../../data_validation/combiner.py), called at the start of
`_null_safe_full_outer_join()`. For each key whose source and target types differ:

- If both types are numeric (integer, decimal, float), try a safe cast of the target to the source
  type, then the source to the target type. Safe casts raise rather than lose data.
- Otherwise, or if neither cast succeeds, raise `ValueError("Unable to join on key ..., incompatible
  data types ...")`. Non-numeric types are deliberately not converted, because a safe cast would
  otherwise turn an `int64` key into a string and silently fail to match.

Unit tests `test_generate_report_with_mismatched_key_types` and
`test_generate_report_with_incompatible_key_types`.

---

## Open problems

### 10. NULL in both source and target marked as fail (parked)

**Tests:**

- `test_row_validation_comp_fields_reserved_words` (BigQuery and PostgreSQL)
- `test_column_validation_tricky_dates_to_bigquery`, 16 validations for group `id = 2` (Oracle) and
  18 validations for the all-NULL row (PostgreSQL)
- `test_row_validation_comp_fields_tricky_dates_to_bigquery` (PostgreSQL)
- Six Oracle to PostgreSQL tests: `test_column_validation_all_null_oracle_to_postgres`,
  `test_row_validation_comp_fields_oracle_to_postgres` and the four
  `test_custom_query_row_validation_{blob,clob}[_fail]_to_postgres` tests
- `test_generate_report_with_nan_agg_value[source_df2-target_df2-run_metadata2-expected2]` (unit test)

**Root cause:** Both comparison branches in `join_and_unpivot()` use
`pc.fill_null(<comparison>, False)`. `pc.equal(null, null)` is `null` in PyArrow, so NULL (or NaN) on
both sides becomes `fail`. `develop` treated "both null" as success.

**Complication:** After the full outer join, a NULL value column can mean either a real NULL value
or that the row is missing from that side. A simple "both null is success" rule would wrongly pass a
group/primary key present on one side only with all NULL values.

**Proposed fix:**

1. Before the join add `__dvt_source_present = True` to the source and `__dvt_target_present = True`
   to the target. After the join `fill_null(False)` identifies which side had the row.
2. One status rule for row and column validations:

   ```text
   both_present = source_present AND target_present
   both_null    = is_null(src, nan_is_null=True) AND is_null(tgt, nan_is_null=True)
   success      = both_present AND (fill_null(match, False) OR both_null)
   ```

3. Treat NaN as NULL (`nan_is_null=True`), matching `develop`'s use of Pandas `isnull()`.
4. Guard against `null` typed columns (`pa.array([None])`), where `pc.equal(null, null)` raises
   `ArrowNotImplementedError`, by casting to the other side's type before comparing.

Optionally the presence flags could also correct `_log_row_validation_summary()`, which treats a row
with a NULL value as not present.

A related cosmetic inconsistency: `_format_value()` renders a NULL float as `"nan"` but a NULL of any
other type as an empty value. In `test_row_validation_comp_fields_oracle_to_postgres` Oracle's float
`col_num` shows `nan` while the PostgreSQL decimal shows blank, although both are NULL.

### 11. Floating point noise in std/avg (parked)

**Test:** `test_column_validation_large_decimals_to_bigquery` (Oracle to BigQuery),
`std__col_dec_38_30`.

**Root cause:** Oracle returns `7.07e-13` and BigQuery `0.0`, giving `pct_difference = -100`. On
`develop`, `_calculate_difference()` cast decimal and float64 aggregate values to `float32` and rounded
them to 4 decimal places before comparing, so noise like this compared equal. That rounding was
removed on this branch in commit `e0750d5b`.

**Proposed fix:** Restore the `develop` rounding for float aggregates (`std`/`avg` and float columns)
in the threshold comparison only, leaving exact integer/decimal comparisons untouched. Rounding to 4
decimal places makes very small non-zero values compare equal, which is the same trade-off `develop`
made.

### 12. String aggregates rejected by the combiner

**Tests:** `test_column_validation_pg_types` (PostgreSQL to PostgreSQL),
`test_column_validation_oracle_to_postgres` and `test_column_validation_uuid_oracle_to_postgres`
(Oracle to PostgreSQL).

**Symptom:**

```text
ValueError: Aggregate value of min__cast_uuid_string__col_uuid must be numeric
```

**Root cause:** Not caused by the fixes in this document. UUID columns are cast to a string before
aggregation (`append_pre_agg_calc_field()` uses `CONFIG_CAST_UUID_STRING`), so `min`/`max` of a UUID
column is genuinely a string, e.g. `'6240b18a-88c2-410e-8aa4-e937cf073835'`. Other string columns are
aggregated using their length, so UUIDs are the case seen in testing.

On `develop`, `_calculate_difference()` handled string aggregates (`datatype.is_string()`) by
comparing values for equality, with NULL `difference`/`pct_difference`. The PyArrow combiner from
commit `2d4282c7` only accepts numeric aggregates in a column validation and raises for anything else.

**Proposed fix:** In the column validation branch of `join_and_unpivot()`, when both sides are
non-numeric (e.g. `pa.string()`), compare by equality as row validations do, leaving
`difference`/`pct_difference` NULL. This is different from the rejected idea in problem 1 of treating
strings as numeric. It should share the NULL handling proposed in problem 10.

### 13. Spanner backend cannot fetch results into PyArrow

**Tests:** All 14 Spanner tests that run a validation, e.g. `test_count_validator`,
`test_column_validation_core_types`, `test_row_validation_core_types_to_bigquery`. The 5 passing tests
do not execute validation queries through `to_pyarrow()`.

**Symptom:**

```text
AttributeError: 'SpannerCursor' object has no attribute 'fetchmany'. Did you mean: 'fetchall'?
  ibis/backends/base/sql/__init__.py:200
```

**Root cause:** Not caused by the fixes in this document. The Spanner Ibis backend is DVT code in
[ibis_cloud_spanner](../../../third_party/ibis/ibis_cloud_spanner/__init__.py) and subclasses
`BaseSQLBackend`. Its `raw_sql()` returns a minimal `SpannerCursor`
([client.py](../../../third_party/ibis/ibis_cloud_spanner/client.py)) that only implements
`fetchall()`.

- On `develop`, `_execute_validation()` fetched results with `client.execute()`, which the Spanner
  backend overrides to read a snapshot into Pandas (`pandas_df.to_pandas()`).
- Commit `6762dbfd` on this branch switched to `client.to_pyarrow()`. For Spanner that resolves to
  DVT's patched `BaseBackend.to_pyarrow` → `_sql_to_pyarrow_batches` → `BaseSQLBackend._cursor_batches`,
  which calls `cursor.fetchmany()`.

Note that `raw_sql()` also returns the cursor after the `db.snapshot()` context has exited.

**Investigation:**

- **Other backends:** BigQuery and Snowflake fetch Arrow tables natively from their drivers. Impala
  and FileSystem/Pandas use upstream Ibis's `pa.Table.from_pandas(self.execute(...))` fallback,
  which re-introduces Pandas type coercion issues. SQLAlchemy backends stream DB-API cursor batches
  through DVT's patched `_sql_to_pyarrow_batches` and `_base_to_pyarrow` in
  [operations.py](../../../third_party/ibis/ibis_addon/operations.py). Note that `client.execute()`
  is still used outside `_execute_validation()` (random-row sampling, partition generation, `query`
  CLI) and must remain functional.
- **Spanner libraries (`google-cloud-spanner`):** Spanner's gRPC API (`ExecuteStreamingSql`) streams
  `PartialResultSet` protobufs and provides no `.to_arrow()` method in either `spanner_v1` or the
  bundled `spanner_dbapi`. Three `StreamedResultSet` behaviours need explicit handling:
  1. *Snapshot lifetime:* `db.snapshot()` (`SnapshotCheckout`) returns its session to the pool on
     `__exit__`, while `snapshot.execute_sql()` is lazy—rows must be streamed inside the `with
     db.snapshot():` block.
  2. *`TypeCode.BYTES`:* Decoded as UTF-8 bytes of the base64 string (`string_value.encode("utf8")`,
     same in `spanner_dbapi`). Generic `pa.array()` infers `pa.binary()` and would silently pass
     base64-encoded bytes instead of raw bytes into comparisons.
  3. *`TypeCode.JSON`:* Decoded as `JsonObject` (a `dict` subclass). Generic `pa.array()` infers
     `pa.struct`, which fails to cast to Ibis's `dt.json` (`pa.string()`); values need
     `.serialize()`.
- **External packages:** `adbc-driver-spanner` (community Rust ADBC driver) requires Python >= 3.11
  and `glibc >= 2.35` (vs DVT's Python >= 3.9), cannot consume DVT's `google.auth.credentials`
  objects directly, and is a single-maintainer `0.x` native binary. `sqlalchemy-spanner` and
  `spanner_dbapi` wrap the same `StreamedResultSet` (no Arrow support, still base64 `BYTES` and
  `JsonObject`), and `connectorx` only supports Spanner's PostgreSQL dialect via PGAdapter.

**Plan:**

1. Implement `to_pyarrow_batches()` in `third_party/ibis/ibis_cloud_spanner/to_pyarrow.py` and wire
   it onto Spanner `Backend`, keeping `with db.snapshot() as snapshot:` open across batch iteration
   and building typed Arrow columns from `result.fields` (`TypeCode` → `pa.DataType`, NULL-safe
   `base64.b64decode` for `BYTES`, NULL-safe `.serialize()` for `JSON`, `pa.decimal128(38, 9)` for
   `NUMERIC`, and empty-result schema fallback). Inherit DVT's `_base_to_pyarrow()` for column
   renaming and safe schema alignment.
2. Fix `_safe_raw_sql()` to keep `db.snapshot()` open for the cursor's lifetime, add `fetchone()` /
   `fetchmany()` to `SpannerCursor`, keep `execute()` intact (making `to_pandas.py`'s
   `base64.b64decode` NULL-safe), and tidy `datatypes.py` (`FLOAT32` and `ARRAY` mapping).
3. Add unit tests with a mocked `StreamedResultSet` covering all Spanner `TypeCode`s, NULLs, 0-row
   results, multi-batch chunking, and snapshot lifetime, then rerun `test_spanner.py` to check for
   any remaining combiner issues (e.g. #10 or #11).

---

## Not yet verified

- System tests for other engines (e.g. SQL Server, Teradata, Snowflake) have not been run with these
  changes. Their test files may have YAML line count expectations affected by problem 1.
- The full Oracle suite (beyond column validations and Oracle to PostgreSQL) has not been run.
- Spanner combiner behaviour is untested until problem 13 is fixed.
