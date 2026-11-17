# Branch Divergence Analysis: `issue-1702-using-duckdb`

**Date:** 2026-11-05  
**Local Branch (`HEAD`):** `issue-1702-using-duckdb` (`0bdb4d5d`)  
**Remote Branch:** `origin/issue-1702-using-duckdb` (`c8c86cca`)  
**Common Ancestor (Merge Base):** `2d4282c7` (*"Updated generate_report to use pyAroow natively instead of pandas and Ibis. Followed the spec in ../docs/internal/design_docs/combiner_review.md"*)

---

## 1. Overview of Divergence

Since commit `2d4282c7`, the local branch and `origin/issue-1702-using-duckdb` have diverged:
- **Local (`HEAD` -> `0bdb4d5d`)** is **1 commit ahead** of `2d4282c7`.
- **Remote (`origin/issue-1702-using-duckdb` -> `c8c86cca`)** is **27 commits ahead** of `2d4282c7`:
  - **15 commits** from merging `develop` into `issue-1702-using-duckdb` (`d70142fd`).
  - **12 commits** authored by `nj1973` addressing PyArrow combiner issues, adding ~800+ lines of unit tests, documenting findings in `docs/internal/design_docs/combiner_pyarrow_issues_and_fixes.md`, and refactoring PyArrow execution helpers.

Both your local commit (`0bdb4d5d`) and the 12 remote commits by `nj1973` started from `2d4282c7` and solved **almost the exact same set of PyArrow combiner bugs in parallel**.

---

## 2. Commit History Since Common Ancestor (`2d4282c7`)

### Local Commit (1 commit)
- `0bdb4d5d` — *Refactored combiner to use pyArrow. Most system integration tests pass; Unit tests not yet tested*
  - `data_validation/combiner.py` (`+104 / -18`)
  - `data_validation/config_manager.py` (`+2 / -2`)
  - `tests/system/data_sources/test_bigquery.py` (`+3 / -3`)
  - `tests/unit/test_combiner.py` (`+1 / -15`)

### Remote Commits on `origin/issue-1702-using-duckdb` (27 commits)

#### PyArrow Combiner Commits (12 commits, newest first)
1. `c8c86cca` — `fix: Empty table fix`
2. `7f331ec8` — `chore: Refactor pyarrow code out of operations.py`
3. `fb55f40b` — `fix: Enhance combiner exact value matching`
4. `16b175b4` — `fix: Widen pct_difference from Float32 to Float64`
5. `86b12a6d` — `fix: Combiner supports string aggregates again`
6. `be22bd7b` — `fix: Remove the Pandas overflow workaround of cast to string`
7. `006240aa` — `docs: Document discovered issues on the pyarrow branch`
8. `ae49fa39` — `docs: Document discovered issues on the pyarrow branch`
9. `057f73d5` — `fix: Align numeric join key types in combiner and restore integer decimals Oracle returns as floats`
10. `177dec40` — `fix: Use decimal256 for numeric joins in combiner`
11. `bb103562` — `fix: Remove the Pandas overflow workaround of cast to string`
12. `e525feef` — `fix: Revert Oracle schema name change`

#### Merge from `develop` (15 commits)
- `d70142fd` — `Merge branch 'develop' into issue-1702-using-duckdb` (includes 14 commits from `develop` up to `4b0426c1`, including release `v8.10.0`).

---

## 3. File-by-File & Feature Comparison

| Area / File | Local Commit (`0bdb4d5d`) | Remote (`origin/issue-1702-using-duckdb`) | Assessment |
| :--- | :--- | :--- | :--- |
| **`data_validation/config_manager.py`**<br>*(Pandas overflow `cast: string` workaround)* | Disabled the string-cast blocks in `append_pre_agg_calc_field` and `build_config_column_aggregates` using `if False and ...`. | Removed the `cast: string` workaround cleanly (`bb103562`, `be22bd7b`), renamed `_decimal_column_too_big_for_pandas` to `_is_high_precision_decimal` (preserving `cast: float64` only for `std`/`avg` on decimals with precision > 18), and updated `tests/unit/test_config_manager.py`. | **Covered on remote** (cleaner implementation + unit tests). |
| **`tests/system/data_sources/test_bigquery.py`**<br>*(YAML line count expectations)* | Updated 3 constants:<br>- `EXPECTED_NUM_YAML_LINES_WILDCARD`: `162 -> 155`<br>- `EXPECTED_NUM_YAML_LINES_TIMESTAMP_SUM_AVG_BITXOR`: `54 -> 53`<br>- `EXPECTED_NUM_YAML_LINES_BQ_DATETIME_SUM_AVG_BITXOR`: `54 -> 53` | Identical updates to the same 3 constants in `bb103562`, plus CLI refactoring updates from merging `develop` (`config_runner`, `validation_runner`). | **Identical on remote** (plus `develop` updates). |
| **`data_validation/combiner.py`**<br>*(NULL keys & mismatched key types in full outer join)* | Cast all `join_on_fields` to `pa.string()`, replaced `NULL` with `NULL_SENTINEL = "__dvt_null__"` before `join`, and restored `None` when serializing `group_by_cols` JSON. | Added `_null_safe_full_outer_join()` (uses `pc.unique` and `pc.index_in(..., skip_nulls=False)` so `NULL == NULL` without converting key columns to string or risking sentinel collision) and `_align_key_types()` (safely aligns numeric key types across engines, e.g. `decimal128(5,0)` vs `int64`). | **Covered on remote** (preserves native key types + unit tests). |
| **`data_validation/combiner.py`**<br>*(High-precision decimals)* | Added `_safe_decimal_type(src_col, tgt_col)` casting operands to `pa.decimal256(75, scale)` prior to `pc.subtract(tgt_col, src_col)`, then casting `difference` to `pa.float64()`. | Added `_is_exact_type()` and `_exact_equal()` using `pa.decimal256(76, max_scale)` for 0-threshold exact comparisons and row value comparisons (`177dec40`, `fb55f40b`), while keeping `float64` difference calculation and widening `pct_difference` from `float32` to `float64` (`16b175b4`). | **Covered on remote** (also handles row validations and excludes `avg`/`std` from exact decimal comparison). |
| **`data_validation/combiner.py`**<br>*(String aggregates, e.g., `min`/`max` on UUID/string)* | Added `elif pa.types.is_string(src_col.type) and pa.types.is_string(tgt_col.type):` setting `difference` and `pct_difference` to `pa.nulls(..., pa.float64())` and comparing equality. | Added `elif _is_string_type(src_col.type) and _is_string_type(tgt_col.type):` (handles `string` and `large_string`) setting `difference` and `pct_difference` to `null_diff` and comparing equality (`86b12a6d`). | **Covered on remote** (with unit tests). |
| **`data_validation/combiner.py`**<br>*(NULL in both source and target value columns)* | Added `both_null = pc.and_kleene(pc.is_null(src_col), pc.is_null(tgt_col))` and `pc.or_kleene(both_null, ...)` across all 3 comparison branches (row validation, numeric aggregate, string aggregate). | **Not yet implemented on remote** — documented in `combiner_pyarrow_issues_and_fixes.md` as **Problem #10 (Open / parked)**. | **Only in local commit `0bdb4d5d`** (see Section 4 below). |
| **`data_validation/combiner.py`**<br>*(Empty tables & multi-row ungrouped guard)* | Not present locally. | Added `_empty_report_table()` when `num_rows == 0` (`c8c86cca`) and a `ValueError` check if an ungrouped column validation has `> 1` row (`177dec40`). | **Remote only**. |
| **`tests/unit/test_combiner.py`** | Removed `test_generate_report_with_too_many_rows` and changed 1 expected status in `test_generate_report_with_group_by` (`SUCCESS -> FAIL`). | Updated `test_generate_report_with_too_many_rows` to assert `pytest.raises(ValueError)` and added ~800 lines of unit tests covering decimals, key alignment, string aggregates, and empty tables. | **Remote has expanded test suite**. |

---

## 4. Deep Dive: The `both_null` Change (Problem #10)

The only functional logic present in your local commit `0bdb4d5d` that is **not** yet on `origin/issue-1702-using-duckdb` is treating `NULL` in both `src_col` and `tgt_col` as `VALIDATION_STATUS_SUCCESS`:

```python
both_null = pc.and_kleene(pc.is_null(src_col), pc.is_null(tgt_col))
values_equal = pc.fill_null(pc.equal(src_col, tgt_col), False)
is_successful = pc.if_else(
    pc.or_kleene(both_null, values_equal),
    consts.VALIDATION_STATUS_SUCCESS,
    consts.VALIDATION_STATUS_FAIL,
)
```

### Caveat Noted in Remote Design Doc (`combiner_pyarrow_issues_and_fixes.md`, Section 10)
After a full outer join (`aligned`), a `NULL` in `src_col` or `tgt_col` can happen for two different reasons:
1. **Both rows exist**, and the column/aggregate value is genuinely `NULL` in both source and target (should be **`SUCCESS`**).
2. **One side is missing the row** (e.g., key `k1` only exists in source, not in target), AND the side that has `k1` happens to have a `NULL` value in `src_col`. After the full outer join, `src_col` is `NULL` (actual value) and `tgt_col` is `NULL` (missing row) — which should be **`FAIL`**, not `SUCCESS`.

Notice that in your local commit `0bdb4d5d`, you had to change `test_generate_report_with_group_by` in `tests/unit/test_combiner.py` because of how `both_null` interacted with grouped rows.

In `combiner_pyarrow_issues_and_fixes.md`, the proposed complete fix for Problem #10 is:
1. Before the full outer join, add boolean presence columns (`__dvt_source_present = True` on source, `__dvt_target_present = True` on target).
2. After the join, compute `both_present = pc.and_kleene(pc.fill_null(aligned["__dvt_source_present"], False), pc.fill_null(aligned["__dvt_target_present"], False))`.
3. Only allow `both_null` to succeed when `both_present` is `True`.

---

## 5. Options for Merging / Resolving the Divergence

### Option 1: Adopt Remote (`origin/issue-1702-using-duckdb`) + Backup Local Commit
1. Create a local backup branch pointing to `0bdb4d5d` (e.g., `git branch backup-local-0bdb4d5d`) so your commit is preserved.
2. Reset or merge `issue-1702-using-duckdb` to match `origin/issue-1702-using-duckdb` (`c8c86cca`).

### Option 2: Merge Remote + Implement Problem #10 (`both_null` with row-presence tracking)
1. Create a backup branch (`git branch backup-local-0bdb4d5d`).
2. Merge `origin/issue-1702-using-duckdb` keeping the remote implementations for `config_manager.py`, `test_bigquery.py`, `test_combiner.py`, and `combiner.py`.
3. Apply the `both_null` + `both_present` logic on top of `combiner.py` and verify with `pytest tests/unit/test_combiner.py`.

### Option 3: Custom Merge
1. Run `git merge origin/issue-1702-using-duckdb` and selectively choose which local hunks in `data_validation/combiner.py` to retain over the remote implementation.
