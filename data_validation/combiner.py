# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module to combine two validation result sets into a single validation run.

To avoid data precision loss, a BigQuery data type as closely matching the
original data type is used.
"""

import datetime
import json
import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
from data_validation import consts


def _is_numeric_type(data_type: pa.DataType) -> bool:
    return (
        pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
        or pa.types.is_decimal(data_type)
        or pa.types.is_timestamp(data_type)
        or pa.types.is_date(data_type)
    )


def _is_string_type(data_type: pa.DataType) -> bool:
    return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)


def _is_exact_type(data_type: pa.DataType) -> bool:
    """Integer and decimal types can be compared exactly, unlike floating point."""
    return pa.types.is_integer(data_type) or pa.types.is_decimal(data_type)


def _exact_equal(src_col, tgt_col):
    """Compare integer/decimal columns exactly.

    Arrow promotes both sides to a common decimal type, scale=max(s1, s2) and
    precision=max(p1-s1, p2-s2)+scale, but stays within decimal128 when both inputs are
    decimal128. For example decimal128(38, 2) vs decimal128(38, 9) needs precision 45 and
    raises an error. Widening to decimal256 first is lossless and allows up to 76 digits.
    """

    def _widen(col):
        if pa.types.is_decimal128(col.type):
            return pc.cast(col, pa.decimal256(col.type.precision, col.type.scale))
        return col

    return pc.equal(_widen(src_col), _widen(tgt_col))


if TYPE_CHECKING:
    from pandas import DataFrame
    from data_validation.metadata import RunMetadata


COMBINER_GET_SUMMARY_EXC_TEXT = (
    "Error while generating summary report of row validation results"
)


def generate_report(
    run_metadata: "RunMetadata",
    source_table: pa.Table,
    target_table: pa.Table,
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> "DataFrame":
    """Combine results into a report.

    Returns:
        pandas.DataFrame:
            A pandas DataFrame with the results of the validation in the same
            schema as the report table.
    """

    # This line was introduced in PR 1465. Is it necessary
    _check_schema_names(source_table, target_table)
    join_on_fields = tuple(join_on_fields)
    result_table = join_and_unpivot(
        run_metadata,
        source_table,
        target_table,
        join_on_fields,
        is_value_comparison,
        verbose,
    )
    _get_summary(run_metadata, result_table, source_table, target_table)
    result_df = result_table.to_pandas(
        timestamp_as_object=False, coerce_temporal_nanoseconds=True
    )
    return result_df


def join_and_unpivot(
    run_metadata: "RunMetadata",
    source_table: pa.Table,
    target_table: pa.Table,
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> pa.Table:
    if not join_on_fields:
        # Plain column validation without a group by has at most one aggregate row per
        # side. We join on a constant fake key so the same code path can be used.
        if source_table.num_rows > 1 or target_table.num_rows > 1:
            raise ValueError(
                "Expected at most one row per side for a column validation without "
                f"grouped columns, got {consts.RESULT_TYPE_SOURCE}={source_table.num_rows}, "
                f"{consts.RESULT_TYPE_TARGET}={target_table.num_rows}"
            )
        source_table = source_table.add_column(
            0, "__dvt_fake_pk", pa.repeat(0, source_table.num_rows)
        )
        target_table = target_table.add_column(
            0, "__dvt_fake_pk", pa.repeat(0, target_table.num_rows)
        )
        pk_columns = ["__dvt_fake_pk"]
    else:
        pk_columns = join_on_fields

    # 1. Each Value column represents a validation. We will process each column and generate a slice
    #    of the validation report
    value_columns = [col for col in source_table.column_names if col not in pk_columns]

    # 2. Rename value columns so they don't collide, then join wide tables on PKs
    source_renamed = source_table.rename_columns(
        [
            col if col in pk_columns else f"{col}_source"
            for col in source_table.column_names
        ]
    )
    target_renamed = target_table.rename_columns(
        [
            col if col in pk_columns else f"{col}_target"
            for col in target_table.column_names
        ]
    )
    aligned = _null_safe_full_outer_join(source_renamed, target_renamed, pk_columns)
    num_rows = aligned.num_rows
    slices = []

    # 3. Many columns in the report are common across all validations, compute these once
    run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)
    run_id_col = pa.repeat(run_metadata.run_id, num_rows)
    labels_col = pa.repeat(run_metadata.labels, num_rows)
    start_time_col = pa.repeat(run_metadata.start_time, num_rows)
    end_time_col = pa.repeat(run_metadata.end_time, num_rows)

    # validation result needs to contain primary key/group by values for status - if present
    if join_on_fields:
        str_schema = pa.schema([(col, pa.string()) for col in join_on_fields])
        group_by_cols = pa.array(
            [
                json.dumps(row)
                for row in aligned.select(join_on_fields).cast(str_schema).to_pylist()
            ],
            type=pa.string(),
        )
    else:
        group_by_cols = pa.nulls(num_rows, type=pa.string())

    null_diff = pa.nulls(num_rows, type=pa.float64())
    # Row validations has nulls for differences and column validation has nulls for primary_keys.
    if is_value_comparison:
        difference = null_diff
        pct_difference = null_diff
        primary_keys = pa.repeat(
            "{" + ", ".join(join_on_fields) + "}" if join_on_fields else None,
            num_rows,
        )
    else:
        primary_keys = pa.nulls(num_rows, type=pa.string())

    # 4. Loop over each value column and compare using native Arrow types
    #    and create the report for that specific validation.
    for col in value_columns:
        validation = run_metadata.validations[col]
        src_col = aligned[f"{col}_source"]
        tgt_col = aligned[f"{col}_target"]

        src_agg_val = _format_value(src_col)
        tgt_agg_val = _format_value(tgt_col)
        threshold = pa.repeat(validation.threshold, num_rows)

        if is_value_comparison:  # row validation - compare values for equality
            # Native type equality check (works on int64, decimal, string, timestamp, etc.)
            is_successful = pc.if_else(
                pc.fill_null(pc.equal(src_col, tgt_col), False),
                consts.VALIDATION_STATUS_SUCCESS,
                consts.VALIDATION_STATUS_FAIL,
            )
        elif _is_numeric_type(src_col.type) and _is_numeric_type(
            tgt_col.type
        ):  # aggregated values, compare % difference against threshold
            # According to PR 195 https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/195
            # date and timestamp aggregatins should use epoch seconds for source and target values
            if pa.types.is_date(src_col.type) or pa.types.is_timestamp(src_col.type):
                src_col = pc.cast(
                    pc.cast(src_col, pa.timestamp("s"), safe=False), pa.int64()
                )
            if pa.types.is_date(tgt_col.type) or pa.types.is_timestamp(tgt_col.type):
                tgt_col = pc.cast(
                    pc.cast(tgt_col, pa.timestamp("s"), safe=False), pa.int64()
                )
            src_num = pc.cast(src_col, pa.float64(), safe=False)
            tgt_num = pc.cast(tgt_col, pa.float64(), safe=False)
            difference = pc.subtract(tgt_num, src_num)
            pct_difference = pc.if_else(
                pc.equal(difference, 0),
                0.0,
                pc.round(
                    pc.multiply(
                        100.0,
                        pc.divide(
                            difference,
                            pc.if_else(pc.equal(src_num, 0), tgt_num, src_num),
                        ),
                    ),
                    ndigits=4,
                ),
            )
            if (
                not validation.threshold
                and validation.aggregation_type
                not in (consts.CONFIG_TYPE_AVG, consts.CONFIG_TYPE_STD)
                and _is_exact_type(src_col.type)
                and _is_exact_type(tgt_col.type)
            ):
                # With a zero threshold integer/decimal aggregates must match exactly.
                # The float64 difference above can lose precision beyond ~16 significant
                # digits so we compare the native values instead, Arrow promotes both
                # sides to a common integer/decimal type without loss.
                # avg and std are approximate by nature, engines return them with
                # different precision/scale, so they use the percentage difference.
                success_condition = _exact_equal(src_col, tgt_col)
            else:
                success_condition = pc.less_equal(pc.abs(pct_difference), threshold)
            is_successful = pc.if_else(
                pc.fill_null(success_condition, False),
                consts.VALIDATION_STATUS_SUCCESS,
                consts.VALIDATION_STATUS_FAIL,
            )
        elif _is_string_type(src_col.type) and _is_string_type(tgt_col.type):
            # String aggregates (e.g. min/max of a UUID cast to string, or legacy
            # cast: string configs) have no numeric difference and compare by equality.
            difference = null_diff
            pct_difference = null_diff
            is_successful = pc.if_else(
                pc.fill_null(pc.equal(src_col, tgt_col), False),
                consts.VALIDATION_STATUS_SUCCESS,
                consts.VALIDATION_STATUS_FAIL,
            )
        else:
            raise ValueError(
                f"Aggregate value of {col} must be numeric or string, got "
                f"{consts.RESULT_TYPE_SOURCE}: {src_col.type}, "
                f"{consts.RESULT_TYPE_TARGET}: {tgt_col.type}"
            )

        # 3. Cast display values to string so all slices share the same output schema
        validation_name_col = pa.repeat(col, num_rows)
        col_slice = pa.table(
            {
                consts.VALIDATION_NAME: validation_name_col,
                consts.VALIDATION_TYPE: pa.repeat(validation.validation_type, num_rows),
                consts.AGGREGATION_TYPE: pa.repeat(
                    pa.scalar(validation.aggregation_type, type=pa.string()), num_rows
                ),
                consts.SOURCE_TABLE_NAME: pa.repeat(
                    pa.scalar(
                        validation.get_table_name(consts.RESULT_TYPE_SOURCE),
                        type=pa.string(),
                    ),
                    num_rows,
                ),
                consts.SOURCE_COLUMN_NAME: pa.repeat(
                    pa.scalar(
                        validation.get_column_name(consts.RESULT_TYPE_SOURCE),
                        type=pa.string(),
                    ),
                    num_rows,
                ),
                consts.SOURCE_AGG_VALUE: src_agg_val,
                consts.TARGET_TABLE_NAME: pa.repeat(
                    pa.scalar(
                        validation.get_table_name(consts.RESULT_TYPE_TARGET),
                        type=pa.string(),
                    ),
                    num_rows,
                ),
                consts.TARGET_COLUMN_NAME: pa.repeat(
                    pa.scalar(
                        validation.get_column_name(consts.RESULT_TYPE_TARGET),
                        type=pa.string(),
                    ),
                    num_rows,
                ),
                consts.TARGET_AGG_VALUE: tgt_agg_val,
                consts.GROUP_BY_COLUMNS: group_by_cols,
                consts.CONFIG_PRIMARY_KEYS: primary_keys,
                consts.NUM_RANDOM_ROWS: pa.repeat(validation.num_random_rows, num_rows),
                consts.VALIDATION_DIFFERENCE: difference,
                consts.VALIDATION_PCT_DIFFERENCE: pct_difference,
                consts.VALIDATION_PCT_THRESHOLD: threshold,
                consts.VALIDATION_STATUS: is_successful,
                consts.CONFIG_RUN_ID: run_id_col,
                consts.CONFIG_LABELS: labels_col,
                consts.CONFIG_START_TIME: start_time_col,
                consts.CONFIG_END_TIME: end_time_col,
            }
        )
        slices.append(col_slice)
    # 4. Stack all per-column slices into the final unpivoted table
    return pa.concat_tables(slices)


def _align_key_types(source_table: pa.Table, target_table: pa.Table, keys: list):
    """Cast join keys with differing source/target types to a common type.

    PyArrow joins require identical key types but engines often return the same key with
    different types, e.g. Oracle NUMBER(5) as decimal128(5, 0) and BigQuery INT64 as int64.
    For numeric keys we try a safe cast in each direction, a safe cast raises rather than
    lose data. Other type mismatches are not silently converted.
    """

    def _is_numeric(data_type: pa.DataType) -> bool:
        return (
            pa.types.is_integer(data_type)
            or pa.types.is_decimal(data_type)
            or pa.types.is_floating(data_type)
        )

    for key in keys:
        source_col = source_table[key]
        target_col = target_table[key]
        if source_col.type == target_col.type:
            continue
        incompatible = ValueError(
            f"Unable to join on key {key}, incompatible data types "
            f"{consts.RESULT_TYPE_SOURCE}: {source_col.type}, "
            f"{consts.RESULT_TYPE_TARGET}: {target_col.type}"
        )
        if not (_is_numeric(source_col.type) and _is_numeric(target_col.type)):
            raise incompatible
        try:
            target_table = target_table.set_column(
                target_table.column_names.index(key),
                key,
                target_col.cast(source_col.type),
            )
            continue
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
        try:
            source_table = source_table.set_column(
                source_table.column_names.index(key),
                key,
                source_col.cast(target_col.type),
            )
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            raise incompatible from e
    return source_table, target_table


def _null_safe_full_outer_join(
    source_table: pa.Table, target_table: pa.Table, keys: list
) -> pa.Table:
    """Full outer join source and target tables where NULL key values match each other.

    PyArrow joins follow SQL semantics where NULL != NULL. A grouped column validation can
    legitimately have a NULL group, which must be matched between source and target.
    For each key containing NULLs we join on a stand-in key instead: the position of each
    value in the distinct values from both tables. NULL is one of those distinct values so
    it matches itself. The original values, including NULLs, are restored after the join.
    """

    def _replace(table: pa.Table, name: str, values) -> pa.Table:
        return table.set_column(table.column_names.index(name), name, values)

    source_table, target_table = _align_key_types(source_table, target_table, keys)
    value_sets = {}
    for key in keys:
        if not (source_table[key].null_count or target_table[key].null_count):
            continue
        value_set = pc.unique(
            pa.chunked_array(source_table[key].chunks + target_table[key].chunks)
        )
        value_sets[key] = value_set
        source_table, target_table = (
            _replace(t, key, pc.index_in(t[key], value_set=value_set, skip_nulls=False))
            for t in (source_table, target_table)
        )

    aligned = source_table.join(target_table, keys=list(keys), join_type="full outer")
    for key, value_set in value_sets.items():
        aligned = _replace(aligned, key, value_set.take(aligned[key]))
    return aligned


def _check_schema_names(source_table: pa.Table, target_table: pa.Table):
    """Check that the two input tables have matching column names."""
    source_names = tuple(source_table.column_names)
    target_names = tuple(target_table.column_names)

    if source_names != target_names:
        raise ValueError(
            "Expected source and target to have same schema, got "
            f"{consts.RESULT_TYPE_SOURCE}: {source_names}; {consts.RESULT_TYPE_TARGET}: {target_names}"
        )


def _format_value(value):
    if pa.types.is_binary(value.type):
        return pa.array(
            [item.hex() if item is not None else None for item in value.to_pylist()],
            type=pa.string(),
        )
    elif pa.types.is_floating(value.type):
        return pc.fill_null(pc.cast(value, pa.string()), "nan")
    else:
        formatted = pc.cast(value, pa.string())
    if pa.types.is_timestamp(value.type):
        formatted = pc.replace_substring_regex(formatted, r" 00:00:00(\.0+)?$", "")
        formatted = pc.replace_substring_regex(formatted, r"(\.0+)?Z$", "+00:00")
        formatted = pc.replace_substring_regex(formatted, r"\.0+$", "")
        formatted = pc.replace_substring_regex(formatted, r"\+00$", "+00:00")
    return formatted


def _log_row_validation_summary(
    run_metadata: "RunMetadata",
    result_table: pa.Table,
    source_table: pa.Table,
    target_table: pa.Table,
):
    """Logs a summary report/stats of row validation results."""
    success_condition = pc.equal(
        result_table[consts.VALIDATION_STATUS], consts.VALIDATION_STATUS_SUCCESS
    )
    fail_condition = pc.invert(success_condition)
    source_present = pc.is_valid(result_table[consts.SOURCE_AGG_VALUE])
    target_present = pc.is_valid(result_table[consts.TARGET_AGG_VALUE])

    logging.info(
        json.dumps(
            {
                consts.CONFIG_RUN_ID: run_metadata.run_id,
                consts.CONFIG_START_TIME: run_metadata.start_time.isoformat(),
                consts.CONFIG_END_TIME: run_metadata.end_time.isoformat(),
                consts.TOTAL_SOURCE_ROWS: source_table.num_rows,
                consts.TOTAL_TARGET_ROWS: target_table.num_rows,
                consts.TOTAL_ROWS_VALIDATED: result_table.num_rows,
                consts.TOTAL_ROWS_SUCCESS: _count_true(success_condition),
                consts.TOTAL_ROWS_FAIL: _count_true(fail_condition),
                consts.FAILED_SOURCE_NOT_IN_TARGET: _count_true(
                    pc.and_kleene(
                        fail_condition,
                        pc.and_kleene(source_present, pc.invert(target_present)),
                    )
                ),
                consts.FAILED_TARGET_NOT_IN_SOURCE: _count_true(
                    pc.and_kleene(
                        fail_condition,
                        pc.and_kleene(pc.invert(source_present), target_present),
                    )
                ),
                consts.FAILED_PRESENT_IN_BOTH_TABLES: _count_true(
                    pc.and_kleene(
                        fail_condition, pc.and_kleene(source_present, target_present)
                    )
                ),
            }
        )
    )


def _log_validation_summary(
    run_metadata: "RunMetadata",
    result_table: pa.Table,
):
    """Logs a summary report/stats of validation results."""
    success_condition = pc.equal(
        result_table[consts.VALIDATION_STATUS], consts.VALIDATION_STATUS_SUCCESS
    )
    fail_condition = pc.invert(success_condition)

    logging.info(
        json.dumps(
            {
                consts.CONFIG_RUN_ID: run_metadata.run_id,
                consts.CONFIG_START_TIME: run_metadata.start_time.isoformat(),
                consts.CONFIG_END_TIME: run_metadata.end_time.isoformat(),
                consts.TOTAL_VALIDATIONS: result_table.num_rows,
                consts.TOTAL_VALIDATIONS_SUCCESS: _count_true(success_condition),
                consts.TOTAL_VALIDATIONS_FAIL: _count_true(fail_condition),
            }
        )
    )


def _get_summary(
    run_metadata: "RunMetadata",
    result_table: pa.Table,
    source_table: pa.Table,
    target_table: pa.Table,
):
    """Logs a summary report/stats of validation results."""
    try:
        if not result_table.num_rows:
            return

        # Primary keys are present for row validation or custom-query row validation.
        if result_table[consts.CONFIG_PRIMARY_KEYS][0].as_py():
            _log_row_validation_summary(
                run_metadata, result_table, source_table, target_table
            )
        else:
            # If we don't output the specific row validation INFO line, output a generic summary.
            _log_validation_summary(run_metadata, result_table)
    except Exception as e:
        logging.warning(
            f"{COMBINER_GET_SUMMARY_EXC_TEXT}: {e}",
            exc_info=True,
        )


def _count_true(condition):
    return int(pc.sum(pc.fill_null(condition, False)).as_py())
