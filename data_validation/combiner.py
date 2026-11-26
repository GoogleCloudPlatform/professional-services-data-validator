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
import decimal
import json
import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
from data_validation import consts

if TYPE_CHECKING:
    from pandas import DataFrame
    from data_validation.metadata import RunMetadata

COMBINER_GET_SUMMARY_EXC_TEXT = (
    "Error while generating summary report of row validation results"
)


def _is_numeric_type(column: pa.ChunkedArray) -> bool:
    return (
        pa.types.is_integer(column.type)
        or pa.types.is_floating(column.type)
        or pa.types.is_decimal(column.type)
        or pa.types.is_timestamp(column.type)
        or pa.types.is_date(column.type)
    )


def _exact_numeric_types(src_col: pa.ChunkedArray, tgt_col: pa.ChunkedArray) -> bool:
    return not (
        pa.types.is_floating(src_col.type) or pa.types.is_floating(tgt_col.type)
    )


def _aggregation_type_not_lossy(aggregation_type: str) -> bool:
    return not (aggregation_type.lower() in ["avg", "std"])


def _is_string_type(column: pa.ChunkedArray) -> bool:
    return pa.types.is_string(column.type) or pa.types.is_large_string(column.type)


def _is_exact_type(column: pa.ChunkedArray) -> bool:
    return pa.types.is_integer(column.type) or pa.types.is_decimal(column.type)


def _safe_decimal_type(
    src_col: pa.ChunkedArray, tgt_col: pa.ChunkedArray
) -> tuple[pa.ChunkedArray, pa.ChunkedArray]:
    # Python Arrow needs one additional digit of precision while doing additions and
    # subtractions. If more than 76 digits are needed, subtraction falls back to float64
    # and _exact_comparison() falls back to an exact Python comparison.
    # It is possible that one of the columns is not decimal, so we need to convert
    # both columns to decimal first. Only integer/decimal pairs are converted: casting a
    # float to decimal rounds (1.5 -> 2), and dates/timestamps cannot be cast to decimal.
    if not (_is_exact_type(src_col) and _is_exact_type(tgt_col)):
        return src_col, tgt_col
    types = (src_col.type, tgt_col.type)
    scale = max(t.scale if pa.types.is_decimal(t) else 0 for t in types)
    int_digits = max(
        (
            max(t.precision - t.scale, 0)
            if pa.types.is_decimal(t)
            else (20 if pa.types.is_uint64(t) else 19)
        )
        for t in types
    )
    precision = int_digits + scale
    if precision > 76:
        return src_col, tgt_col
    common_type = pa.decimal256(max(precision, 1), scale)
    return pc.cast(src_col, common_type), pc.cast(tgt_col, common_type)


def _naive_timestamp_as_utc(
    src_col: pa.ChunkedArray, tgt_col: pa.ChunkedArray
) -> tuple[pa.ChunkedArray, pa.ChunkedArray]:
    # Arrow cannot compare time zone aware and naive timestamps, e.g. Spanner TIMESTAMP vs
    # BigQuery DATETIME. DVT queries engines in UTC, so a naive timestamp is treated as UTC.
    if not (
        pa.types.is_timestamp(src_col.type) and pa.types.is_timestamp(tgt_col.type)
    ):
        return src_col, tgt_col
    if src_col.type.tz and not tgt_col.type.tz:
        tgt_col = pc.cast(tgt_col, pa.timestamp(tgt_col.type.unit, tz="UTC"))
    elif tgt_col.type.tz and not src_col.type.tz:
        src_col = pc.cast(src_col, pa.timestamp(src_col.type.unit, tz="UTC"))
    return src_col, tgt_col


def _align_key_columns(
    src_col: pa.ChunkedArray, tgt_col: pa.ChunkedArray, col_name: str
) -> tuple[pa.ChunkedArray, pa.ChunkedArray]:
    src_col, tgt_col = _naive_timestamp_as_utc(src_col, tgt_col)
    if src_col.type != tgt_col.type:
        try:
            unified = pa.unify_schemas(
                [
                    pa.schema([(col_name, src_col.type)]),
                    pa.schema([(col_name, tgt_col.type)]),
                ],
                promote_options="permissive",
            )
            common_type = unified.field(col_name).type
            src_col = pc.cast(src_col, common_type)
            tgt_col = pc.cast(tgt_col, common_type)
        except (
            pa.ArrowTypeError,
            pa.ArrowInvalid,
            pa.ArrowNotImplementedError,
        ) as exc:
            raise ValueError(f"Unable to join on key {col_name}") from exc
    if pa.types.is_null(src_col.type):
        src_col = pc.cast(src_col, pa.int8())
        tgt_col = pc.cast(tgt_col, pa.int8())
    return src_col, tgt_col


def _dummy_fill_scalar(arrow_type: pa.DataType) -> pa.Scalar:
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return pa.scalar("", type=arrow_type)
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return pa.scalar(b"", type=arrow_type)
    if pa.types.is_boolean(arrow_type):
        return pa.scalar(False, type=arrow_type)
    if pa.types.is_decimal(arrow_type):
        return pa.scalar(decimal.Decimal(0), type=arrow_type)
    # Handles int, float, date32/64, timestamp, and time32/64
    return pa.scalar(0, type=arrow_type)


def _format_key_value(col: pa.ChunkedArray):
    if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
        return pa.array(
            [item.hex() if item is not None else None for item in col.to_pylist()],
            type=pa.string(),
        )
    return pc.cast(col, pa.string())


def _empty_report_table(run_metadata: "RunMetadata") -> pa.Table:
    """Return an empty report table with the standard 20-column schema."""
    start_time_type = (
        pa.scalar(run_metadata.start_time).type
        if run_metadata.start_time is not None
        else pa.timestamp("us", tz="UTC")
    )
    return pa.schema(
        [
            (consts.VALIDATION_NAME, pa.string()),
            (consts.VALIDATION_TYPE, pa.string()),
            (consts.AGGREGATION_TYPE, pa.string()),
            (consts.SOURCE_TABLE_NAME, pa.string()),
            (consts.SOURCE_COLUMN_NAME, pa.string()),
            (consts.SOURCE_AGG_VALUE, pa.string()),
            (consts.TARGET_TABLE_NAME, pa.string()),
            (consts.TARGET_COLUMN_NAME, pa.string()),
            (consts.TARGET_AGG_VALUE, pa.string()),
            (consts.GROUP_BY_COLUMNS, pa.string()),
            (consts.CONFIG_PRIMARY_KEYS, pa.string()),
            (consts.NUM_RANDOM_ROWS, pa.int64()),
            (consts.VALIDATION_DIFFERENCE, pa.float64()),
            (consts.VALIDATION_PCT_DIFFERENCE, pa.float64()),
            (consts.VALIDATION_PCT_THRESHOLD, pa.float64()),
            (consts.VALIDATION_STATUS, pa.string()),
            (consts.CONFIG_RUN_ID, pa.string()),
            (consts.CONFIG_LABELS, pa.list_(pa.list_(pa.string()))),
            (consts.CONFIG_START_TIME, start_time_type),
            (consts.CONFIG_END_TIME, pa.timestamp("us", tz="UTC")),
        ]
    ).empty_table()


def _exact_comparison(src_col, tgt_col, float_nulls_col, both_present=None):
    # User is looking for exact comparison, so we return the validation status
    # along with the difference and percentage difference, both null values.
    both_null = pc.and_kleene(
        pc.is_null(src_col, nan_is_null=True),
        pc.is_null(tgt_col, nan_is_null=True),
    )
    if both_present is not None:
        both_null = pc.and_kleene(both_present, both_null)
    try:
        equal = pc.equal(src_col, tgt_col)
    except pa.ArrowInvalid:
        # Decimals needing more than 76 combined digits have no common Arrow type.
        # Other type mismatches must raise rather than be reported as a fail.
        if not (
            pa.types.is_decimal(src_col.type) and pa.types.is_decimal(tgt_col.type)
        ):
            raise
        equal = pa.array(
            [
                None if src is None or tgt is None else src == tgt
                for src, tgt in zip(src_col.to_pylist(), tgt_col.to_pylist())
            ],
            type=pa.bool_(),
        )
    values_equal = pc.fill_null(equal, False)
    is_successful = pc.if_else(
        pc.or_kleene(both_null, values_equal),
        consts.VALIDATION_STATUS_SUCCESS,
        consts.VALIDATION_STATUS_FAIL,
    )
    return is_successful, float_nulls_col, float_nulls_col


def _compare_numeric_aggregate(
    src_col, tgt_col, validation, threshold, float_nulls_col, both_present=None
):
    """Compare numeric aggregate columns and return (status, difference, pct_difference)."""
    # According to PR 195 https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/195
    # date and timestamp aggregations should use epoch seconds for source and target values
    if pa.types.is_date(src_col.type) or pa.types.is_timestamp(src_col.type):
        src_col = pc.cast(pc.cast(src_col, pa.timestamp("s"), safe=False), pa.int64())
    if pa.types.is_date(tgt_col.type) or pa.types.is_timestamp(tgt_col.type):
        tgt_col = pc.cast(pc.cast(tgt_col, pa.timestamp("s"), safe=False), pa.int64())
    try:
        difference = pc.subtract_checked(tgt_col, src_col)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        # The exact result overflows int64 or needs more than 76 decimal
        # digits, so subtract as float64 instead.
        difference = pc.subtract(
            pc.cast(tgt_col, pa.float64(), safe=False),
            pc.cast(src_col, pa.float64(), safe=False),
        )
    # difference is the same type as tgt_col and src_col, but needs to be
    # float in the table, so casting it now.
    difference = pc.cast(difference, pa.float64(), safe=False)
    if validation.aggregation_type.lower() == consts.CONFIG_TYPE_STD:
        # There are different methods to calculate standard deviation,
        # and we ignore small differences less than consts.STD_SENSITIVITY.
        difference = pc.if_else(
            pc.less(pc.abs(difference), consts.STD_SENSITIVITY), 0.0, difference
        )
    pct_difference = pc.if_else(
        pc.equal(difference, 0.0),
        0.0,
        pc.divide(
            pc.multiply(100.0, difference),
            pc.if_else(
                pc.equal(src_col, pa.scalar(0, type=src_col.type)),
                pc.cast(tgt_col, pa.float64(), safe=False),
                pc.cast(src_col, pa.float64(), safe=False),
            ),
        ),
    )
    if (
        _aggregation_type_not_lossy(validation.aggregation_type)
        and validation.threshold < consts.VALIDATION_PCT_SENSITIVITY
        and _exact_numeric_types(src_col, tgt_col)
    ):
        # User is looking for exact comparison
        is_successful, _, _ = _exact_comparison(
            src_col, tgt_col, float_nulls_col, both_present=both_present
        )
    else:
        # We need to use threshold for success and failure
        # Some aggregates can be null if all the values are null so handle null==null;
        both_null = pc.and_kleene(
            pc.is_null(src_col, nan_is_null=True),
            pc.is_null(tgt_col, nan_is_null=True),
        )
        if both_present is not None:
            both_null = pc.and_kleene(both_present, both_null)
        values_below_threshold = pc.fill_null(
            pc.less_equal(pc.abs(pct_difference), threshold), False
        )
        is_successful = pc.if_else(
            pc.or_kleene(both_null, values_below_threshold),
            consts.VALIDATION_STATUS_SUCCESS,
            consts.VALIDATION_STATUS_FAIL,
        )
    return is_successful, difference, pct_difference


def _fillcolumn_string_scalar(value, num_rows) -> pa.ChunkedArray:
    # helper function to build a column from scalar
    return pa.repeat(pa.scalar(value, type=pa.string()), num_rows)


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


def _join_source_and_target(
    source_table: pa.Table,
    target_table: pa.Table,
    pk_columns,
    join_on_fields,
) -> pa.Table:
    """Rename value columns, align key types with NULL-safe flags, and full-outer-join."""
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
    # Align PK / group-by columns to a common compatible Arrow type and pair each with
    # an is-null flag so NULL == NULL matches in the join without sentinel collisions.
    join_keys = list(pk_columns)
    for col in join_on_fields:
        src_col, tgt_col = _align_key_columns(
            source_renamed[col], target_renamed[col], col
        )
        is_null_col = f"__dvt_is_null_{col}"
        dummy = _dummy_fill_scalar(src_col.type)

        src_idx = source_renamed.schema.get_field_index(col)
        source_renamed = source_renamed.set_column(
            src_idx, col, pc.fill_null(src_col, dummy)
        )
        source_renamed = source_renamed.append_column(is_null_col, pc.is_null(src_col))

        tgt_idx = target_renamed.schema.get_field_index(col)
        target_renamed = target_renamed.set_column(
            tgt_idx, col, pc.fill_null(tgt_col, dummy)
        )
        target_renamed = target_renamed.append_column(is_null_col, pc.is_null(tgt_col))

        join_keys.append(is_null_col)

    source_renamed = source_renamed.append_column(
        "__dvt_source_present",
        pa.repeat(pa.scalar(True, type=pa.bool_()), source_renamed.num_rows),
    )
    target_renamed = target_renamed.append_column(
        "__dvt_target_present",
        pa.repeat(pa.scalar(True, type=pa.bool_()), target_renamed.num_rows),
    )
    return source_renamed.join(target_renamed, keys=join_keys, join_type="full outer")


def join_and_unpivot(
    run_metadata: "RunMetadata",
    source_table: pa.Table,
    target_table: pa.Table,
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> pa.Table:
    """Join source and target Arrow tables on keys and unpivot each validation into report rows."""
    if (
        not join_on_fields
    ):  # Plain column validation without a group by, has only one row, we inject a fake join key
        # An aggregate query without a GROUP BY always returns exactly one row, even on an
        # empty table. Any other row count fails loudly.
        if source_table.num_rows != 1 or target_table.num_rows != 1:
            raise ValueError(
                "Expected exactly one row for column validation, received "
                f"{source_table.num_rows} and {target_table.num_rows}"
            )
        source_table = source_table.add_column(0, "__dvt_fake_pk", [[0]])
        target_table = target_table.add_column(0, "__dvt_fake_pk", [[0]])
        pk_columns = ["__dvt_fake_pk"]
    else:
        pk_columns = join_on_fields

    # 1. Each Value column represents a validation. We will process each column and generate a slice
    #    of the validation report
    value_columns = [col for col in source_table.column_names if col not in pk_columns]

    # 2. Rename value columns so they don't collide, then join wide tables on PKs
    aligned = _join_source_and_target(
        source_table, target_table, pk_columns, join_on_fields
    )
    num_rows = aligned.num_rows
    slices = []
    run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)

    # If the number of rows is null (usually row validation) or there are no value
    # columns, return an empty table
    if num_rows == 0 or not value_columns:
        return _empty_report_table(run_metadata)

    # 3. Many columns in the report are common across all validations, compute these once
    run_id_col = pa.repeat(run_metadata.run_id, num_rows)
    labels_col = pa.repeat(run_metadata.labels, num_rows)
    start_time_col = pa.repeat(run_metadata.start_time, num_rows)
    end_time_col = pa.repeat(run_metadata.end_time, num_rows)
    # Column of null values, created once and reused
    string_nulls_col = pa.nulls(num_rows, type=pa.string())
    float_nulls_col = pa.nulls(num_rows, type=pa.float64())
    both_present = pc.and_kleene(
        pc.fill_null(aligned["__dvt_source_present"], False),
        pc.fill_null(aligned["__dvt_target_present"], False),
    )

    # validation result needs to contain primary key/group by values for status - if present
    if join_on_fields:
        pk_cols = {}
        for col in join_on_fields:
            is_null_col = f"__dvt_is_null_{col}"
            restored = pc.if_else(
                aligned[is_null_col],
                pa.scalar(None, type=aligned[col].type),
                aligned[col],
            )
            pk_cols[col] = _format_key_value(restored)
        group_by_cols = pa.array(
            [json.dumps(row) for row in pa.table(pk_cols).to_pylist()],
            type=pa.string(),
        )
    else:
        group_by_cols = string_nulls_col

    # 4. Loop over each value column and compare using native Arrow types
    #    and create the report for that specific validation.
    for col in value_columns:
        validation = run_metadata.validations[col]
        src_col = aligned[f"{col}_source"]
        tgt_col = aligned[f"{col}_target"]

        # Row validations has nulls for differences and column validation has nulls for primary_keys.
        # validation.primary_keys keeps the user's key order, join_on_fields is a set.
        if is_value_comparison:
            primary_keys = pa.repeat(
                (
                    "{" + ", ".join(validation.primary_keys) + "}"
                    if validation.primary_keys
                    else None
                ),
                num_rows,
            )
        else:
            primary_keys = string_nulls_col

        src_agg_val = _format_value(src_col)
        tgt_agg_val = _format_value(tgt_col)
        threshold = pa.repeat(
            (
                validation.threshold
                if abs(validation.threshold) > consts.VALIDATION_PCT_SENSITIVITY
                else consts.VALIDATION_PCT_SENSITIVITY
            ),
            num_rows,
        )

        # With comparision-fields (row validation) or aggregates (column validation)
        # the src_col can be of integer type and the tgt_col can be of decimal type.
        # pyArrow comparisons will work with mixed types, however some customers
        # have reported issues with very large decimal values, when the number of decimal
        # digits is at the max. Hence making such comparisons safe here.
        if (
            pa.types.is_decimal(src_col.type)
            or pa.types.is_decimal(tgt_col.type)
            or pa.types.is_uint64(src_col.type)
            or pa.types.is_uint64(tgt_col.type)
        ) and not (is_value_comparison and src_col.type == tgt_col.type):
            src_col, tgt_col = _safe_decimal_type(src_col, tgt_col)
        if is_value_comparison:  # row validation - compare values for equality
            # With Hash and concat, the column values will always be non null
            # With comparison-fields, we can have nulls, so the check here null==null as success.
            # Native type equality check (works on int64, decimal, string, timestamp, etc.)
            src_col, tgt_col = _naive_timestamp_as_utc(src_col, tgt_col)
            is_successful, difference, pct_difference = _exact_comparison(
                src_col, tgt_col, float_nulls_col, both_present=both_present
            )
        elif _is_numeric_type(src_col) and _is_numeric_type(
            tgt_col
        ):  # aggregated values, compare % difference against threshold
            is_successful, difference, pct_difference = _compare_numeric_aggregate(
                src_col,
                tgt_col,
                validation,
                threshold,
                float_nulls_col,
                both_present=both_present,
            )
        elif _is_string_type(src_col) and _is_string_type(tgt_col):
            # column aggregates should generally be numeric. Where we allow aggregates like
            # max, min of strings, uuid, binary, we have to set difference and pct_difference
            # to null and just provide success/fail. We also have to handle null == null as
            # success.
            is_successful, difference, pct_difference = _exact_comparison(
                src_col, tgt_col, float_nulls_col, both_present=both_present
            )
        else:
            raise ValueError(f"Aggregate value {col} must be numeric or string")

        # Reset threshold so the value of the sensitivity does not confuse users
        threshold = pa.repeat(validation.threshold, num_rows)

        # Build the report slice for this validation
        validation_name_col = pa.repeat(col, num_rows)
        col_slice = pa.table(
            {
                consts.VALIDATION_NAME: validation_name_col,
                consts.VALIDATION_TYPE: pa.repeat(validation.validation_type, num_rows),
                consts.AGGREGATION_TYPE: _fillcolumn_string_scalar(
                    validation.aggregation_type, num_rows
                ),
                consts.SOURCE_TABLE_NAME: _fillcolumn_string_scalar(
                    validation.get_table_name(consts.RESULT_TYPE_SOURCE), num_rows
                ),
                consts.SOURCE_COLUMN_NAME: _fillcolumn_string_scalar(
                    validation.get_column_name(consts.RESULT_TYPE_SOURCE), num_rows
                ),
                consts.SOURCE_AGG_VALUE: src_agg_val,
                consts.TARGET_TABLE_NAME: _fillcolumn_string_scalar(
                    validation.get_table_name(consts.RESULT_TYPE_TARGET), num_rows
                ),
                consts.TARGET_COLUMN_NAME: _fillcolumn_string_scalar(
                    validation.get_column_name(consts.RESULT_TYPE_TARGET), num_rows
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
    # 5. Stack all per-column slices into the final unpivoted table
    return pa.concat_tables(slices)


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
