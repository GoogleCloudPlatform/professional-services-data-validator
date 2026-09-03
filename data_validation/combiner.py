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

import pyarrow
import pyarrow.compute as pc

from data_validation import consts

if TYPE_CHECKING:
    from pandas import DataFrame
    from data_validation.metadata import RunMetadata


COMBINER_GET_SUMMARY_EXC_TEXT = (
    "Error while generating summary report of row validation results"
)


def generate_report(
    run_metadata: "RunMetadata",
    source_table: pyarrow.Table,
    target_table: pyarrow.Table,
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
    _check_schema_names(source_table, target_table)

    join_on_fields = tuple(join_on_fields)
    report_table = _generate_report_slice(
        run_metadata,
        source_table,
        target_table,
        join_on_fields=join_on_fields,
        is_value_comparison=is_value_comparison,
        verbose=verbose,
    )

    # Get the first validation metadata object to fill source and/or target empty table names.
    first = run_metadata.validations[next(iter(run_metadata.validations))]
    if first.validation_type != consts.CUSTOM_QUERY:
        report_table = report_table.set_column(
            report_table.schema.get_field_index(consts.SOURCE_TABLE_NAME),
            consts.SOURCE_TABLE_NAME,
            pc.fill_null(
                report_table[consts.SOURCE_TABLE_NAME],
                first.get_table_name(consts.RESULT_TYPE_SOURCE),
            ),
        )
        report_table = report_table.set_column(
            report_table.schema.get_field_index(consts.TARGET_TABLE_NAME),
            consts.TARGET_TABLE_NAME,
            pc.fill_null(
                report_table[consts.TARGET_TABLE_NAME],
                first.get_table_name(consts.RESULT_TYPE_TARGET),
            ),
        )

    _get_summary(run_metadata, report_table, source_table, target_table)

    result_df = report_table.to_pandas(
        timestamp_as_object=False, coerce_temporal_nanoseconds=True
    )
    if consts.NUM_RANDOM_ROWS in result_df.columns:
        result_df[consts.NUM_RANDOM_ROWS] = (
            result_df[consts.NUM_RANDOM_ROWS]
            .where(result_df[consts.NUM_RANDOM_ROWS].notnull(), None)
            .astype(object)
        )
    return result_df


def _generate_report_slice(
    run_metadata: "RunMetadata",
    source_table: pyarrow.Table,
    target_table: pyarrow.Table,
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> pyarrow.Table:
    """Combine results into a report.

    Args:
        run_metadata: Metadata about the run and validations.
        source_table: PyArrow table containing source query results.
        target_table: PyArrow table containing target query results.
        join_on_fields (Sequence[str]):
            A collection of column names to use to join source and target.
            These are the columns that both the source and target queries
            are grouped by.
        is_value_comparison (boolean): Boolean representing if source and
            target agg values should be compared with 'equals to' rather than
            a 'difference' comparison.

    Returns:
        pyarrow.Table: The results of the validation in the report-table schema.
    """
    aligned = _align_tables(source_table, target_table, join_on_fields)
    reports = [
        _report_for_validation(
            aligned, field, validation, join_on_fields, is_value_comparison
        )
        for field, validation in run_metadata.validations.items()
        if field in source_table.column_names
    ]
    report_table = pyarrow.concat_tables(reports)
    report_table = _add_metadata(report_table, run_metadata)

    if verbose:
        logging.debug("-- ** Combiner Report Schema ** --")
        logging.debug(report_table.schema)

    return report_table


def _check_schema_names(source_table: pyarrow.Table, target_table: pyarrow.Table):
    """Check that the two input tables have matching column names."""
    source_names = tuple(source_table.column_names)
    target_names = tuple(target_table.column_names)

    if source_names != target_names:
        raise ValueError(
            "Expected source and target to have same schema, got "
            f"{consts.RESULT_TYPE_SOURCE}: {source_names}; {consts.RESULT_TYPE_TARGET}: {target_names}"
        )


def _align_tables(source_table, target_table, join_on_fields):
    source = _rename_columns(source_table, consts.RESULT_TYPE_SOURCE)
    target = _rename_columns(target_table, consts.RESULT_TYPE_TARGET)
    if join_on_fields:
        join_keys = []
        for field in join_on_fields:
            key_name = f"__dvt_join_{field}"
            null_marker = f"__dvt_null_{field}"
            source = source.append_column(
                key_name,
                pc.fill_null(
                    pc.cast(source[f"{field}_source"], pyarrow.string()), null_marker
                ),
            )
            target = target.append_column(
                key_name,
                pc.fill_null(
                    pc.cast(target[f"{field}_target"], pyarrow.string()), null_marker
                ),
            )
            join_keys.append(key_name)
        return source.join(target, keys=join_keys, join_type="full outer")

    cross_join_key = "__dvt_cross_join_key"
    source = source.append_column(
        cross_join_key, pyarrow.array([True] * source.num_rows)
    )
    target = target.append_column(
        cross_join_key, pyarrow.array([True] * target.num_rows)
    )
    return source.join(target, keys=cross_join_key, join_type="inner")


def _rename_columns(table, result_type):
    return table.rename_columns(
        [f"{name}_{result_type}" for name in table.column_names]
    )


def _report_for_validation(
    aligned, field, validation, join_on_fields, is_value_comparison
):
    source_value = aligned[f"{field}_{consts.RESULT_TYPE_SOURCE}"]
    target_value = aligned[f"{field}_{consts.RESULT_TYPE_TARGET}"]
    source_type = source_value.type
    target_type = target_value.type
    source_compare, target_compare = _comparison_values(
        source_value, target_value, source_type, target_type
    )
    difference, pct_difference, validation_status = _comparison_result(
        source_compare,
        target_compare,
        source_type,
        target_type,
        validation.threshold,
        is_value_comparison,
    )
    length = aligned.num_rows
    return pyarrow.table(
        {
            consts.VALIDATION_NAME: pyarrow.array(
                [field] * length, type=pyarrow.string()
            ),
            consts.VALIDATION_TYPE: pyarrow.array(
                [validation.validation_type] * length, type=pyarrow.string()
            ),
            consts.AGGREGATION_TYPE: pyarrow.array(
                [validation.aggregation_type] * length, type=pyarrow.string()
            ),
            consts.SOURCE_TABLE_NAME: _string_array(
                [validation.get_table_name(consts.RESULT_TYPE_SOURCE)] * length
            ),
            consts.SOURCE_COLUMN_NAME: _string_array(
                [validation.get_column_name(consts.RESULT_TYPE_SOURCE)] * length
            ),
            consts.SOURCE_AGG_VALUE: _format_value(source_value),
            consts.TARGET_TABLE_NAME: _string_array(
                [validation.get_table_name(consts.RESULT_TYPE_TARGET)] * length
            ),
            consts.TARGET_COLUMN_NAME: _string_array(
                [validation.get_column_name(consts.RESULT_TYPE_TARGET)] * length
            ),
            consts.TARGET_AGG_VALUE: _format_value(target_value),
            consts.GROUP_BY_COLUMNS: _group_by_columns(aligned, join_on_fields),
            consts.CONFIG_PRIMARY_KEYS: _string_array(
                [
                    (
                        "{" + ", ".join(validation.primary_keys) + "}"
                        if validation.primary_keys
                        else None
                    )
                ]
                * length
            ),
            consts.NUM_RANDOM_ROWS: pyarrow.array(
                [validation.num_random_rows] * length, type=pyarrow.int64()
            ),
            consts.VALIDATION_DIFFERENCE: difference,
            consts.VALIDATION_PCT_DIFFERENCE: pct_difference,
            consts.VALIDATION_PCT_THRESHOLD: pyarrow.array(
                [validation.threshold] * length, type=pyarrow.float64()
            ),
            consts.VALIDATION_STATUS: validation_status,
        }
    )


def _comparison_values(source_value, target_value, source_type, target_type):
    if pyarrow.types.is_timestamp(source_type) or pyarrow.types.is_date(source_type):
        source_value = pc.cast(
            pc.cast(source_value, pyarrow.timestamp("s")), pyarrow.int64()
        )
        target_value = pc.cast(
            pc.cast(target_value, pyarrow.timestamp("s")), pyarrow.int64()
        )
    elif pyarrow.types.is_boolean(source_type) or pyarrow.types.is_boolean(target_type):
        source_value = pc.cast(source_value, pyarrow.bool_())
        target_value = pc.cast(target_value, pyarrow.bool_())
    elif pyarrow.types.is_decimal(source_type) or pyarrow.types.is_float64(source_type):
        source_value = pc.round(pc.cast(source_value, pyarrow.float32()), ndigits=4)
        target_value = pc.round(pc.cast(target_value, pyarrow.float32()), ndigits=4)
    return source_value, target_value


def _comparison_result(
    source_value,
    target_value,
    source_type,
    target_type,
    threshold,
    is_value_comparison,
):
    compare_as_values = (
        is_value_comparison
        or pyarrow.types.is_string(source_type)
        or pyarrow.types.is_null(source_type)
        or pyarrow.types.is_null(target_type)
    )
    if compare_as_values:
        source_null = _is_null(source_value)
        target_null = _is_null(target_value)
        both_null = pc.and_kleene(source_null, target_null)
        values_equal = pc.fill_null(pc.equal(source_value, target_value), False)
        status = pc.if_else(
            pc.or_kleene(both_null, values_equal),
            consts.VALIDATION_STATUS_SUCCESS,
            consts.VALIDATION_STATUS_FAIL,
        )
        nulls = pyarrow.nulls(len(source_value), type=pyarrow.float64())
        return nulls, nulls, status

    difference = pc.cast(pc.subtract(target_value, source_value), pyarrow.float64())
    denominator = pc.if_else(pc.equal(source_value, 0), target_value, source_value)
    pct_difference_nonzero = pc.divide(
        pc.multiply(100.0, pc.cast(difference, pyarrow.float32())),
        pc.cast(denominator, pyarrow.float64()),
    )
    pct_difference = pc.if_else(pc.equal(difference, 0), 0.0, pct_difference_nonzero)
    threshold_difference = pc.subtract(pc.abs(pct_difference), threshold)
    both_null = pc.and_kleene(pc.is_null(source_value), pc.is_null(target_value))
    is_failure = pc.or_kleene(
        pc.or_kleene(pc.is_nan(threshold_difference), pc.is_null(threshold_difference)),
        pc.greater(threshold_difference, 0.0),
    )
    status = pc.if_else(
        both_null,
        consts.VALIDATION_STATUS_SUCCESS,
        pc.if_else(
            pc.fill_null(is_failure, True),
            consts.VALIDATION_STATUS_FAIL,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
    )
    return difference, pct_difference, status


def _is_null(value):
    null = pc.is_null(value)
    if not pyarrow.types.is_string(value.type):
        return null
    value_as_string = pc.cast(value, pyarrow.string())
    for null_string in ("<NA>", "nan", "None", "NULL"):
        null = pc.or_kleene(null, pc.equal(value_as_string, null_string))
    return pc.fill_null(null, False)


def _format_value(value):
    if pyarrow.types.is_binary(value.type):
        return _string_array(
            [item.hex() if item is not None else "nan" for item in value.to_pylist()]
        )
    else:
        formatted = pc.cast(value, pyarrow.string())
    if pyarrow.types.is_timestamp(value.type):
        formatted = pc.replace_substring_regex(formatted, r" 00:00:00(\.0+)?$", "")
        formatted = pc.replace_substring_regex(formatted, r"\+00$", "+00:00")
    return pc.fill_null(formatted, "nan")


def _group_by_columns(aligned, join_on_fields):
    if not join_on_fields:
        return pyarrow.nulls(aligned.num_rows, type=pyarrow.string())
    group_values = {}
    for field in join_on_fields:
        source_value = aligned[f"{field}_{consts.RESULT_TYPE_SOURCE}"]
        target_value = aligned[f"{field}_{consts.RESULT_TYPE_TARGET}"]
        group_values[field] = pc.fill_null(
            _format_value(source_value), _format_value(target_value)
        ).to_pylist()
    return pyarrow.array(
        [
            json.dumps({field: group_values[field][index] for field in join_on_fields})
            for index in range(aligned.num_rows)
        ]
    )


def _string_array(values):
    return pyarrow.array(values, type=pyarrow.string())


def _add_metadata(joined: pyarrow.Table, run_metadata: "RunMetadata"):
    length = joined.num_rows
    run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)
    return (
        joined.append_column(
            consts.CONFIG_RUN_ID, pyarrow.array([run_metadata.run_id] * length)
        )
        .append_column(
            consts.CONFIG_LABELS, pyarrow.array([run_metadata.labels] * length)
        )
        .append_column(
            consts.CONFIG_START_TIME,
            pyarrow.array([run_metadata.start_time] * length),
        )
        .append_column(
            consts.CONFIG_END_TIME,
            pyarrow.array(
                [run_metadata.end_time] * length,
                type=pyarrow.timestamp("us", tz="UTC"),
            ),
        )
    )


def _log_row_validation_summary(
    run_metadata: "RunMetadata",
    result_table: pyarrow.Table,
    source_table: pyarrow.Table,
    target_table: pyarrow.Table,
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
    result_table: pyarrow.Table,
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
    result_table: pyarrow.Table,
    source_table: pyarrow.Table,
    target_table: pyarrow.Table,
):
    """Logs a summary report/stats of validation results."""
    try:
        if not result_table.num_rows:
            return

        if (
            result_table[consts.VALIDATION_TYPE][0].as_py() == consts.ROW_VALIDATION
        ) or (
            # Check for custom-query row validation which always should have primary keys (not null)
            result_table[consts.VALIDATION_TYPE][0].as_py() == consts.CUSTOM_QUERY
            and result_table[consts.CONFIG_PRIMARY_KEYS][0].as_py()
        ):
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
