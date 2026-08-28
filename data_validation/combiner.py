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
import functools
import json
import logging
from typing import TYPE_CHECKING

import ibis
import ibis.expr.datatypes as dt
import pandas

from data_validation import consts

if TYPE_CHECKING:
    from pandas import DataFrame
    import ibis.expr.types.relations.table as IbisTable
    from data_validation.metadata import RunMetadata, ValidationMetadata


# At around 140 columns we hit RecursionError when unioning Ibis subqueries.
# This constant is a threshold at which we slice up the input Dataframes
# and stitch them back together again after Ibis processing.
COMBINER_COLUMN_SLICE_WIDTH = 60

COMBINER_GET_SUMMARY_EXC_TEXT = (
    "Error while generating summary report of row validation results"
)

_MAX_INT64 = 9223372036854775807
_MIN_INT64 = -9223372036854775808


def _convert_large_ints_to_decimals(df: "DataFrame") -> "DataFrame":
    """Casts out-of-bounds 64-bit python int values to decimal.Decimal objects.

    This prevents PyArrow from failing with `OverflowError: Python int too large to convert to C long`
    during table inference, by converting integers that exceed 64-bit limits (e.g., 20-digit or 38-digit
    integers returned by some database drivers) to decimals inside Pandas DataFrames prior to loading
    them into the in-memory pandas client.
    """
    df_copied = False
    for col in df.columns:
        if df[col].dtype == object:
            converted = False

            def convert_if_large(x):
                nonlocal converted
                if type(x) is int and (x > _MAX_INT64 or x < _MIN_INT64):
                    converted = True
                    return decimal.Decimal(str(x))
                return x

            new_col = df[col].apply(convert_if_large)

            if converted:
                if not df_copied:
                    df = df.copy()
                    df_copied = True
                df[col] = new_col
    return df


def generate_report(
    run_metadata: "RunMetadata",
    source_df: "DataFrame",
    target_df: "DataFrame",
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> "DataFrame":
    import sys

    sys.setrecursionlimit(max(sys.getrecursionlimit(), 20000))
    """Combine results into a report.

    This function is a wrapper around _generate_report_slice(). _generate_report_slice() does the main work, this
    wrapper simply manages the input columns and stitches the results back together.
    This is because validations of > 140(ish) columns trigger a RecursionError when unioning Ibis subqueries.
    In this method we pass in column slices of the incoming Dataframes and combine the results.

    It is a bit of a hack but I cannot find a way to optimize the Ibis processing. It appears to be
    inefficient in that we create a subquery for each validation (column) in _calculate_differences() and
    then union them all. We then do the same on the source/target table expressions to join it all back
    together again. I (nj1973) spent a singificant amount of time trying to understand/optimize the Ibis
    processing but fell back on this simpler (less risky) workaround.

    Returns:
        pandas.DataFrame:
            A pandas DataFrame with the results of the validation in the same
            schema as the report table.
    """
    source_df = _convert_large_ints_to_decimals(source_df)
    target_df = _convert_large_ints_to_decimals(target_df)

    _check_schema_names(source_df, target_df)

    join_on_fields = tuple(join_on_fields)

    validation_columns = list(run_metadata.validations.keys())

    result_df = None
    # Process the input Dataframes in slices of columns to avoid "RecursionError"s.
    for slice_start in range(0, len(validation_columns), COMBINER_COLUMN_SLICE_WIDTH):
        columns_in_vertical_slice = validation_columns[
            slice_start : slice_start + COMBINER_COLUMN_SLICE_WIDTH
        ]
        # Ensure any join columns are included in the column slice.
        columns_in_vertical_slice.extend(
            set(join_on_fields) - set(columns_in_vertical_slice)
        )
        interim_result_df = _generate_report_slice(
            run_metadata,
            source_df[columns_in_vertical_slice],
            target_df[columns_in_vertical_slice],
            join_on_fields=join_on_fields,
            is_value_comparison=is_value_comparison,
            verbose=verbose,
        )
        if result_df is None:
            result_df = interim_result_df
        else:
            result_df = pandas.concat([result_df, interim_result_df], ignore_index=True)

    # Get the first validation metadata object to fill source and/or target empty table names.
    first = run_metadata.validations[next(iter(run_metadata.validations))]
    if first.validation_type != consts.CUSTOM_QUERY:
        result_df["source_table_name"] = result_df["source_table_name"].fillna(
            first.get_table_name(consts.RESULT_TYPE_SOURCE)
        )
        result_df["target_table_name"] = result_df["target_table_name"].fillna(
            first.get_table_name(consts.RESULT_TYPE_TARGET)
        )

    _get_summary(run_metadata, result_df, source_df, target_df)

    return result_df


def _sanitize_df_for_duckdb(df: pandas.DataFrame) -> pandas.DataFrame:
    df_copied = False
    if df.empty:
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype("string")
        return df

    # DuckDB's native pandas/arrow integration requires timedelta64 to be nanosecond precision ('timedelta64[ns]')
    for col in df.columns:
        if (
            pandas.api.types.is_timedelta64_dtype(df[col])
            and df[col].dtype != "timedelta64[ns]"
        ):
            if not df_copied:
                df = df.copy()
                df_copied = True
            df[col] = df[col].astype("timedelta64[ns]")

    obj_cols = [col for col in df.columns if df[col].dtype == object]
    if obj_cols:
        null_cols = [col for col in obj_cols if df[col].isnull().all()]
        if null_cols:
            if not df_copied:
                df = df.copy()
                df_copied = True
            for col in null_cols:
                df[col] = df[col].astype("string")
    return df


def _generate_report_slice(
    run_metadata: "RunMetadata",
    source_df: "DataFrame",
    target_df: "DataFrame",
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> "DataFrame":
    """Combine results into a report.

    Args:
        run_metadata: Metadata about the run and validations.
        source_df: Dataframe contains results of source query.
        target_df: Dataframe contains results of target query.
        join_on_fields (Sequence[str]):
            A collection of column names to use to join source and target.
            These are the columns that both the source and target queries
            are grouped by.
        is_value_comparison (boolean): Boolean representing if source and
            target agg values should be compared with 'equals to' rather than
            a 'difference' comparison.

    Returns:
        pandas.DataFrame:
            A pandas DataFrame with the results of the validation in the same
            schema as the report table.
    """
    con = ibis.duckdb.connect()
    con.create_table(consts.RESULT_TYPE_SOURCE, _sanitize_df_for_duckdb(source_df))
    con.create_table(consts.RESULT_TYPE_TARGET, _sanitize_df_for_duckdb(target_df))
    source = con.table(consts.RESULT_TYPE_SOURCE)
    target = con.table(consts.RESULT_TYPE_TARGET)

    differences_pivot = _calculate_differences(
        source, target, join_on_fields, run_metadata.validations, is_value_comparison
    )
    source_pivot = _pivot_result(
        source, join_on_fields, run_metadata.validations, consts.RESULT_TYPE_SOURCE
    )
    target_pivot = _pivot_result(
        target, join_on_fields, run_metadata.validations, consts.RESULT_TYPE_TARGET
    )

    joined = _join_pivots(source_pivot, target_pivot, differences_pivot, join_on_fields)

    documented, run_metadata = _add_metadata(joined, run_metadata)

    if verbose:
        logging.debug("-- ** Combiner Query ** --")
        logging.debug(documented.compile())

    result_df = con.execute(documented)
    result_df["validation_status"] = result_df["validation_status"].fillna(
        consts.VALIDATION_STATUS_FAIL
    )
    if consts.NUM_RANDOM_ROWS in result_df.columns:
        result_df[consts.NUM_RANDOM_ROWS] = (
            result_df[consts.NUM_RANDOM_ROWS]
            .where(result_df[consts.NUM_RANDOM_ROWS].notnull(), None)
            .astype(object)
        )
    return result_df


def _calculate_difference(
    field_differences: "IbisTable",
    datatype: dt.DataType,
    target_type: dt.DataType,
    validation: "ValidationMetadata",
    is_value_comparison: bool,
):
    pct_threshold = ibis.literal(validation.threshold)
    if datatype.is_timestamp() or datatype.is_date():
        source_value = (
            field_differences["differences_source_value"].epoch_seconds().cast("int64")
        )
        target_value = (
            field_differences["differences_target_value"].epoch_seconds().cast("int64")
        )
    elif datatype.is_boolean() or (target_type and target_type.is_boolean()):
        source_value = field_differences["differences_source_value"].cast("boolean")
        target_value = field_differences["differences_target_value"].cast("boolean")
    elif datatype.is_decimal() or datatype.is_float64():
        source_value = (
            field_differences["differences_source_value"]
            .cast("float32")
            .round(digits=4)
        )
        target_value = (
            field_differences["differences_target_value"]
            .cast("float32")
            .round(digits=4)
        )
    else:
        source_value = field_differences["differences_source_value"]
        target_value = field_differences["differences_target_value"]

    # Does not calculate difference between agg values for row hash due to int64 overflow
    if (
        is_value_comparison
        or datatype.is_string()
        or isinstance(target_value, ibis.expr.types.generic.NullColumn)
        or isinstance(source_value, ibis.expr.types.generic.NullColumn)
    ):
        # String data types i.e "None" can be returned for NULL timestamp/datetime aggs
        difference = pct_difference = ibis.null().cast("float64")
        is_source_null = (
            source_value.isnull()
            | (source_value.cast("string") == "<NA>")
            | (source_value.cast("string") == "nan")
            | (source_value.cast("string") == "None")
            | (source_value.cast("string") == "NULL")
        )
        is_target_null = (
            target_value.isnull()
            | (target_value.cast("string") == "<NA>")
            | (target_value.cast("string") == "nan")
            | (target_value.cast("string") == "None")
            | (target_value.cast("string") == "NULL")
        )
        validation_status = (
            ibis.case()
            .when(
                is_target_null & is_source_null,
                consts.VALIDATION_STATUS_SUCCESS,
            )
            .when(
                target_value.cast("string") == source_value.cast("string"),
                consts.VALIDATION_STATUS_SUCCESS,
            )
            .else_(consts.VALIDATION_STATUS_FAIL)
            .end()
        )
    else:
        difference = (target_value - source_value).cast("float64")

        pct_difference_nonzero = (
            ibis.literal(100.0)
            * difference.cast("float32")
            / (
                source_value.case()
                .when(ibis.literal(0), target_value)
                .else_(source_value)
                .end()
            ).cast("float64")
        ).cast("float64")

        # Considers case that source and target agg values can both be 0
        pct_difference = (
            ibis.case()
            .when(difference == ibis.literal(0), ibis.literal(0).cast("float64"))
            .else_(pct_difference_nonzero)
            .end()
        )

        th_diff = (pct_difference.abs() - pct_threshold).cast("float64")
        validation_status = (
            ibis.case()
            .when(
                source_value.isnull() & target_value.isnull(),
                consts.VALIDATION_STATUS_SUCCESS,
            )
            .when(
                th_diff.isnan() | th_diff.isnull() | (th_diff > 0.0),
                consts.VALIDATION_STATUS_FAIL,
            )
            .else_(consts.VALIDATION_STATUS_SUCCESS)
            .end()
        )
    return (
        difference.name(consts.VALIDATION_DIFFERENCE),
        pct_difference.name(consts.VALIDATION_PCT_DIFFERENCE),
        pct_threshold.name(consts.VALIDATION_PCT_THRESHOLD),
        validation_status.name(consts.VALIDATION_STATUS),
    )


def _calculate_differences(
    source: "IbisTable",
    target: "IbisTable",
    join_on_fields: tuple,
    validations: "dict[ValidationMetadata]",
    is_value_comparison: bool,
):
    """Calculate differences between source and target fields.

    This function is separate from the "pivot" logic because we want to
    calculate the differences with the original data type before casting to a
    floating point value. The pivot casts all values to string, so the
    difference calculation would fail if done after that step.
    """
    schema = source.schema()
    target_schema = target.schema()

    if join_on_fields:
        # Use an inner join because a row must be present in source and target
        # for the difference to be well defined.
        predicates = [source[k].identical_to(target[k]) for k in join_on_fields]
        differences_joined = source.join(target, predicates, how="inner")
    else:
        # When no join_on_fields are present, we expect only one row per table.
        # This is validated in generate_report before this function is called.
        differences_joined = source.cross_join(target)
    differences_pivots = []
    for field, field_type in schema.items():
        if field not in validations:
            continue
        target_type = target_schema.get(field, None)
        validation = validations[field]
        field_differences = differences_joined.projection(
            [
                source[field].name("differences_source_value"),
                target[field].name("differences_target_value"),
            ]
            + [source[join_field] for join_field in join_on_fields]
        )
        differences_pivots.append(
            field_differences[
                (ibis.literal(field).name(consts.VALIDATION_NAME),)
                + join_on_fields
                + _calculate_difference(
                    field_differences,
                    field_type,
                    target_type,
                    validation,
                    is_value_comparison,
                )
            ]
        )
    differences_pivot = functools.reduce(
        lambda pivot1, pivot2: pivot1.union(pivot2), differences_pivots
    )
    return differences_pivot


def _check_schema_names(source_df, target_df):
    """Check that the two input Dataframes have matching column names."""
    source_names = tuple(source_df.columns)
    target_names = tuple(target_df.columns)

    if source_names != target_names:
        raise ValueError(
            "Expected source and target to have same schema, got "
            f"{consts.RESULT_TYPE_SOURCE}: {source_names}; {consts.RESULT_TYPE_TARGET}: {target_names}"
        )


def _pivot_result(
    result: "IbisTable",
    join_on_fields: tuple,
    validations: "dict[ValidationMetadata]",
    result_type: str,
):
    all_fields = frozenset(result.schema().names)
    validation_fields = (
        all_fields - frozenset(join_on_fields)
        if "hash__all" not in join_on_fields
        else all_fields
    )
    pivots = []
    for field in validation_fields:
        if field not in validations:
            continue
        else:
            validation = validations[field]
            if validation.primary_keys:
                primary_keys = (
                    ibis.literal("{")
                    + ibis.literal(", ").join(validation.primary_keys)
                    + ibis.literal("}")
                ).name(consts.CONFIG_PRIMARY_KEYS)
            else:
                primary_keys = (
                    ibis.literal(None).cast("string").name(consts.CONFIG_PRIMARY_KEYS)
                )

            pivots.append(
                result.projection(
                    (
                        ibis.literal(field).name(consts.VALIDATION_NAME),
                        ibis.literal(validation.validation_type).name(
                            consts.VALIDATION_TYPE
                        ),
                        ibis.literal(validation.aggregation_type).name(
                            consts.AGGREGATION_TYPE
                        ),
                        ibis.literal(validation.get_table_name(result_type)).name(
                            consts.COMBINER_TABLE_NAME
                        ),
                        # Cast to string to ensure types match, even when column
                        # name is NULL (such as for count aggregations).
                        ibis.literal(validation.get_column_name(result_type))
                        .cast("string")
                        .name(consts.COMBINER_COLUMN_NAME),
                        primary_keys,
                        ibis.literal(validation.num_random_rows)
                        .cast("int64")
                        .name(consts.NUM_RANDOM_ROWS),
                        _cast_agg_value(result[field]).name(consts.COMBINER_AGG_VALUE),
                    )
                    + join_on_fields
                )
            )
    pivot = functools.reduce(lambda pivot1, pivot2: pivot1.union(pivot2), pivots)
    return pivot


def _cast_agg_value(expr):
    """Cast agg value to string, handling timezone offsets and nan/null conversion."""
    casted = expr.cast("string")
    if expr.type().is_timestamp():
        casted = casted.re_replace(r" 00:00:00(\.0+)?$", "")
        casted = casted.re_replace(r"\+00$", "+00:00")
    return casted.fillna("nan")


def _as_json(expr):
    """Make field value into valid string.

    https://stackoverflow.com/a/3020108/101923
    """
    casted = expr.cast("string")
    if expr.type().is_timestamp():
        casted = casted.re_replace(r" 00:00:00(\.0+)?$", "")
        casted = casted.re_replace(r"\+00$", "+00:00")
    return casted.fillna("null").re_replace(r"\\", r"\\\\").re_replace('"', r'\\"')


def _join_pivots(
    source: "IbisTable",
    target: "IbisTable",
    differences: "IbisTable",
    join_on_fields: tuple,
):
    join_keys = (consts.VALIDATION_NAME,) + join_on_fields
    source_diff_predicates = [source[k].identical_to(differences[k]) for k in join_keys]
    source_difference = source.join(differences, source_diff_predicates, how="outer")[
        [source[field] for field in join_keys]
        + [
            source[consts.VALIDATION_TYPE],
            source[consts.AGGREGATION_TYPE],
            source[consts.COMBINER_TABLE_NAME],
            source[consts.COMBINER_COLUMN_NAME],
            source[consts.CONFIG_PRIMARY_KEYS],
            source[consts.NUM_RANDOM_ROWS],
            source[consts.COMBINER_AGG_VALUE],
            differences[consts.VALIDATION_DIFFERENCE],
            differences[consts.VALIDATION_PCT_DIFFERENCE],
            differences[consts.VALIDATION_PCT_THRESHOLD],
            differences[consts.VALIDATION_STATUS],
        ]
    ]

    if join_on_fields:
        join_values = []
        for field in join_on_fields:
            coalesced_field = source_difference[field].fillna(target[field])
            join_values.append(
                ibis.literal(json.dumps(field))
                + ibis.literal(': "')
                + _as_json(coalesced_field)
                + ibis.literal('"')
            )

        group_by_columns = (
            ibis.literal("{") + ibis.literal(", ").join(join_values) + ibis.literal("}")
        ).name(consts.GROUP_BY_COLUMNS)
    else:
        group_by_columns = (
            ibis.literal(None).cast("string").name(consts.GROUP_BY_COLUMNS)
        )

    target_predicates = [
        source_difference[k].identical_to(target[k]) for k in join_keys
    ]
    joined = source_difference.join(target, target_predicates, how="outer")[
        source_difference[consts.VALIDATION_NAME]
        .fillna(target[consts.VALIDATION_NAME])
        .name(consts.VALIDATION_NAME),
        source_difference[consts.VALIDATION_TYPE]
        .fillna(target[consts.VALIDATION_TYPE])
        .name(consts.VALIDATION_TYPE),
        source_difference[consts.AGGREGATION_TYPE]
        .fillna(target[consts.AGGREGATION_TYPE])
        .name(consts.AGGREGATION_TYPE),
        source_difference[consts.COMBINER_TABLE_NAME].name(consts.SOURCE_TABLE_NAME),
        source_difference[consts.COMBINER_COLUMN_NAME].name(consts.SOURCE_COLUMN_NAME),
        source_difference[consts.COMBINER_AGG_VALUE].name(consts.SOURCE_AGG_VALUE),
        target[consts.COMBINER_TABLE_NAME].name(consts.TARGET_TABLE_NAME),
        target[consts.COMBINER_COLUMN_NAME].name(consts.TARGET_COLUMN_NAME),
        target[consts.COMBINER_AGG_VALUE].name(consts.TARGET_AGG_VALUE),
        group_by_columns,
        source_difference[consts.CONFIG_PRIMARY_KEYS]
        .fillna(target[consts.CONFIG_PRIMARY_KEYS])
        .name(consts.CONFIG_PRIMARY_KEYS),
        source_difference[consts.NUM_RANDOM_ROWS]
        .fillna(target[consts.NUM_RANDOM_ROWS])
        .name(consts.NUM_RANDOM_ROWS),
        source_difference[consts.VALIDATION_DIFFERENCE],
        source_difference[consts.VALIDATION_PCT_DIFFERENCE],
        source_difference[consts.VALIDATION_PCT_THRESHOLD],
        source_difference[consts.VALIDATION_STATUS],
    ]
    return joined


def _add_metadata(joined: "IbisTable", run_metadata: "RunMetadata"):
    # TODO: Add source and target queries to metadata
    run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)

    joined = joined[
        joined,
        ibis.literal(run_metadata.run_id).name(consts.CONFIG_RUN_ID),
        ibis.literal(run_metadata.labels).name(consts.CONFIG_LABELS),
        ibis.literal(run_metadata.start_time).name(consts.CONFIG_START_TIME),
        ibis.literal(run_metadata.end_time).name(consts.CONFIG_END_TIME),
    ]

    return (joined, run_metadata)


def _log_row_validation_summary(
    run_metadata: "RunMetadata",
    result_df: "DataFrame",
    source_df: "DataFrame",
    target_df: "DataFrame",
):
    """Logs a summary report/stats of row validation results."""
    success_condition = (
        result_df[consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_SUCCESS
    )
    fail_condition = ~success_condition

    source_not_in_target = (
        result_df[consts.SOURCE_AGG_VALUE].notnull()
        & result_df[consts.TARGET_AGG_VALUE].isnull()
    )
    target_not_in_source = (
        result_df[consts.SOURCE_AGG_VALUE].isnull()
        & result_df[consts.TARGET_AGG_VALUE].notnull()
    )
    present_in_both_tables = (
        result_df[consts.SOURCE_AGG_VALUE].notnull()
        & result_df[consts.TARGET_AGG_VALUE].notnull()
    )

    logging.info(
        json.dumps(
            {
                consts.CONFIG_RUN_ID: run_metadata.run_id,
                consts.CONFIG_START_TIME: run_metadata.start_time.isoformat(),
                consts.CONFIG_END_TIME: run_metadata.end_time.isoformat(),
                consts.TOTAL_SOURCE_ROWS: int(source_df.shape[0]),
                consts.TOTAL_TARGET_ROWS: int(target_df.shape[0]),
                consts.TOTAL_ROWS_VALIDATED: int(result_df.shape[0]),
                consts.TOTAL_ROWS_SUCCESS: int(success_condition.sum()),
                consts.TOTAL_ROWS_FAIL: int(fail_condition.sum()),
                consts.FAILED_SOURCE_NOT_IN_TARGET: int(
                    (fail_condition & source_not_in_target).sum()
                ),
                consts.FAILED_TARGET_NOT_IN_SOURCE: int(
                    (fail_condition & target_not_in_source).sum()
                ),
                consts.FAILED_PRESENT_IN_BOTH_TABLES: int(
                    (fail_condition & present_in_both_tables).sum()
                ),
            }
        )
    )


def _log_validation_summary(
    run_metadata: "RunMetadata",
    result_df: "DataFrame",
):
    """Logs a summary report/stats of validation results."""
    success_condition = (
        result_df[consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_SUCCESS
    )
    fail_condition = ~success_condition

    logging.info(
        json.dumps(
            {
                consts.CONFIG_RUN_ID: run_metadata.run_id,
                consts.CONFIG_START_TIME: run_metadata.start_time.isoformat(),
                consts.CONFIG_END_TIME: run_metadata.end_time.isoformat(),
                consts.TOTAL_VALIDATIONS: int(result_df.shape[0]),
                consts.TOTAL_VALIDATIONS_SUCCESS: int(success_condition.sum()),
                consts.TOTAL_VALIDATIONS_FAIL: int(fail_condition.sum()),
            }
        )
    )


def _get_summary(
    run_metadata: "RunMetadata",
    result_df: "DataFrame",
    source_df: "DataFrame",
    target_df: "DataFrame",
):
    """Logs a summary report/stats of validation results."""
    try:
        if result_df.empty:
            return

        if (result_df.loc[0, consts.VALIDATION_TYPE] == consts.ROW_VALIDATION) or (
            # Check for custom-query row validation which always should have primary keys (not null)
            result_df.loc[0, consts.VALIDATION_TYPE] == consts.CUSTOM_QUERY
            and result_df.loc[0, consts.CONFIG_PRIMARY_KEYS]
        ):
            _log_row_validation_summary(run_metadata, result_df, source_df, target_df)
        else:
            # If we don't output the specific row validation INFO line, output a generic summary.
            _log_validation_summary(run_metadata, result_df)
    except Exception as e:
        logging.warning(
            f"{COMBINER_GET_SUMMARY_EXC_TEXT}: {e}",
            exc_info=True,
        )
