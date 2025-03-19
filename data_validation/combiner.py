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
COMBINER_COLUMN_SLICE_WIDTH = 120

COMBINER_GET_SUMMARY_EXC_TEXT = (
    "Error while generating summary report of row validation results"
)


def generate_report(
    run_metadata: "RunMetadata",
    source_df: "DataFrame",
    target_df: "DataFrame",
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
) -> "DataFrame":
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
    _check_schema_names(source_df, target_df)

    join_on_fields = tuple(join_on_fields)

    validation_columns = run_metadata.validations.keys()
    # slice_thresholds is a list of points at which we should break up the Dataframe by column.
    # e.g. [10, 20, 30] would mean process columns 0-9, 10-19 and 20-the max column.
    # 1. len(...) / COMBINER_COLUMN_SLICE_WIDTH: Divides total columns by the slice width to get the number of slices.
    # 2. int(...) + 1: int()+1 is effectively ceil() which is what we want to get the actual whole number of slices
    # 3. _ * COMBINER_COLUMN_SLICE_WIDTH: Multiplies each number by the slice width to get actual column counts for each slice.
    slice_thresholds = [
        (_ * COMBINER_COLUMN_SLICE_WIDTH)
        for _ in range(int(len(validation_columns) / COMBINER_COLUMN_SLICE_WIDTH) + 1)
    ]

    result_df = None
    # Process the input Dataframes in slices of columns to avoid "RecursionError"s.
    for slice_start in slice_thresholds:
        columns_in_vertical_slice = list(validation_columns)[
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
            result_df = pandas.concat([result_df, interim_result_df])

    # Get the first validation metadata object to fill source and/or target empty table names.
    first = run_metadata.validations[next(iter(run_metadata.validations))]
    if first.validation_type != consts.CUSTOM_QUERY:
        result_df.source_table_name.fillna(
            first.get_table_name(consts.RESULT_TYPE_SOURCE), inplace=True
        )
        result_df.target_table_name.fillna(
            first.get_table_name(consts.RESULT_TYPE_TARGET), inplace=True
        )

    _get_summary(run_metadata, result_df, source_df, target_df)

    return result_df


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
    client = ibis.pandas.connect(
        {
            consts.RESULT_TYPE_SOURCE: source_df,
            consts.RESULT_TYPE_TARGET: target_df,
        }
    )
    source = client.table(consts.RESULT_TYPE_SOURCE)
    target = client.table(consts.RESULT_TYPE_TARGET)

    differences_pivot = _calculate_differences(
        source, target, join_on_fields, run_metadata.validations, is_value_comparison
    )
    differences_df = client.execute(differences_pivot)

    source_pivot = _pivot_result(
        source, join_on_fields, run_metadata.validations, consts.RESULT_TYPE_SOURCE
    )
    source_pivot_df = client.execute(source_pivot)

    target_pivot = _pivot_result(
        target, join_on_fields, run_metadata.validations, consts.RESULT_TYPE_TARGET
    )
    target_pivot_df = client.execute(target_pivot)

    con = ibis.pandas.connect(
        {
            consts.RESULT_TYPE_SOURCE: source_pivot_df,
            consts.RESULT_TYPE_DIFFERENCES: differences_df,
            consts.RESULT_TYPE_TARGET: target_pivot_df,
        }
    )
    joined = _join_pivots(
        con.tables.source, con.tables.target, con.tables.differences, join_on_fields
    )

    documented, run_metadata = _add_metadata(joined, run_metadata)

    if verbose:
        logging.debug("-- ** Combiner Query ** --")
        logging.debug(documented.compile())

    result_df = client.execute(documented)
    result_df.validation_status.fillna(consts.VALIDATION_STATUS_FAIL, inplace=True)
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
        if is_value_comparison:
            difference = pct_difference = ibis.null()
        else:
            difference = pct_difference = ibis.null().cast("float64")
        validation_status = (
            ibis.case()
            .when(
                target_value.isnull() & source_value.isnull(),
                consts.VALIDATION_STATUS_SUCCESS,
            )
            .when(
                target_value == source_value,
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
            .when(th_diff.isnan() | (th_diff > 0.0), consts.VALIDATION_STATUS_FAIL)
            .else_(consts.VALIDATION_STATUS_SUCCESS)
            .end()
        )
    return (
        difference.name("difference"),
        pct_difference.name("pct_difference"),
        pct_threshold.name("pct_threshold"),
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
        differences_joined = source.join(target, join_on_fields, how="inner")
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
                (ibis.literal(field).name("validation_name"),)
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
                        ibis.literal(field).name("validation_name"),
                        ibis.literal(validation.validation_type).name(
                            consts.VALIDATION_TYPE
                        ),
                        ibis.literal(validation.aggregation_type).name(
                            consts.AGGREGATION_TYPE
                        ),
                        ibis.literal(validation.get_table_name(result_type)).name(
                            "table_name"
                        ),
                        # Cast to string to ensure types match, even when column
                        # name is NULL (such as for count aggregations).
                        ibis.literal(validation.get_column_name(result_type))
                        .cast("string")
                        .name("column_name"),
                        primary_keys,
                        ibis.literal(validation.num_random_rows).name(
                            "num_random_rows"
                        ),
                        result[field].cast("string").name("agg_value"),
                    )
                    + join_on_fields
                )
            )
    pivot = functools.reduce(lambda pivot1, pivot2: pivot1.union(pivot2), pivots)
    return pivot


def _as_json(expr):
    """Make field value into valid string.

    https://stackoverflow.com/a/3020108/101923
    """
    return (
        expr.cast("string")
        .fillna("null")
        .re_replace(r"\\", r"\\\\")
        .re_replace('"', '\\"')
    )


def _join_pivots(
    source: "IbisTable",
    target: "IbisTable",
    differences: "IbisTable",
    join_on_fields: tuple,
):
    if join_on_fields:
        join_values = []
        for field in join_on_fields:
            join_values.append(
                ibis.literal(json.dumps(field))
                + ibis.literal(': "')
                + _as_json(target[field])
                + ibis.literal('"')
            )

        group_by_columns = (
            ibis.literal("{") + ibis.literal(", ").join(join_values) + ibis.literal("}")
        ).name(consts.GROUP_BY_COLUMNS)
    else:
        group_by_columns = (
            ibis.literal(None).cast("string").name(consts.GROUP_BY_COLUMNS)
        )

    join_keys = ("validation_name",) + join_on_fields
    source_difference = source.join(differences, join_keys, how="outer")[
        [source[field] for field in join_keys]
        + [
            source[consts.VALIDATION_TYPE],
            source[consts.AGGREGATION_TYPE],
            source[consts.CONFIG_TABLE_NAME],
            source["column_name"],
            source[consts.CONFIG_PRIMARY_KEYS],
            source["num_random_rows"],
            source["agg_value"],
            differences["difference"],
            differences["pct_difference"],
            differences["pct_threshold"],
            differences[consts.VALIDATION_STATUS],
        ]
    ]
    joined = source_difference.join(target, join_keys, how="outer")[
        source_difference["validation_name"],
        source_difference[consts.VALIDATION_TYPE]
        .fillna(target[consts.VALIDATION_TYPE])
        .name(consts.VALIDATION_TYPE),
        source_difference[consts.AGGREGATION_TYPE]
        .fillna(target[consts.AGGREGATION_TYPE])
        .name(consts.AGGREGATION_TYPE),
        source_difference["table_name"].name(consts.SOURCE_TABLE_NAME),
        source_difference["column_name"].name(consts.SOURCE_COLUMN_NAME),
        source_difference["agg_value"].name(consts.SOURCE_AGG_VALUE),
        target["table_name"].name(consts.TARGET_TABLE_NAME),
        target["column_name"].name(consts.TARGET_COLUMN_NAME),
        target["agg_value"].name(consts.TARGET_AGG_VALUE),
        group_by_columns,
        source_difference[consts.CONFIG_PRIMARY_KEYS],
        source_difference["num_random_rows"],
        source_difference["difference"],
        source_difference["pct_difference"],
        source_difference["pct_threshold"],
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


def _get_summary(
    run_metadata: "RunMetadata",
    result_df: "DataFrame",
    source_df: "DataFrame",
    target_df: "DataFrame",
):
    """Logs a summary report/stats of row validation results."""
    try:
        if result_df.empty:
            return

        if (result_df.loc[0, consts.VALIDATION_TYPE] == consts.ROW_VALIDATION) or (
            # Check for custom-query row validation which always should have primary keys (not null)
            result_df.loc[0, consts.VALIDATION_TYPE] == consts.CUSTOM_QUERY
            and result_df.loc[0, consts.CONFIG_PRIMARY_KEYS]
        ):
            # Vectorized calculations for all counts.
            success_condition = (
                result_df[consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_SUCCESS
            )
            fail_condition = ~success_condition  # Invert success for fail condition.

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
                        # Explicit conversion of numpy's int64 values to int for JSON serializability
                        consts.TOTAL_SOURCE_ROWS: int(source_df.shape[0]),
                        consts.TOTAL_TARGET_ROWS: int(target_df.shape[0]),
                        consts.TOTAL_ROWS_VALIDATED: int(result_df.shape[0]),
                        # Using .sum() on boolean Series for much faster counting
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
    except Exception as e:
        logging.warning(
            f"{COMBINER_GET_SUMMARY_EXC_TEXT}: {e}",
            exc_info=True,
        )
