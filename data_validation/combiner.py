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

import ibis
import ibis.expr.datatypes

from data_validation import consts

DEFAULT_SOURCE = "source"
DEFAULT_TARGET = "target"


def generate_report(
    client,
    run_metadata,
    source,
    target,
    join_on_fields=(),
    is_value_comparison=False,
    verbose=False,
):
    """Combine results into a report.

    Args:
        client (ibis.client.Client): Ibis client used to combine results.
        run_metadata (data_validation.metadata.RunMetadata):
            Metadata about the run and validations.
        source (ibis.QUERY): Ibis query / table object.
        target (ibis.QUERY): Ibis query / table object.
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
    join_on_fields = tuple(join_on_fields)

    source_names = source.schema().names
    target_names = target.schema().names

    if source_names != target_names:
        raise ValueError(
            "Expected source and target to have same schema, got "
            f"source: {source_names} target: {target_names}"
        )

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
    documented = _add_metadata(joined, run_metadata)

    if verbose:
        print("-- ** Combiner Query ** --")
        print(documented.compile())

    result_df = client.execute(documented)
    result_df.validation_status.fillna(consts.VALIDATION_STATUS_FAIL, inplace=True)
    return result_df


def _calculate_difference(field_differences, datatype, validation, is_value_comparison):
    pct_threshold = ibis.literal(validation.threshold)

    if isinstance(datatype, ibis.expr.datatypes.Timestamp):
        source_value = field_differences["differences_source_value"].epoch_seconds()
        target_value = field_differences["differences_target_value"].epoch_seconds()
    elif isinstance(datatype, ibis.expr.datatypes.Float64):
        # Float64 type results from AVG() aggregation
        source_value = field_differences["differences_source_value"].round(digits=4)
        target_value = field_differences["differences_target_value"].round(digits=4)
    else:
        source_value = field_differences["differences_source_value"]
        target_value = field_differences["differences_target_value"]

    # Does not calculate difference between agg values for row hash due to int64 overflow
    if is_value_comparison:
        difference = pct_difference = ibis.null()
        validation_status = (
            ibis.case()
            .when(
                target_value.isnull() & source_value.isnull(),
                consts.VALIDATION_STATUS_SUCCESS,
            )
            .when(target_value == source_value, consts.VALIDATION_STATUS_SUCCESS)
            .else_(consts.VALIDATION_STATUS_FAIL)
            .end()
        )
    # String data types i.e "None" can be returned for NULL timestamp/datetime aggs
    elif isinstance(datatype, ibis.expr.datatypes.String):
        difference = pct_difference = ibis.null().cast("float64")
        validation_status = (
            ibis.case()
            .when(
                target_value.isnull() & source_value.isnull(),
                consts.VALIDATION_STATUS_SUCCESS,
            )
            .else_(consts.VALIDATION_STATUS_FAIL)
            .end()
        )
    else:
        difference = (target_value - source_value).cast("float64")

        pct_difference_nonzero = (
            ibis.literal(100.0)
            * difference
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
        validation_status.name("validation_status"),
    )


def _calculate_differences(
    source, target, join_on_fields, validations, is_value_comparison
):
    """Calculate differences between source and target fields.

    This function is separate from the "pivot" logic because we want to
    calculate the differences with the original data type before casting to a
    floating point value. The pivot casts all values to string, so the
    difference calculation would fail if done after that step.
    """
    schema = source.schema()

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
        else:
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
                        field_differences, field_type, validation, is_value_comparison
                    )
                ]
            )
    differences_pivot = functools.reduce(
        lambda pivot1, pivot2: pivot1.union(pivot2), differences_pivots
    )
    return differences_pivot


def _pivot_result(result, join_on_fields, validations, result_type):
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
                ).name("primary_keys")
            else:
                primary_keys = ibis.literal(None).cast("string").name("primary_keys")

            pivots.append(
                result.projection(
                    (
                        ibis.literal(field).name("validation_name"),
                        ibis.literal(validation.validation_type).name(
                            "validation_type"
                        ),
                        ibis.literal(validation.aggregation_type).name(
                            "aggregation_type"
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


def _join_pivots(source, target, differences, join_on_fields):
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
        ).name("group_by_columns")
    else:
        group_by_columns = ibis.literal(None).cast("string").name("group_by_columns")

    join_keys = ("validation_name",) + join_on_fields
    source_difference = source.join(differences, join_keys, how="outer")[
        [source[field] for field in join_keys]
        + [
            source["validation_type"],
            source["aggregation_type"],
            source["table_name"],
            source["column_name"],
            source["primary_keys"],
            source["num_random_rows"],
            source["agg_value"],
            differences["difference"],
            differences["pct_difference"],
            differences["pct_threshold"],
            differences["validation_status"],
        ]
    ]
    joined = source_difference.join(target, join_keys, how="outer")[
        source_difference["validation_name"],
        source_difference["validation_type"]
        .fillna(target["validation_type"])
        .name("validation_type"),
        source_difference["aggregation_type"]
        .fillna(target["aggregation_type"])
        .name("aggregation_type"),
        source_difference["table_name"].name("source_table_name"),
        source_difference["column_name"].name("source_column_name"),
        source_difference["agg_value"].name("source_agg_value"),
        target["table_name"].name("target_table_name"),
        target["column_name"].name("target_column_name"),
        target["agg_value"].name("target_agg_value"),
        group_by_columns,
        source_difference["primary_keys"],
        source_difference["num_random_rows"],
        source_difference["difference"],
        source_difference["pct_difference"],
        source_difference["pct_threshold"],
        source_difference["validation_status"],
    ]
    return joined


def _add_metadata(joined, run_metadata):
    # TODO: Add source and target queries to metadata
    run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)

    joined = joined[
        joined,
        ibis.literal(run_metadata.run_id).name("run_id"),
        ibis.literal(
            run_metadata.labels, type="array<struct<key:string,value:string>>"
        ).name("labels"),
        ibis.literal(run_metadata.start_time).name("start_time"),
        ibis.literal(run_metadata.end_time).name("end_time"),
    ]
    return joined
