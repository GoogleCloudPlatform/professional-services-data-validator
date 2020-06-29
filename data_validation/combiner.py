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

import functools
import json

import ibis

from data_validation import consts


DEFAULT_SOURCE = "source"
DEFAULT_TARGET = "target"


def generate_report(
    client,
    run_metadata,
    source_table=DEFAULT_SOURCE,
    target_table=DEFAULT_TARGET,
    join_on_fields=(),
):
    """Combine results into a report.

    Args:
        client (ibis.client.Client): Ibis client used to combine results.
        run_metadata (data_validation.metadata.RunMetadata):
            Metadata about the run and validations.
        source_table (str): Name of source results table in client system.
        target_table (str): Name of target results table in client system.
        join_on_fields (Sequence[str]):
            A collection of column names to use to join source and target.
            These are the columns that both the source and target queries
            grouped by.

    Returns:
        pandas.DataFrame:
            A pandas DataFrame with the results of the validation in the same
            schema as the report table.
    """
    join_on_fields = tuple(join_on_fields)
    source = client.table(source_table)
    target = client.table(target_table)
    source_names = source.schema().names
    target_names = target.schema().names

    if source_names != target_names:
        raise ValueError(
            "Expected source and target to have same schema, got "
            f"source: {source_names} target: {target_names}"
        )

    source_count = source.count().execute()
    target_count = target.count().execute()
    if not join_on_fields and (source_count > 1 or target_count > 1):
        raise ValueError(
            "Expected 1 row per result table when receiving no join_on_fields. "
            f"Got source: {source_count} rows, target: {target_count} rows"
        )

    differences_pivot = _calculate_differences(source, target, join_on_fields)
    source_pivot = _pivot_result(
        source, join_on_fields, run_metadata.validations, consts.RESULT_TYPE_SOURCE
    )
    target_pivot = _pivot_result(
        target, join_on_fields, run_metadata.validations, consts.RESULT_TYPE_TARGET
    )
    joined = _join_pivots(source_pivot, target_pivot, differences_pivot, join_on_fields)
    documented = _add_metadata(joined, run_metadata)
    return client.execute(documented)


def _calculate_differences(source, target, join_on_fields):
    """Calculate differences between source and target fields.

    This function is separate from the "pivot" logic because we want to
    calculate the differences with the original data type before casting to a
    floating point value. The pivot casts all values to string, so the
    difference calculation would fail if done after that step.
    """
    all_fields = frozenset(source.schema().names)
    validation_fields = all_fields - frozenset(join_on_fields)

    if join_on_fields:
        # Use an inner join because a row must be present in source and target
        # for the difference to be well defined.
        differences_joined = source.join(target, join_on_fields, how="inner")
    else:
        # When no join_on_fields are present, we expect only one row per table.
        # This is validated in generate_report before this function is called.
        differences_joined = source.cross_join(target)

    differences_pivots = []

    for field in validation_fields:
        field_differences = differences_joined.projection(
            [
                source[field].name("differences_source_agg_value"),
                target[field].name("differences_target_agg_value"),
            ]
            + [source[join_field] for join_field in join_on_fields]
        )
        differences_pivots.append(
            field_differences[
                (
                    ibis.literal(field).name("validation_name"),
                    (
                        field_differences["differences_target_agg_value"]
                        - field_differences["differences_source_agg_value"]
                    )
                    .cast("double")
                    .name("difference"),
                    (
                        ibis.literal(100.0)
                        * (
                            field_differences["differences_target_agg_value"]
                            - field_differences["differences_source_agg_value"]
                        ).cast("double")
                        / field_differences["differences_source_agg_value"].cast(
                            "double"
                        )
                    )
                    .cast("double")
                    .name("pct_difference"),
                )
                + join_on_fields
            ]
        )

    differences_pivot = functools.reduce(
        lambda pivot1, pivot2: pivot1.union(pivot2), differences_pivots
    )
    return differences_pivot


def _pivot_result(result, join_on_fields, validations, result_type):
    all_fields = frozenset(result.schema().names)
    validation_fields = all_fields - frozenset(join_on_fields)
    pivots = []

    for field in validation_fields:
        validation = validations[field]
        pivots.append(
            result.projection(
                (
                    ibis.literal(field).name("validation_name"),
                    ibis.literal(validation.validation_type).name("validation_type"),
                    ibis.literal(validation.aggregation_type).name("aggregation_type"),
                    ibis.literal(validation.get_table_name(result_type)).name(
                        "table_name"
                    ),
                    # Cast to string to ensure types match, even when column
                    # name is NULL (such as for count aggregations).
                    ibis.literal(validation.get_column_name(result_type))
                    .cast("string")
                    .name("column_name"),
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
    return expr.cast("string").re_replace(r"\\", r"\\\\").re_replace('"', '\\"')


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
            source["agg_value"],
            differences["difference"],
            differences["pct_difference"],
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
        source_difference["difference"],
        source_difference["pct_difference"],
    ]
    return joined


def _add_metadata(joined, run_metadata):
    joined = joined[joined, ibis.literal(run_metadata.start_time).name("start_time")]
    joined = joined[joined, ibis.literal(run_metadata.end_time).name("end_time")]
    return joined
