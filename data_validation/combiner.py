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

import ibis


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

    all_fields = frozenset(source_names)
    validation_fields = all_fields - frozenset(join_on_fields)
    source_pivots = []
    target_pivots = []

    for field in validation_fields:
        validation_metadata = run_metadata.validations[field]
        source_pivots.append(
            source.projection(
                (
                    ibis.literal(field).name("validation_name"),
                    ibis.literal(validation_metadata.validation_type).name(
                        "validation_type"
                    ),
                    ibis.literal(validation_metadata.aggregation_type).name(
                        "aggregation_type"
                    ),
                    ibis.literal(validation_metadata.source_table_name).name(
                        "source_table_name"
                    ),
                    # Cast to string to ensure types match, even when column name is NULL.
                    ibis.literal(validation_metadata.source_column_name)
                    .cast("string")
                    .name("source_column_name"),
                    source[field].cast("string").name("source_agg_value"),
                )
                + join_on_fields
            )
        )
        target_pivots.append(
            target.projection(
                (
                    ibis.literal(field).name("validation_name"),
                    ibis.literal(validation_metadata.target_table_name).name(
                        "target_table_name"
                    ),
                    # Cast to string to ensure types match, even when column name is NULL.
                    ibis.literal(validation_metadata.target_column_name)
                    .cast("string")
                    .name("target_column_name"),
                    target[field].cast("string").name("target_agg_value"),
                )
                + join_on_fields
            )
        )

    source_pivot = functools.reduce(
        lambda pivot1, pivot2: pivot1.union(pivot2), source_pivots
    )
    target_pivot = functools.reduce(
        lambda pivot1, pivot2: pivot1.union(pivot2), target_pivots
    )
    joined = _join_pivots(source_pivot, target_pivot, join_on_fields)
    joined = _add_metadata(joined, run_metadata)
    return client.execute(joined)


def _add_metadata(joined, run_metadata):
    joined = joined[joined, ibis.literal(run_metadata.start_time).name("start_time")]
    joined = joined[joined, ibis.literal(run_metadata.end_time).name("end_time")]
    return joined


def _join_pivots(source, target, join_on_fields):
    joined = source.join(target, ("validation_name",) + join_on_fields, how="outer")[
        [
            source["validation_name"],
            source["validation_type"],
            source["aggregation_type"],
            source["source_table_name"],
            source["source_column_name"],
            source["source_agg_value"],
            target["target_table_name"],
            target["target_column_name"],
            target["target_agg_value"],
        ]
        + [source[key] for key in join_on_fields]
    ]

    # TODO(GH#14): remove group-by key columns and write into an array of
    #              key-value structs
    joined = joined[joined, ibis.literal([]).name("source_group_by_columns")]
    joined = joined[joined, ibis.literal([]).name("target_group_by_columns")]

    return joined
