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
    client, source_table=DEFAULT_SOURCE, target_table=DEFAULT_TARGET, join_on_fields=()
):
    """Combine results into a report.

    Args:
        client (ibis.client.Client): Ibis client used to combine results.
        source_table (str): Name of source results table in client system.
        target_table (str): Name of target results table in client system.
        join_on_fields (Sequence[str]):
            A collection of column names to use to join source and target.
            These are the columns that both the source and target queries
            grouped by.

    Returns:
        TODO
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

    all_fields = frozenset(target_names)
    validation_fields = all_fields - frozenset(join_on_fields)
    source_pivots = []
    target_pivots = []

    for field in validation_fields:
        source_pivots.append(
            source.projection(
                (
                    # TODO(GH#2): send config so that original source &
                    #             target name can be added as literals
                    ibis.literal(field).name("validation_name"),
                    source[field].cast("string").name("source_agg_value"),
                )
                + join_on_fields
            )
        )
        target_pivots.append(
            target.projection(
                (
                    # TODO(GH#2): send config so that original source &
                    #             target name can be added as literals
                    ibis.literal(field).name("validation_name"),
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

    # TODO(GH#2): generate run ID
    # TODO(GH#2): add validation timing values
    return client.execute(joined)


def _join_pivots(source, target, join_on_fields):
    joined = source.join(target, ("validation_name",) + join_on_fields, how="outer")[
        [
            source["validation_name"],
            source["source_agg_value"],
            target["target_agg_value"],
        ]
        + [source[key] for key in join_on_fields]
    ]
    # TODO(GH#14): remove group-by key columns and write into an array of
    #              key-value structs
    return joined
