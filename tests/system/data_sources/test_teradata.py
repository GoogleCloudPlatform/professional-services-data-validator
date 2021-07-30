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

from datetime import datetime

from data_validation import data_validation
from data_validation.query_builder import query_builder


TERADATA_CONFIG = {
    # Configuration Required for All Data Soures
    "source_type": "Teradata",
    # BigQuery Specific Connection Config
    "config": {
        "host": "127.0.0.1",
        "user_name": "dbc",
        "password": "dbc",
        "port": 10255,
    },
    # Configuration Required Depending on Validator Type
    "schema_name": "Sys_Calendar",
    "table_name": "CALENDAR",
    "partition_column": "year_of_calendar",
}


def create_validator(builder):
    return data_validation.DataValidation(
        TERADATA_CONFIG, builder=builder, result_handler=None, format="json", verbose=False
    )


def test_count_validator():
    builder = query_builder.QueryBuilder.build_count_validator()
    validator = create_validator(builder)
    df = validator.execute()
    assert df["count_inp"][0] > 0
    assert df["count_inp"][0] == df["count_out"][0]


def test_partitioned_count_validator():
    builder = query_builder.QueryBuilder.build_partition_count_validator(
        days_past=700, limit=100
    )
    # Add Filters for large table
    _add_calendar_date_filters(builder)

    validator = create_validator(builder)
    df = validator.execute()
    rows = list(df.iterrows())

    # Check that all partitions are unique.
    partitions = frozenset(df["partition_key"])
    assert len(rows) == len(partitions)

    for _, row in rows:
        assert row["count_inp"] > 0
        assert row["count_inp"] == row["count_out"]


def _add_calendar_date_filters(builder):
    # Adding custom filter for better results
    project_start_date = datetime(2020, 1, 1, 0, 0, 0)
    filter_obj = query_builder.FilterField.less_than(
        "calendar_date", project_start_date
    )
    builder.add_filter_field(filter_obj)

    in_the_past = datetime(1991, 5, 2, 0, 0, 0)
    filter_obj = query_builder.FilterField.greater_than("calendar_date", in_the_past)
    builder.add_filter_field(filter_obj)
