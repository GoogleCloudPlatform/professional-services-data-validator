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
import os

from data_validation import data_validation, consts
from data_validation.query_builder import query_builder

TERADATA_PASSWORD = os.getenv("TERADATA_PASSWORD")
TERADATA_HOST = os.getenv("TERADATA_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")

conn = {
    "source_type": "Teradata",
    "host": TERADATA_HOST,
    "user_name": "udf",
    "password": TERADATA_PASSWORD,
    "port": 1025,
}


TERADATA_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: conn,
    consts.CONFIG_TARGET_CONN: conn,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TABLE_NAME: "CALENDAR",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "year_of_calendar",
            consts.CONFIG_TARGET_COLUMN: "year_of_calendar",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTERS: [
        {
            consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,
            consts.CONFIG_FILTER_SOURCE_COLUMN = "year_of_calendar",
            consts.CONFIG_FILTER_SOURCE_VALUE = 2010,
            consts.CONFIG_FILTER_TARGET_COLUMN = "year_of_calendar",
            consts.CONFIG_FILTER_TARGET_VALUE = 2010,
        },
    ],
}


def test_count_validator():
    validator = data_validation.DataValidation(TERADATA_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


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
