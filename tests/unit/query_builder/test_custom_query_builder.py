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

import pytest

from data_validation import consts

INPUT_QUERY = "SELECT b.mascot, count(*) as count from dvt_testing.mascot b group by mascot order by 2 desc"
DATAFRAME_COLUMNS = ["mascot", "count"]
CALCULATED_COLUMNS = {
    "columns": ["mascot", "count"],
    "cast": ["cast__mascot", "cast__count"],
    "ifnull": ["ifnull__cast__mascot", "ifnull__cast__count"],
    "rstrip": ["rstrip__ifnull__cast__mascot", "rstrip__ifnull__cast__count"],
    "upper": [
        "upper__rstrip__ifnull__cast__mascot",
        "upper__rstrip__ifnull__cast__count",
    ],
}
AGGREGATES_TEST = [
    {
        consts.CONFIG_FIELD_ALIAS: "sum_starttime",
        consts.CONFIG_SOURCE_COLUMN: "starttime",
        consts.CONFIG_TARGET_COLUMN: "starttime",
        consts.CONFIG_TYPE: "sum",
    }
]
AGGREGATION_QUERY = "sum(starttime) as sum_starttime,"
BASE_QUERY = "SELECT * FROM bigquery-public-data.usa_names.usa_1910_2013"


@pytest.fixture
def module_under_test():
    import data_validation.query_builder.custom_query_builder

    return data_validation.query_builder.custom_query_builder


def test_import(module_under_test):
    assert module_under_test is not None


def test_get_calculated_columns(module_under_test):
    calculated_columns = module_under_test.CustomQueryBuilder().get_calculated_columns(
        DATAFRAME_COLUMNS
    )
    assert calculated_columns == CALCULATED_COLUMNS


def test_custom_query_get_aggregation_query(module_under_test):
    aggregation_query = module_under_test.CustomQueryBuilder().get_aggregation_query(
        AGGREGATES_TEST[0]["type"], AGGREGATES_TEST[0]["source_column"]
    )
    assert aggregation_query == "sum(starttime) as sum__starttime,"


def test_custom_query_get_wrapper_aggregation_query(module_under_test):
    wrapper_query = (
        module_under_test.CustomQueryBuilder().get_wrapper_aggregation_query(
            AGGREGATION_QUERY, BASE_QUERY
        )
    )
    assert (
        wrapper_query
        == "sum(starttime) as sum_starttime FROM (SELECT * FROM bigquery-public-data.usa_names.usa_1910_2013) as base_query"
    )
