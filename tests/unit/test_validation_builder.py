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

from copy import deepcopy
from data_validation import consts


COLUMN_VALIDATION_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": None,
    "target_conn": None,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TABLE_NAME: "citibike_trips",
    consts.CONFIG_GROUPED_COLUMNS: [],
}

QUERY_LIMIT = 100
COLUMN_VALIDATION_CONFIG_LIMIT = deepcopy(COLUMN_VALIDATION_CONFIG)
COLUMN_VALIDATION_CONFIG_LIMIT[consts.CONFIG_LIMIT] = QUERY_LIMIT

QUERY_GROUPS_TEST = [
    {
        consts.CONFIG_FIELD_ALIAS: "start_alias",
        consts.CONFIG_SOURCE_COLUMN: "starttime",
        consts.CONFIG_TARGET_COLUMN: "starttime",
        consts.CONFIG_CAST: "date",
    }
]


@pytest.fixture
def module_under_test():
    import data_validation.validation_builder

    return data_validation.validation_builder


def test_import(module_under_test):
    assert True


def test_column_validation(module_under_test):
    mock_client = None
    builder = module_under_test.ValidationBuilder(
        COLUMN_VALIDATION_CONFIG, mock_client, mock_client, verbose=False
    )

    assert builder.verbose == False
    assert builder._get_query_limit() is None


def test_validation_add_groups(module_under_test):
    mock_client = None
    builder = module_under_test.ValidationBuilder(
        COLUMN_VALIDATION_CONFIG, mock_client, mock_client, verbose=False
    )

    builder.add_query_groups(QUERY_GROUPS_TEST)

    assert builder.group_aliases == ["start_alias"]


def test_column_validation_limit(module_under_test):
    mock_client = None
    builder = module_under_test.ValidationBuilder(
        COLUMN_VALIDATION_CONFIG_LIMIT, mock_client, mock_client, verbose=False
    )
    builder.add_query_limit()

    assert builder.source_builder.limit == QUERY_LIMIT
