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

INPUT_QUERY = "SELECT b.mascot, count(*) as count from dvt_testing.mascot b group by mascot order by 2 desc"
DATAFRAME_COLUMNS = ["mascot", "count"]
CALCULATED_COLUMNS = {
    'columns': ['mascot', 'count'],
    'cast': ['cast__mascot', 'cast__count'],
    'ifnull': ['ifnull__cast__mascot', 'ifnull__cast__count'],
    'rstrip': ['rstrip__ifnull__cast__mascot', 'rstrip__ifnull__cast__count'],
    'upper': ['upper__rstrip__ifnull__cast__mascot', 'upper__rstrip__ifnull__cast__count']
}


@pytest.fixture
def module_under_test():
    import data_validation.query_builder.custom_query_row_builder
    return data_validation.query_builder.custom_query_row_builder


def test_import(module_under_test):
    assert module_under_test is not None


def test_get_calculated_columns(
    module_under_test
):
    calculated_columns = module_under_test.CustomQueryRowBuilder().get_calculated_columns(DATAFRAME_COLUMNS)
    assert calculated_columns == CALCULATED_COLUMNS
