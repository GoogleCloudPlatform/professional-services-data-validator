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


SAMPLE_CONFIG = {
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

AGGREGATE_CONFIG_A = {
    consts.CONFIG_SOURCE_COLUMN: "a",
    consts.CONFIG_TARGET_COLUMN: "a",
    consts.CONFIG_FIELD_ALIAS: f"sum__a",
    consts.CONFIG_TYPE: "sum",
}

GROUPED_COLUMN_CONFIG_A = {
    consts.CONFIG_SOURCE_COLUMN: "a",
    consts.CONFIG_TARGET_COLUMN: "a",
    consts.CONFIG_FIELD_ALIAS: "a",
    consts.CONFIG_CAST: None,
}


class MockIbisTable(object):
    columns = ["a", "b", "c"]

    def __getitem__(self, key):
        return self

    def type(self):
        return "int64"

@pytest.fixture
def module_under_test():
    from data_validation import config_manager

    return config_manager


def test_import(module_under_test):
    """Test import cleanly """
    assert True


def test_get_table_info(module_under_test):
    """Test basic handler executes """
    config_manager = module_under_test.ConfigManager(SAMPLE_CONFIG, None, None, verbose=False)
    
    source_table_spec = "{}.{}".format(config_manager.get_source_schema(), config_manager.get_source_table())
    target_table_spec = "{}.{}".format(config_manager.get_target_schema(), config_manager.get_target_table())
    expected_table_spec = "{}.{}".format(SAMPLE_CONFIG[consts.CONFIG_SCHEMA_NAME], SAMPLE_CONFIG[consts.CONFIG_TABLE_NAME])
    
    assert source_table_spec == expected_table_spec
    assert target_table_spec == expected_table_spec


def test_build_config_grouped_columns(module_under_test):
    config_manager = module_under_test.ConfigManager(SAMPLE_CONFIG, None, None, verbose=False)
    config_manager._source_table = MockIbisTable()
    config_manager._target_table = MockIbisTable()
    
    column_configs = config_manager.build_config_grouped_columns(["a"])
    assert column_configs[0] == GROUPED_COLUMN_CONFIG_A


def test_build_config_aggregates(module_under_test):
    config_manager = module_under_test.ConfigManager(SAMPLE_CONFIG, None, None, verbose=False)
    config_manager._source_table = MockIbisTable()
    config_manager._target_table = MockIbisTable()
    
    aggregate_configs = config_manager.build_config_aggregates("sum", ["a"], [])
    assert aggregate_configs[0] == AGGREGATE_CONFIG_A


def test_build_config_aggregates_no_match(module_under_test):
    config_manager = module_under_test.ConfigManager(SAMPLE_CONFIG, None, None, verbose=False)
    config_manager._source_table = MockIbisTable()
    config_manager._target_table = MockIbisTable()
    
    aggregate_configs = config_manager.build_config_aggregates("sum", ["a"], ["float64"])
    assert not aggregate_configs
