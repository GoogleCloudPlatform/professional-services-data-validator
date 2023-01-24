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

import os
import shutil
import pytest
import json
import random
from datetime import datetime, timedelta

from data_validation import cli_tools
from data_validation import consts
from data_validation.config_manager import ConfigManager

SOURCE_TABLE_FILE_PATH = "source_table_data.json"
TARGET_TABLE_FILE_PATH = "target_table_data.json"

TEST_INPUT_FILE_PATH = os.path.join(
    "./", "tests", "unit", "test_inputs", "test_partition_builder.json"
)
PARTITION_FILTERS_LIST = "PARTITION_FILTERS_LIST"
YAML_CONFIGS_LIST = "YAML_CONFIGS_LIST"

STRING_CONSTANT = "constant"
RANDOM_STRINGS = ["a", "b", "c", "d"]

SOURCE_CONN_CONFIG = {
    "source_type": "FileSystem",
    "table_name": "my_table",
    "file_path": SOURCE_TABLE_FILE_PATH,
    "file_type": "json",
}

TARGET_CONN_CONFIG = {
    "source_type": "FileSystem",
    "table_name": "my_table",
    "file_path": TARGET_TABLE_FILE_PATH,
    "file_type": "json",
}

GENERATE_TABLE_PARTITIONS_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": None,
    "target_conn": None,
    # Validation Type
    consts.CONFIG_TYPE: "Row",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TABLE_NAME: "citibike_stations",
    consts.CONFIG_TARGET_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TARGET_TABLE_NAME: "citibike_stations",
    consts.CONFIG_CALCULATED_FIELDS: [],
    consts.CONFIG_FILTERS: [
        {
            consts.CONFIG_TYPE: consts.FILTER_TYPE_CUSTOM,
            consts.CONFIG_FILTER_SOURCE: "station_id>3000",
            consts.CONFIG_FILTER_TARGET: "station_id>3000",
        }
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_SOURCE_COLUMN: "station_id",
            consts.CONFIG_TARGET_COLUMN: "station_id",
            consts.CONFIG_FIELD_ALIAS: "station_id",
            consts.CONFIG_CAST: None,
        }
    ],
}

TEST_CONN = "{'source_type':'Example'}"
PARTITION_NUM = 20
PARTITIONS_DIR = "test_partitions_dir"
CLI_ARGS = [
    "generate-table-partitions",
    "-sc",
    TEST_CONN,
    "-tc",
    TEST_CONN,
    "-tbls",
    "bigquery-public-data.new_york_citibike.citibike_stations,bigquery-public-data.new_york_citibike.citibike_stations",
    "--primary-keys",
    "station_id",
    "--hash",
    "*",
    "--filter-status",
    "fail",
    "--filters",
    "station_id>3000",
    "-cdir",
    PARTITIONS_DIR,
    "--partition-num",
    f"{PARTITION_NUM}",
    "--partition-key",
    "bike_id",
    "--labels",
    "name=test_run",
]

CLI_ARGS_JSON_SOURCE = [
    "generate-table-partitions",
    "-sc",
    TEST_CONN,
    "-tc",
    TEST_CONN,
    "-tbls",
    "test_table1,test_table2",
    "--primary-keys",
    "id",
    "--hash",
    "*",
    "-cdir",
    PARTITIONS_DIR,
    "--partition-num",
    f"{PARTITION_NUM}",
    "--partition-key",
    "id",
]

# partition_key is not passed
CLI_ARGS_ABSENT = [
    "generate-table-partitions",
    "-sc",
    TEST_CONN,
    "-tc",
    TEST_CONN,
    "-tbls",
    "bigquery-public-data.new_york_citibike.citibike_stations,bigquery-public-data.new_york_citibike.citibike_stations",
    "--primary-keys",
    "station_id",
    "--hash",
    "*",
    "--filter-status",
    "fail",
    "-cdir",
    "partitions_dir",
    "--partition-num",
    f"{PARTITION_NUM}",
    "--labels",
    "name=test_run",
]


class MockIbisClient(object):
    _source_type = "BigQuery"


@pytest.fixture
def module_under_test():
    import data_validation.partition_builder

    return data_validation.partition_builder


@pytest.fixture
def test_inputs():
    fin = open(TEST_INPUT_FILE_PATH)
    return json.load(fin)


def teardown_module(module):
    # Clean up: Delete test partitions directory and its contents
    folder_path = os.path.join("./", PARTITIONS_DIR)
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)


def _generate_fake_data(
    rows=10, initial_id=0, second_range=60 * 60 * 24, int_range=100, random_strings=None
):
    """Return a list of dicts with given number of rows.

    Data Keys:
        id: a unique int per row
        timestamp_value: a random timestamp in the past {second_range} back
        date_value: a random date in the past {second_range} back
        int_value: a random int value inside 0 to {int_range}
        text_value: a random string from supplied list
    """
    data = []
    random_strings = random_strings or RANDOM_STRINGS
    for i in range(initial_id, initial_id + rows):
        rand_seconds = random.randint(0, second_range)
        rand_timestamp = datetime.now() - timedelta(seconds=rand_seconds)
        rand_date = rand_timestamp.date()

        row = {
            "id": i,
            "date_value": rand_date,
            "timestamp_value": rand_timestamp,
            "int_value": random.randint(0, int_range),
            "text_constant": STRING_CONSTANT,
            "text_numeric": "2",
            "text_value": random.choice(random_strings),
            "text_value_two": random.choice(random_strings),
        }
        data.append(row)

    return data


def _generate_config_manager(table_name: str = "my_table") -> ConfigManager:
    """Returns a Dummy ConfigManager Object"""

    row_config = {
        # BigQuery Specific Connection Config
        "source_conn": SOURCE_CONN_CONFIG,
        "target_conn": TARGET_CONN_CONFIG,
        # Validation Type
        consts.CONFIG_TYPE: consts.ROW_VALIDATION,
        # Configuration Required Depending on Validator Type
        "schema_name": None,
        "table_name": table_name,
        "target_schema_name": None,
        "target_table_name": table_name,
        consts.CONFIG_PRIMARY_KEYS: [
            {
                consts.CONFIG_FIELD_ALIAS: "id",
                consts.CONFIG_SOURCE_COLUMN: "id",
                consts.CONFIG_TARGET_COLUMN: "id",
                consts.CONFIG_CAST: None,
            },
        ],
        consts.CONFIG_COMPARISON_FIELDS: [
            {
                consts.CONFIG_FIELD_ALIAS: "int_value",
                consts.CONFIG_SOURCE_COLUMN: "int_value",
                consts.CONFIG_TARGET_COLUMN: "int_value",
                consts.CONFIG_CAST: None,
            },
            {
                consts.CONFIG_FIELD_ALIAS: "text_value",
                consts.CONFIG_SOURCE_COLUMN: "text_value",
                consts.CONFIG_TARGET_COLUMN: "text_value",
                consts.CONFIG_CAST: None,
            },
        ],
        consts.CONFIG_RESULT_HANDLER: None,
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
        consts.CONFIG_FILTERS: [],
    }
    return ConfigManager(row_config)


def _get_fake_json_data(data):
    for row in data:
        row["date_value"] = str(row["date_value"])
        row["timestamp_value"] = str(row["timestamp_value"])
        row["text_constant"] = row["text_constant"]
        row["text_numeric"] = row["text_numeric"]
        row["text_value"] = row["text_value"]
        row["text_value_two"] = row["text_value_two"]

    return json.dumps(data)


def _create_table_file(table_path, data):
    """Create JSON File"""
    with open(table_path, "w") as f:
        f.write(data)


def test_import(module_under_test):
    assert module_under_test is not None


def test_class_object_creation(module_under_test):
    """Create a PartitionBuilder object passing 2 tables and assert the following
    1. Table/Configs count
    2. config_dir value present and absent
    3. primary_key value
    4. partition_key value present and absent
    """
    mock_config_manager1 = ConfigManager(
        GENERATE_TABLE_PARTITIONS_CONFIG,
        MockIbisClient(),
        MockIbisClient(),
        verbose=False,
    )
    mock_config_manager2 = ConfigManager(
        GENERATE_TABLE_PARTITIONS_CONFIG,
        MockIbisClient(),
        MockIbisClient(),
        verbose=False,
    )
    config_managers = [mock_config_manager1, mock_config_manager2]

    parser = cli_tools.configure_arg_parser()

    # partition_key is present and different from primary_key
    # config_dir is passed
    args = parser.parse_args(CLI_ARGS)
    builder = module_under_test.PartitionBuilder(config_managers, args)
    assert builder.table_count == len(config_managers)
    assert builder.partition_key == "bike_id"
    assert builder.primary_key == "station_id"

    # partition_key is absent, expected to default to primary_key
    args = parser.parse_args(CLI_ARGS_ABSENT)
    builder = module_under_test.PartitionBuilder(config_managers, args)
    assert builder.table_count == len(config_managers)
    assert builder.partition_key == builder.primary_key


def test_get_partition_key_filters(module_under_test, test_inputs):
    """Build partitions filters and assert:
    1. Table count
    2. Filters count
    3. Partition filters
    """
    data = _generate_fake_data(rows=1001, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    config_manager = _generate_config_manager("my_table")
    config_managers = [config_manager]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_ARGS_JSON_SOURCE)

    expected_partition_filters_list = test_inputs[PARTITION_FILTERS_LIST]

    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    partition_filters_list = builder._get_partition_key_filters()
    assert len(partition_filters_list) == len(config_managers)
    assert len(partition_filters_list[0]) == mock_args.partition_num
    assert partition_filters_list[0] == expected_partition_filters_list


def test_add_partition_filters_to_config(module_under_test, test_inputs):
    """Add partition filters to ConfigManager object, build YAML config list
    and assert YAML configs
    """
    # Generate dummy YAML configs list
    config_manager1 = _generate_config_manager("test_table1")
    config_manager2 = _generate_config_manager("test_table2")
    config_managers = [config_manager1, config_manager2]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_ARGS_JSON_SOURCE)

    expected_yaml_configs_list = test_inputs[YAML_CONFIGS_LIST]

    partition_filters1 = test_inputs[PARTITION_FILTERS_LIST]
    partition_filters2 = test_inputs[PARTITION_FILTERS_LIST]
    master_filter_list = [partition_filters1, partition_filters2]

    # Create PartitionBuilder object and get YAML configs list
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    yaml_configs_list = builder._add_partition_filters(master_filter_list)
    assert yaml_configs_list == expected_yaml_configs_list


def test_store_yaml_partitions_local(module_under_test, test_inputs):
    """Store all the Partition YAMLs for 2 tables to specified local directory"""

    # Generate dummy YAML configs list
    config_manager1 = _generate_config_manager("test_table1")
    config_manager2 = _generate_config_manager("test_table2")
    config_managers = [config_manager1, config_manager2]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_ARGS_JSON_SOURCE)

    # Dummy YAML configs list
    yaml_configs_list = test_inputs[YAML_CONFIGS_LIST]

    # Create test partitions directory to store results
    folder_path = os.path.join("./", PARTITIONS_DIR)
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

    # Store YAML partition configs to local directory
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    builder._store_partitions(yaml_configs_list)

    # Assert file count for 2 tables and sample file names
    partition_dir1_contents = os.listdir(os.path.join(PARTITIONS_DIR, "test_table1"))
    partition_dir2_contents = os.listdir(os.path.join(PARTITIONS_DIR, "test_table2"))

    assert len(partition_dir1_contents) == PARTITION_NUM
    assert len(partition_dir2_contents) == PARTITION_NUM
    assert "0000.yaml" in partition_dir1_contents
    assert "0019.yaml" in partition_dir2_contents
