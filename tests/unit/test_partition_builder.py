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
import math
from datetime import datetime, timedelta

from data_validation import cli_tools
from data_validation import consts
from data_validation.config_manager import ConfigManager

SOURCE_TABLE_FILE_PATH = "source_table_data.json"
TARGET_TABLE_FILE_PATH = "target_table_data.json"

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

TEST_CONN = "{'source_type':'Example'}"
PARTITION_NUM = 9
PARTS_PER_FILE = 5
PARTITIONS_DIR = "test_partitions_dir"
SOURCE_QUERY = (
    "select * from `bigquery-public-data.new_york_citibike.citibike_stations`"
)
TARGET_QUERY_FILE = "target_query.sql"

TABLE_PART_ARGS = [
    "generate-table-partitions",
    "-sc",
    TEST_CONN,
    "-tc",
    TEST_CONN,
    "-tbls",
    "test_table",
    "--primary-keys",
    "id",
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
    "--parts-per-file",
    f"{PARTS_PER_FILE}",
]

# partition_key is not passed
QUERY_PART_ARGS = [
    "generate-table-partitions",
    "-sc",
    TEST_CONN,
    "-tc",
    TEST_CONN,
    "-sq",
    SOURCE_QUERY,
    "-tqf",
    TARGET_QUERY_FILE,
    "--primary-keys",
    "region_id,station_id",
    "--hash",
    "*",
    "--filter-status",
    "fail",
    "-cdir",
    PARTITIONS_DIR,
    "--partition-num",
    f"{PARTITION_NUM}",
    "--parts-per-file",
    f"{PARTS_PER_FILE}",
    "--labels",
    "name=test_run",
]

PARTITION_FILTERS_LIST = [
    "id >= 0 and id < 99",
    "id >= 100 and id < 199",
    "id >= 200 and id < 299",
    "id >= 300 and id < 399",
    "id >= 400 and id < 499",
    "id >= 500 and id < 599",
    "id >= 600 and id < 699",
    "id >= 700 and id < 799",
    "id >= 800 and id < 899",
]

YAML_CONFIGS_LIST = [
    {
        "target_folder_name": "test_table",
        "yaml_files": [
            {
                "target_file_name": "0000.yaml",
                "yaml_config": {
                    "source": "{'source_type':'Example'}",
                    "target": "{'source_type':'Example'}",
                    "result_handler": {},
                    "validations": [
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 0 and id < 99",
                                    "target": "id >= 0 and id < 99",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 100 and id < 199",
                                    "target": "id >= 100 and id < 199",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 200 and id < 299",
                                    "target": "id >= 200 and id < 299",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 300 and id < 399",
                                    "target": "id >= 300 and id < 399",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 400 and id < 499",
                                    "target": "id >= 400 and id < 499",
                                }
                            ],
                        },
                    ],
                },
            },
            {
                "target_file_name": "0001.yaml",
                "yaml_config": {
                    "source": "{'source_type':'Example'}",
                    "target": "{'source_type':'Example'}",
                    "result_handler": {},
                    "validations": [
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 500 and id < 599",
                                    "target": "id >= 500 and id < 599",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 600 and id < 699",
                                    "target": "id >= 600 and id < 699",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 700 and id < 799",
                                    "target": "id >= 700 and id < 799",
                                }
                            ],
                        },
                        {
                            "type": "Row",
                            "schema_name": None,
                            "table_name": "test_table",
                            "target_schema_name": None,
                            "target_table_name": "test_table",
                            "primary_keys": [
                                {
                                    "field_alias": "id",
                                    "source_column": "id",
                                    "target_column": "id",
                                    "cast": None,
                                }
                            ],
                            "comparison_fields": [
                                {
                                    "field_alias": "int_value",
                                    "source_column": "int_value",
                                    "target_column": "int_value",
                                    "cast": None,
                                },
                                {
                                    "field_alias": "text_value",
                                    "source_column": "text_value",
                                    "target_column": "text_value",
                                    "cast": None,
                                },
                            ],
                            "format": "table",
                            "filter_status": None,
                            "filters": [
                                {
                                    "type": "custom",
                                    "source": "id >= 800 and id < 899",
                                    "target": "id >= 800 and id < 899",
                                }
                            ],
                        },
                    ],
                },
            },
        ],
    }
]


@pytest.fixture
def module_under_test():
    import data_validation.partition_builder

    return data_validation.partition_builder


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
    2. config_dir value present and correct
    3. single primary_key value (is this already tested in row validation?)
    4. multiple primary keys (is this already tested in row validation?)
    """
    mock_config_manager = _generate_config_manager("test_table")
    config_managers = [mock_config_manager]

    parser = cli_tools.configure_arg_parser()

    # config_dir is passed
    args = parser.parse_args(TABLE_PART_ARGS)
    builder = module_under_test.PartitionBuilder(config_managers, args)
    assert builder._get_arg_config_dir() == PARTITIONS_DIR
    assert builder.args.partition_num == PARTITION_NUM
    assert builder.args.parts_per_file == PARTS_PER_FILE

    args = parser.parse_args(QUERY_PART_ARGS)
    builder = module_under_test.PartitionBuilder(config_managers, args)
    assert builder.args.primary_keys == "region_id,station_id"
    assert builder.args.source_query == SOURCE_QUERY
    assert builder.args.target_query_file == TARGET_QUERY_FILE


def test_add_partition_filters_to_config(module_under_test):
    """Add partition filters to ConfigManager object, build YAML config list
    and assert YAML configs
    """
    # Generate dummy YAML configs list
    config_manager = _generate_config_manager("test_table")
    config_managers = [config_manager]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(TABLE_PART_ARGS)

    expected_yaml_configs_list = YAML_CONFIGS_LIST

    # two partition filters are needed, one for source and one for target
    partition_filters = PARTITION_FILTERS_LIST
    master_filter_list = [[partition_filters, partition_filters]]

    # Create PartitionBuilder object and get YAML configs list
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    yaml_configs_list = builder._add_partition_filters(master_filter_list)

    assert len(yaml_configs_list[0]["yaml_files"]) == 2
    # 5 validations in the first file
    assert len(yaml_configs_list[0]["yaml_files"][0]["yaml_config"]["validations"]) == 5
    # 4 validations in the second file
    assert len(yaml_configs_list[0]["yaml_files"][1]["yaml_config"]["validations"]) == 4
    assert yaml_configs_list == expected_yaml_configs_list


def test_store_yaml_partitions(module_under_test, tmp_path):
    """Store all the Partition YAMLs for a table to specified local directory"""

    # Create test partitions directory to store results
    folder_path = tmp_path / PARTITIONS_DIR
    folder_path.mkdir()
    TABLE_PART_ARGS[TABLE_PART_ARGS.index("-cdir") + 1] = str(folder_path)

    # Generate dummy YAML configs list
    config_manager = _generate_config_manager("test_table")
    config_managers = [config_manager]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(TABLE_PART_ARGS)

    # Dummy YAML configs list
    yaml_configs_list = YAML_CONFIGS_LIST

    # Store YAML partition configs to local directory
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    builder._store_partitions(yaml_configs_list)

    # Assert file count for 1 table and sample file names
    partition_dir_contents = os.listdir(folder_path / "test_table")

    assert len(partition_dir_contents) == math.ceil(PARTITION_NUM / PARTS_PER_FILE)
    assert "0000.yaml" in partition_dir_contents
    assert "0001.yaml" in partition_dir_contents


def test_create_partition_query_yaml(module_under_test):
    """Add partition filters to ConfigManager object, build YAML config list
    and assert that the folder name, number of files and number of validations are what we would expect.
    """
    # Generate dummy YAML configs list
    config_manager = _generate_config_manager(None)
    config_managers = [config_manager]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(QUERY_PART_ARGS)

    # two partition filters are needed, one for source and one for target
    partition_filters = PARTITION_FILTERS_LIST
    master_filter_list = [[partition_filters, partition_filters]]

    # Create PartitionBuilder object and get YAML configs list
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    yaml_configs_list = builder._add_partition_filters(master_filter_list)

    assert yaml_configs_list[0]["target_folder_name"].startswith("custom.")
    assert len(yaml_configs_list[0]["yaml_files"]) == 2
    # 5 validations in the first file
    assert len(yaml_configs_list[0]["yaml_files"][0]["yaml_config"]["validations"]) == 5
    # 4 validations in the second file
    assert len(yaml_configs_list[0]["yaml_files"][1]["yaml_config"]["validations"]) == 4
