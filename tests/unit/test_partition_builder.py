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

import argparse
import os
import shutil
import pytest
import json
import random
import math
from datetime import datetime, timedelta
from unittest import mock

import ibis
import pandas

from data_validation import cli_tools, consts, exceptions
from data_validation.config_manager import ConfigManager
from data_validation.partition_builder import PartitionBuilder
from data_validation.query_builder.query_builder import FilterField, QueryBuilder

SOURCE_TABLE_FILE_PATH = "source_table_data.json"
TARGET_TABLE_FILE_PATH = "target_table_data.json"


STRING_CONSTANT = "constant"
RANDOM_STRINGS = ["a", "b", "c", "d"]

SOURCE_CONN_CONFIG = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_FILESYSTEM,
    "table_name": "my_table",
    "file_path": SOURCE_TABLE_FILE_PATH,
    "file_type": "json",
}

TARGET_CONN_CONFIG = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_FILESYSTEM,
    "table_name": "my_table",
    "file_path": TARGET_TABLE_FILE_PATH,
    "file_type": "json",
}

TEST_CONN = "{'source_type':'Example'}"
PARTITION_NUM = 9
ROWS_PER_PARTITION = 4
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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
                            "source_query": None,
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
                            "format": consts.FORMAT_TYPE_TABLE,
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


def _get_pandas_client():
    df = pandas.DataFrame([{"a": 1, "b": 2}])
    pandas_client = ibis.pandas.connect({"my_table": df})

    return pandas_client


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


def _generate_config_manager(
    table_name: str = "test_table", query: str = None, source_conn_override=None
) -> ConfigManager:
    """Returns a Dummy ConfigManager Object
    If query keyword parameter is provided, then query validation is assumed"""

    if query:
        table_name = None
    row_config = {
        # BigQuery Specific Connection Config
        "source_conn": source_conn_override or SOURCE_CONN_CONFIG,
        "target_conn": TARGET_CONN_CONFIG,
        # Validation Type
        consts.CONFIG_TYPE: consts.ROW_VALIDATION,
        # Configuration Required Depending on Validator Type
        "schema_name": None,
        "table_name": table_name,
        "target_schema_name": None,
        "target_table_name": table_name,
        consts.CONFIG_SOURCE_QUERY: query,
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
        consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
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
    mock_config_manager = _generate_config_manager()
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


def test_partition_number_and_rows_per_partition_are_mutually_exclusive():
    parser = cli_tools.configure_arg_parser()
    rows_per_partition_args = TABLE_PART_ARGS.copy()
    partition_num_index = rows_per_partition_args.index("--partition-num")
    del rows_per_partition_args[partition_num_index : partition_num_index + 2]
    rows_per_partition_args.extend(["--rows-per-partition", str(ROWS_PER_PARTITION)])

    args = parser.parse_args(rows_per_partition_args)
    assert args.partition_num is None
    assert args.rows_per_partition == ROWS_PER_PARTITION

    with pytest.raises(SystemExit):
        parser.parse_args(
            TABLE_PART_ARGS + ["--rows-per-partition", str(ROWS_PER_PARTITION)]
        )

    with pytest.raises(SystemExit):
        parser.parse_args(rows_per_partition_args[:-2])


@pytest.mark.parametrize("value", ["0", "-1", "many"])
def test_rows_per_partition_must_be_positive(value):
    args = TABLE_PART_ARGS.copy()
    partition_num_index = args.index("--partition-num")
    args[partition_num_index : partition_num_index + 2] = [
        "--rows-per-partition",
        value,
    ]
    with pytest.raises(SystemExit):
        cli_tools.configure_arg_parser().parse_args(args)


@pytest.mark.parametrize(
    "partition_argument,partition_value,source_rows,target_rows,expected_source,expected_target",
    [
        (
            "--rows-per-partition",
            4,
            10,
            10,
            ["0,1,2,3", "4,5,6,7", "8,9"],
            ["0,1,2,3", "4,5,6,7", "8,9"],
        ),
        (
            "--rows-per-partition",
            4,
            10,
            14,
            ["0,1,2,3", "4,5,6,7", "8,9"],
            ["0,1,2,3", "4,5,6,7", "8,9,10,11,12,13"],
        ),
        ("--rows-per-partition", 20, 10, 10, ["1 = 1"], ["1 = 1"]),
        ("--rows-per-partition", 4, 0, 0, ["1 = 1"], ["1 = 1"]),
        ("--rows-per-partition", 4, 1, 1, ["1 = 1"], ["1 = 1"]),
        ("--rows-per-partition", 1, 3, 3, ["0", "1", "2"], ["0", "1", "2"]),
        (
            "--partition-num",
            3,
            10,
            10,
            ["0,1,2", "3,4,5", "6,7,8,9"],
            ["0,1,2", "3,4,5", "6,7,8,9"],
        ),
    ],
)
def test_get_partition_key_filters_for_partition_mode(
    module_under_test,
    partition_argument,
    partition_value,
    source_rows,
    target_rows,
    expected_source,
    expected_target,
):
    data = pandas.DataFrame({"id": pandas.Series(range(source_rows), dtype="int64")})
    target_data = pandas.DataFrame(
        {"id": pandas.Series(range(target_rows), dtype="int64")}
    )
    ibis_client = ibis.sqlite.connect(":memory:")
    ibis_client.create_table("source_table", obj=data)
    ibis_client.create_table("target_table", obj=target_data)
    source_table = ibis_client.table("source_table")
    target_table = ibis_client.table("target_table")

    config_manager = mock.Mock()
    config_manager.primary_keys = [{"source_column": "id", "target_column": "id"}]
    config_manager.source_client = ibis_client
    config_manager.source_schema = None
    config_manager.source_table = "source_table"
    config_manager.source_query = None
    config_manager.target_client = ibis_client
    config_manager.target_schema = None
    config_manager.target_table = "target_table"
    config_manager.target_query = None

    source_row_builder = mock.Mock(query=source_table)
    source_row_builder.get_count.return_value = len(data)
    target_row_builder = mock.Mock(query=target_table)
    target_row_builder.get_count.return_value = len(target_data)

    def get_rows(table_expr, _client):
        return ",".join(str(value) for value in table_expr.execute()["id"])

    with mock.patch.object(
        module_under_test, "ValidationBuilder"
    ) as validation_builder, mock.patch.object(
        module_under_test,
        "PartitionRowBuilder",
        side_effect=[source_row_builder, target_row_builder],
    ), mock.patch.object(
        module_under_test.cli_tools, "check_no_yaml_files"
    ) as check_no_yaml_files, mock.patch.object(
        module_under_test.PartitionBuilder,
        "_extract_where",
        new=staticmethod(get_rows),
    ):
        validation_builder.return_value.source_builder = object()
        validation_builder.return_value.target_builder = object()

        parser_args = TABLE_PART_ARGS.copy()
        partition_num_index = parser_args.index("--partition-num")
        del parser_args[partition_num_index : partition_num_index + 2]
        parser_args.extend([partition_argument, str(partition_value)])
        args = cli_tools.configure_arg_parser().parse_args(parser_args)
        builder = module_under_test.PartitionBuilder([config_manager], args)

        partition_filters = builder._get_partition_key_filters()

    if partition_argument == "--rows-per-partition":
        expected_partition_count = max(1, math.ceil(len(data) / partition_value))
        check_no_yaml_files.assert_called_once_with(
            expected_partition_count, args.parts_per_file
        )
    else:
        check_no_yaml_files.assert_not_called()

    assert partition_filters == [[expected_source, expected_target]]


def test_rows_per_partition_uses_filtered_counts_and_boundaries(module_under_test):
    client = ibis.sqlite.connect(":memory:")
    client.create_table("source_table", obj=pandas.DataFrame({"id": range(10)}))
    client.create_table("target_table", obj=pandas.DataFrame({"id": range(14)}))
    source_builder = QueryBuilder([], [], [FilterField.greater_than("id", 3)], [], [])
    target_builder = QueryBuilder([], [], [FilterField.greater_than("id", 7)], [], [])
    config_manager = mock.Mock(
        primary_keys=[{"source_column": "id", "target_column": "id"}],
        source_client=client,
        target_client=client,
        source_schema=None,
        target_schema=None,
        source_table="source_table",
        target_table="target_table",
        source_query=None,
        target_query=None,
    )
    target_rows = module_under_test.PartitionRowBuilder(
        ["id"], client, None, "target_table", None, target_builder
    )
    assert target_rows.get_count() == 6

    args = TABLE_PART_ARGS.copy()
    partition_num_index = args.index("--partition-num")
    args[partition_num_index : partition_num_index + 2] = ["--rows-per-partition", "4"]
    builder = module_under_test.PartitionBuilder(
        [config_manager], cli_tools.configure_arg_parser().parse_args(args)
    )

    def get_rows(table_expr, _client):
        return ",".join(str(value) for value in table_expr.execute()["id"])

    with mock.patch.object(
        module_under_test, "ValidationBuilder"
    ) as validation_builder, mock.patch.object(
        module_under_test.cli_tools, "check_no_yaml_files"
    ) as check_no_yaml_files, mock.patch.object(
        module_under_test.PartitionBuilder, "_extract_where", new=staticmethod(get_rows)
    ):
        validation_builder.return_value.source_builder = source_builder
        validation_builder.return_value.target_builder = target_builder
        partition_filters = builder._get_partition_key_filters()

    check_no_yaml_files.assert_called_once_with(2, builder.args.parts_per_file)
    assert partition_filters == [[["4,5,6,7", "8,9"], ["", "8,9,10,11,12,13"]]]


def test_rows_per_partition_generated_sql_filters(module_under_test):
    client = ibis.sqlite.connect(":memory:")
    client.create_table("source_table", obj=pandas.DataFrame({"id": range(10)}))
    client.create_table("target_table", obj=pandas.DataFrame({"id": range(14)}))
    source = mock.Mock(query=client.table("source_table"))
    target = mock.Mock(query=client.table("target_table"))
    source.get_count.return_value = 10
    target.get_count.return_value = 14
    config_manager = mock.Mock(
        primary_keys=[{"source_column": "id", "target_column": "id"}],
        source_client=client,
        target_client=client,
        source_schema=None,
        target_schema=None,
        source_table="source_table",
        target_table="target_table",
        source_query=None,
        target_query=None,
    )
    args = TABLE_PART_ARGS.copy()
    partition_num_index = args.index("--partition-num")
    args[partition_num_index : partition_num_index + 2] = ["--rows-per-partition", "4"]
    builder = module_under_test.PartitionBuilder(
        [config_manager], cli_tools.configure_arg_parser().parse_args(args)
    )

    with mock.patch.object(module_under_test, "ValidationBuilder"), mock.patch.object(
        module_under_test, "PartitionRowBuilder", side_effect=[source, target]
    ), mock.patch("data_validation.clients.is_sqlalchemy_backend", return_value=True):
        source_filters, target_filters = builder._get_partition_key_filters()[0]

    for table, filters, expected in (
        ("source_table", source_filters, [list(range(4)), list(range(4, 8)), [8, 9]]),
        (
            "target_table",
            target_filters,
            [list(range(4)), list(range(4, 8)), list(range(8, 14))],
        ),
    ):
        actual = []
        for partition_filter in filters:
            rows = client.raw_sql(
                f"SELECT id FROM {table} WHERE {partition_filter} ORDER BY id"
            ).fetchall()
            actual.append([row[0] for row in rows])
        assert actual == expected


def test_rows_per_partition_checks_file_limit_before_fetching_boundaries(
    module_under_test,
):
    args = TABLE_PART_ARGS.copy()
    partition_num_index = args.index("--partition-num")
    args[partition_num_index : partition_num_index + 2] = ["--rows-per-partition", "1"]
    args[args.index("--parts-per-file") + 1] = "1"
    builder = module_under_test.PartitionBuilder(
        [mock.Mock(primary_keys=[])], cli_tools.configure_arg_parser().parse_args(args)
    )
    row_builder = mock.Mock()
    row_builder.get_count.return_value = 10001
    with mock.patch.object(module_under_test, "ValidationBuilder"), mock.patch.object(
        module_under_test, "PartitionRowBuilder", return_value=row_builder
    ):
        with pytest.raises(argparse.ArgumentTypeError, match="10,000"):
            builder._get_partition_key_filters()


@pytest.mark.parametrize(
    "ibis_table_str,where_exp",
    [
        (
            "SELECT t0.*\nFROM `pso-kokoro-resources.pso_data_validator.dvt_core_types` t0\nWHERE t0.`id` < 2",
            "`id` < 2",
        ),
        (
            'SELECT t0.*\nFROM udf.test_generate_partitions_v2 t0\nWHERE ((t0."course_id" < \'ALG002\') OR ((t0."course_id" = \'ALG002\') AND (t0."quarter_id" < 1234)))',
            '(("course_id" < \'ALG002\') OR (("course_id" = \'ALG002\') AND ("quarter_id" < 1234)))',
        ),
        (
            'SELECT t0."course_id", t0."quarter_id", t0."recd_timestamp",\n       t0."registration_date", t0."approved", t0."grade"\nFROM (\n  select * from udf.test_generate_partitions_v2 where course_id < \'ALG003\'\n) t0\nWHERE (t0."course_id" < \'ALG002\') OR ((t0."course_id" = \'ALG002\') AND (t0."quarter_id" < 1234))',
            '("course_id" < \'ALG002\') OR (("course_id" = \'ALG002\') AND ("quarter_id" < 1234))',
        ),
        ("SELECT t0.a FROM sch.tbl t0\nWHERE t0.a < ' WHERE '", "t0.a < ' WHERE '"),
        ('SELECT t0."at0.b" FROM sch.tbl t0\nWHERE t0."at0.b" < 2', '"at0.b" < 2'),
    ],
)
@mock.patch("data_validation.util.ibis_table_to_sql")
def test_extract_where(mocked_to_sql, ibis_table_str, where_exp):
    failed_cases = [
        "SELECT t0.a FROM sch.tbl t0\nWHERE t0.a < ' WHERE '",
        'SELECT t0."at0.b" FROM sch.tbl t0\nWHERE t0."at0.b" < 2',
    ]
    if ibis_table_str in failed_cases:
        pytest.skip("Skipping test_extract_where due to issue 1503")
    mocked_to_sql.return_value = ibis_table_str
    assert (
        PartitionBuilder._extract_where("dummy ibis table", _get_pandas_client())
        == where_exp
    )


def test_add_partition_filters_to_config(module_under_test):
    """Add partition filters to ConfigManager object, build YAML config list
    and assert YAML configs
    """
    # Generate dummy YAML configs list
    config_manager = _generate_config_manager()
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


def test_add_partition_filters_to_multiple_configs(module_under_test):
    """Add partition filters to 2 ConfigManager objects, build YAML config list
    and assert number of validations twice as in single config. Generally we don't expect users to invoke generate-table-partitions with
    multiple table pairs from command line, where both pairs have the same primary key, columns to validate,
    number of partitions and partitions per file. DVT can add the same partition_filters to multiple configs
    because the validation for a table with numerous columns had to be split into multiple validations performed serially.
    Each validation has its own config with fewer columns which can be successfully validated.
    This test addresses issue 1549.
    """
    # Generate dummy YAML configs list
    config_manager = _generate_config_manager()
    config_managers = [config_manager, config_manager]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(TABLE_PART_ARGS)

    # two partition filters are needed, one for source and one for target
    # have to add a pair for each config_manager object
    master_filter_list = []
    for i in range(len(config_managers)):
        master_filter_list.append([PARTITION_FILTERS_LIST, PARTITION_FILTERS_LIST])

    # Create PartitionBuilder object and get YAML configs list
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    yaml_configs_list = builder._add_partition_filters(master_filter_list)

    # There were nine validations with one table in
    # test_add_partition_filters_to_config, there should be 18 now.
    num_val = 0
    for yaml_config in yaml_configs_list:
        for yaml_file in yaml_config["yaml_files"]:
            num_val = num_val + len(yaml_file["yaml_config"]["validations"])
    assert num_val == 18


def test_store_yaml_partitions(module_under_test, tmp_path):
    """Store all the Partition YAMLs for a table to specified local directory"""

    # Create test partitions directory to store results
    folder_path = tmp_path / PARTITIONS_DIR
    folder_path.mkdir()
    TABLE_PART_ARGS[TABLE_PART_ARGS.index("-cdir") + 1] = str(folder_path)

    # Generate dummy YAML configs list
    config_manager = _generate_config_manager()
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
    config_manager = _generate_config_manager(query="Select 1")
    config_managers = [config_manager]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(QUERY_PART_ARGS)

    # two partition filters are needed, one for source and one for target
    partition_filters = PARTITION_FILTERS_LIST
    master_filter_list = [[partition_filters, partition_filters]]

    # Create PartitionBuilder object and get YAML configs list
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    yaml_configs_list = builder._add_partition_filters(master_filter_list)

    assert yaml_configs_list[0]["target_folder_name"] == ""
    assert len(yaml_configs_list[0]["yaml_files"]) == 2
    # 5 validations in the first file
    assert len(yaml_configs_list[0]["yaml_files"][0]["yaml_config"]["validations"]) == 5
    # 4 validations in the second file
    assert len(yaml_configs_list[0]["yaml_files"][1]["yaml_config"]["validations"]) == 4


@mock.patch("data_validation.clients.get_data_client")
def test_check_partition_configs_sybase(mock_get_data_client, module_under_test):
    """Ensure check_partition_filters raises exception for Sybase source connection"""
    # Generate dummy YAML configs list
    sybase_conn_config = {
        consts.SOURCE_TYPE: consts.SOURCE_TYPE_SYBASE,
    }
    config_managers = [
        _generate_config_manager(source_conn_override=sybase_conn_config)
    ]

    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(TABLE_PART_ARGS)

    # Create PartitionBuilder object and get YAML configs list
    builder = module_under_test.PartitionBuilder(config_managers, mock_args)
    with pytest.raises(exceptions.PartitionBuilderException) as excinfo:
        builder.check_partition_configs()
    assert "window" in str(excinfo.value)


def test_definitely_no_time_part_true():
    """Test _definitely_no_time_part returns True when no time part is present."""
    dt = datetime(2023, 10, 27, 0, 0, 0, 0)
    assert PartitionBuilder._definitely_no_time_part(dt) is True


def test_definitely_no_time_part_false_hour():
    """Test _definitely_no_time_part returns False when hour is present."""
    dt = datetime(2023, 10, 27, 1, 0, 0, 0)
    assert PartitionBuilder._definitely_no_time_part(dt) is False
