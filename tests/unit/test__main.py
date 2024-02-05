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
from unittest import mock

from data_validation import cli_tools, consts
from data_validation import __main__ as main
from tests.system.data_sources.test_bigquery import BQ_CONN


TEST_CONN = '{"source_type":"Example"}'
CLI_ARGS = {
    "command": "validate",
    "validate_cmd": "column",
    "source_conn": TEST_CONN,
    "target_conn": TEST_CONN,
    "tables_list": "my_schema.my_table",
    "sum": "col_a,col_b",
    "count": "col_a,col_b",
    "config_file": "example_test.yaml",
    "verbose": True,
}

SCHEMA_TABLE_OBJ = {
    consts.CONFIG_SCHEMA_NAME: "schema",
    consts.CONFIG_TABLE_NAME: "table",
}
OTHER_SCHEMA_TABLE_OBJ = {
    consts.CONFIG_SCHEMA_NAME: "schema",
    consts.CONFIG_TABLE_NAME: "other_table",
}
SOURCE_TABLE_MAP = {
    "schema_table": SCHEMA_TABLE_OBJ,
}
TARGET_TABLE_MAP = {
    "schema_table": SCHEMA_TABLE_OBJ,
    "schema_other_table": OTHER_SCHEMA_TABLE_OBJ,
}
RESULT_TABLE_CONFIGS = [
    {
        "schema_name": "schema",
        "table_name": "table",
        "target_schema_name": "schema",
        "target_table_name": "table",
    }
]
TEST_JSON_VALIDATION_CONFIG = {
    consts.CONFIG_TYPE: "Column",
    consts.CONFIG_SOURCE_CONN_NAME: "bq",
    consts.CONFIG_TARGET_CONN_NAME: "bq",
    consts.CONFIG_TABLE_NAME: "dvt_core_types",
    consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
    consts.CONFIG_TARGET_SCHEMA_NAME: "pso_data_validator",
    consts.CONFIG_TARGET_TABLE_NAME: "dvt_core_types",
    consts.CONFIG_LABELS: [],
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FILTERS: [],
    consts.CONFIG_USE_RANDOM_ROWS: False,
    consts.CONFIG_RANDOM_ROW_BATCH_SIZE: None,
    consts.CONFIG_FILTER_STATUS: ["fail"],
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
            consts.CONFIG_TYPE: "count",
        },
    ],
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
}


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CLI_ARGS),
)
def test_configure_arg_parser(mock_args):
    """Test arg parser values."""
    args = cli_tools.get_parsed_args()
    file_path = main._get_arg_config_file(args)

    assert file_path == "example_test.yaml"


def test__compare_match_tables():
    """Test matching tables from source and target."""
    table_configs = main._compare_match_tables(SOURCE_TABLE_MAP, TARGET_TABLE_MAP)

    assert table_configs == RESULT_TABLE_CONFIGS


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    return_value=BQ_CONN,
)
def test_column_validation_convert_config_to_json(mock_conn):
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--config-file-json=bq-column-validation.json",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1

    json_config = main.convert_config_to_json(config_managers)
    # assert structure
    assert json_config == TEST_JSON_VALIDATION_CONFIG
