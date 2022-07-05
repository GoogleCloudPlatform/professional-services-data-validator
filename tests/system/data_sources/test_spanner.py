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

import datetime
import json
import os

import pytest

from data_validation import cli_tools, consts, data_validations
from data_validation import __main__ as main
from data_validation import state_manager
from third_party.ibis.ibis_cloud_spanner.tests import conftest


SPANNER_CONN_NAME = "spanner-integration-test"
CLI_FIND_TABLES_ARGS = [
    "find-tables",
    "--source-conn",
    SPANNER_CONN_NAME,
    "--target-conn",
    SPANNER_CONN_NAME,
]


# Copy text fixtures from Spanner Ibis client tests, because it's a bit verbose
# to create a Spanner instance and load data to it. Relevant test fixtures can
# be copied here after clients are upstreamed to Ibis.
spanner_client = conftest.spanner_client
instance_id = conftest.instance_id
database_id = conftest.database_id


@pytest.fixture
def spanner_connection_config(instance_id, database_id):
    return {
        "source_type": "Spanner",
        "project_id": os.environ["PROJECT_ID"],
        "instance_id": instance_id,
        "database_id": database_id,
    }


@pytest.fixture
def spanner_connection_args(instance_id, database_id):
    return [
        "connections",
        "add",
        "--connection-name",
        SPANNER_CONN_NAME,
        "Spanner",
        "--project-id",
        os.environ["PROJECT_ID"],
        "--instance-id",
        instance_id,
        "--database-id",
        database_id,
    ]


@pytest.fixture
def count_config(spanner_connection_config, database_id):
    return {
        # Connection Name
        consts.CONFIG_SOURCE_CONN: spanner_connection_config,
        consts.CONFIG_TARGET_CONN: spanner_connection_config,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: database_id,
        consts.CONFIG_TABLE_NAME: "functional_alltypes",
        consts.CONFIG_GROUPED_COLUMNS: [],
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: "string_col",
                consts.CONFIG_TARGET_COLUMN: "string_col",
                consts.CONFIG_FIELD_ALIAS: "count_string_col",
            },
            {
                consts.CONFIG_TYPE: "avg",
                consts.CONFIG_SOURCE_COLUMN: "float_col",
                consts.CONFIG_TARGET_COLUMN: "float_col",
                consts.CONFIG_FIELD_ALIAS: "avg_float_col",
            },
            {
                consts.CONFIG_TYPE: "max",
                consts.CONFIG_SOURCE_COLUMN: "timestamp_col",
                consts.CONFIG_TARGET_COLUMN: "timestamp_col",
                consts.CONFIG_FIELD_ALIAS: "max_timestamp_col",
            },
            {
                consts.CONFIG_TYPE: "min",
                consts.CONFIG_SOURCE_COLUMN: "int_col",
                consts.CONFIG_TARGET_COLUMN: "int_col",
                consts.CONFIG_FIELD_ALIAS: "min_int_col",
            },
        ],
        consts.CONFIG_FORMAT: "table",
    }


@pytest.fixture
def grouped_config(spanner_connection_config, database_id):
    return {
        # Connection Name
        consts.CONFIG_SOURCE_CONN: spanner_connection_config,
        consts.CONFIG_TARGET_CONN: spanner_connection_config,
        # Validation Type
        consts.CONFIG_TYPE: "GroupedColumn",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: database_id,
        consts.CONFIG_TABLE_NAME: "functional_alltypes",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: "sum",
                consts.CONFIG_SOURCE_COLUMN: "float_col",
                consts.CONFIG_TARGET_COLUMN: "float_col",
                consts.CONFIG_FIELD_ALIAS: "sum_float_col",
            },
        ],
        consts.CONFIG_GROUPED_COLUMNS: [
            {
                consts.CONFIG_FIELD_ALIAS: "timestamp_col",
                consts.CONFIG_SOURCE_COLUMN: "timestamp_col",
                consts.CONFIG_TARGET_COLUMN: "timestamp_col",
                consts.CONFIG_CAST: "date",
            },
        ],
        consts.CONFIG_FORMAT: "table",
    }


CLI_CONFIG_FILE = "example_test.yaml"
CLI_RUN_CONFIG_ARGS = ["run-config", "--config-file", CLI_CONFIG_FILE]


STRING_MATCH_RESULT = '{"schema_name": "pso_data_validator", "table_name": "results", "target_schema_name": "pso_data_validator", "target_table_name": "results"}'


def test_count_validator(count_config):
    validator = data_validations.DataValidation(count_config, verbose=True)
    df = validator.execute()

    count_value = df[df["validation_name"] == "count"]["source_agg_value"].values[0]
    count_string_value = df[df["validation_name"] == "count_string_col"][
        "source_agg_value"
    ].values[0]
    avg_float_value = df[df["validation_name"] == "avg_float_col"][
        "source_agg_value"
    ].values[0]
    max_timestamp_value = df[df["validation_name"] == "max_timestamp_col"][
        "source_agg_value"
    ].values[0]
    min_int_value = df[df["validation_name"] == "min_int_col"][
        "source_agg_value"
    ].values[0]

    assert float(count_value) > 0
    assert float(count_string_value) > 0
    assert float(avg_float_value) > 0
    assert datetime.datetime.strptime(
        max_timestamp_value,
        "%Y-%m-%d %H:%M:%S%z",
    ) > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    assert float(min_int_value) > 0


def test_grouped_count_validator(grouped_config):
    validator = data_validations.DataValidation(grouped_config, verbose=True)
    df = validator.execute()
    rows = list(df[df["validation_name"] == "count"].iterrows())

    # Check that all partitions are unique.
    partitions = frozenset(df["group_by_columns"])
    assert len(rows) == len(partitions)
    assert len(rows) > 1
    assert df["source_agg_value"].sum() == df["target_agg_value"].sum()

    for _, row in rows:
        assert float(row["source_agg_value"]) > 0
        assert row["source_agg_value"] == row["target_agg_value"]


def test_cli_find_tables(spanner_connection_args, database_id):
    _store_spanner_conn(spanner_connection_args)

    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_FIND_TABLES_ARGS)
    tables_json = main.find_tables_using_string_matching(args)
    tables = json.loads(tables_json)
    assert isinstance(tables_json, str)
    assert {
        "schema_name": database_id,
        "table_name": "array_table",
        "target_schema_name": database_id,
        "target_table_name": "array_table",
    } in tables
    assert {
        "schema_name": database_id,
        "table_name": "functional_alltypes",
        "target_schema_name": database_id,
        "target_table_name": "functional_alltypes",
    } in tables
    assert {
        "schema_name": database_id,
        "table_name": "students_pointer",
        "target_schema_name": database_id,
        "target_table_name": "students_pointer",
    } in tables

    _remove_spanner_conn()


def _store_spanner_conn(spanner_connection_args):
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(spanner_connection_args)
    main.run_connections(mock_args)


def _remove_spanner_conn():
    mgr = state_manager.StateManager()
    file_path = mgr._get_connection_path(SPANNER_CONN_NAME)
    os.remove(file_path)
