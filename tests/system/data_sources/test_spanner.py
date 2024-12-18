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
from unittest import mock

import pytest

from data_validation import cli_tools, consts, data_validation, find_tables
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    raw_query_test,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    schema_validation_test,
    column_validation_test,
    row_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN


PROJECT_ID = os.environ["PROJECT_ID"]
SPANNER_CONN_NAME = "spanner-integration-test"
CLI_FIND_TABLES_ARGS = [
    "find-tables",
    "--source-conn",
    "mock-conn",
    "--target-conn",
    "mock-conn",
]

SPANNER_CONN = {
    "source_type": "Spanner",
    "project_id": PROJECT_ID,
    "instance_id": "span1",
    "database_id": "pso_data_validator",
}


def mock_get_connection_config(*args):
    if args[1] in ("spanner-conn", "mock-conn"):
        return SPANNER_CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@pytest.fixture
def count_config():
    return {
        # Connection Name
        consts.CONFIG_SOURCE_CONN: SPANNER_CONN,
        consts.CONFIG_TARGET_CONN: SPANNER_CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
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
        consts.CONFIG_FILTER_STATUS: None,
    }


@pytest.fixture
def grouped_config():
    return {
        # Connection Name
        consts.CONFIG_SOURCE_CONN: SPANNER_CONN,
        consts.CONFIG_TARGET_CONN: SPANNER_CONN,
        # Validation Type
        consts.CONFIG_TYPE: "GroupedColumn",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
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
        consts.CONFIG_FILTER_STATUS: None,
    }


CLI_CONFIG_FILE = "example_test.yaml"
CLI_RUN_CONFIG_ARGS = ["run-config", "--config-file", CLI_CONFIG_FILE]


STRING_MATCH_RESULT = '{"schema_name": "pso_data_validator", "table_name": "results", "target_schema_name": "pso_data_validator", "target_table_name": "results"}'


def test_count_validator(count_config):
    validator = data_validation.DataValidation(count_config, verbose=True)
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
    validator = data_validation.DataValidation(grouped_config, verbose=True)
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


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_cli_find_tables():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_FIND_TABLES_ARGS)
    tables_json = find_tables.find_tables_using_string_matching(args)
    tables = json.loads(tables_json)
    assert isinstance(tables_json, str)
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "array_table",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "array_table",
    } in tables
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "functional_alltypes",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "functional_alltypes",
    } in tables
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "students_pointer",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "students_pointer",
    } in tables
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "dvt_core_types_vw",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "dvt_core_types_vw",
    } not in tables


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Spanner to Spanner dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Spanner to Spanner dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types column validation"""
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        grouped_columns="col_varchar_30",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Spanner to Spanner dvt_core_types row validation"""
    row_validation_test(
        tc="mock-conn",
        hash="*",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types column validation"""
    row_validation_test(
        tc="bq-conn",
        # TODO Change --hash to include col_date and col_datetime when issue-1061 is complete.
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_string,col_tstz",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """Spanner to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_binary",
            "--primary-keys=binary_id",
            "--hash=int_id,other_data",
            "--use-random-row",
            "--random-row-batch-size=5",
        ]
    )
    df = run_test_from_cli_args(args)
    binary_key_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_validation_core_types():
    """Spanner to Spanner dvt_core_types custom-query validation"""
    custom_query_validation_test(
        tc="mock-conn",
        source_query="select * from dvt_core_types",
        target_query="select * from dvt_core_types",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """Spanner dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Spanner dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(validation_type="custom-query", schema=None)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys, table="dvt_core_types")
