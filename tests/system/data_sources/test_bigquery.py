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

from data_validation import cli_tools, consts, data_validation
from data_validation import __main__ as main


BQ_CONN = {"source_type": "BigQuery", "project_id": os.environ["PROJECT_ID"]}
CONFIG_COUNT_VALID = {
    # BigQuery Specific Connection Name
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TABLE_NAME: "citibike_trips",
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
            consts.CONFIG_SOURCE_COLUMN: "tripduration",
            consts.CONFIG_TARGET_COLUMN: "tripduration",
            consts.CONFIG_FIELD_ALIAS: "count_tripduration",
        },
        {
            consts.CONFIG_TYPE: "avg",
            consts.CONFIG_SOURCE_COLUMN: "tripduration",
            consts.CONFIG_TARGET_COLUMN: "tripduration",
            consts.CONFIG_FIELD_ALIAS: "avg_tripduration",
        },
        {
            consts.CONFIG_TYPE: "max",
            consts.CONFIG_SOURCE_COLUMN: "birth_year",
            consts.CONFIG_TARGET_COLUMN: "birth_year",
            consts.CONFIG_FIELD_ALIAS: "max_birth_year",
        },
        {
            consts.CONFIG_TYPE: "min",
            consts.CONFIG_SOURCE_COLUMN: "birth_year",
            consts.CONFIG_TARGET_COLUMN: "birth_year",
            consts.CONFIG_FIELD_ALIAS: "min_birth_year",
        },
    ],
}

CONFIG_GROUPED_COUNT_VALID = {
    # BigQuery Specific Connection Config
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
    # Validation Type
    consts.CONFIG_TYPE: "GroupedColumn",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TABLE_NAME: "citibike_trips",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
        },
        {
            consts.CONFIG_TYPE: "sum",
            consts.CONFIG_SOURCE_COLUMN: "tripduration",
            consts.CONFIG_TARGET_COLUMN: "tripduration",
            consts.CONFIG_FIELD_ALIAS: "sum_tripduration",
        },
    ],
    consts.CONFIG_GROUPED_COLUMNS: [
        {
            consts.CONFIG_FIELD_ALIAS: "starttime",
            consts.CONFIG_SOURCE_COLUMN: "starttime",
            consts.CONFIG_TARGET_COLUMN: "starttime",
            consts.CONFIG_CAST: "date",
        },
    ],
}

BQ_CONN_NAME = "bq-integration-test"
CLI_CONFIG_FILE = "example_test.yaml"

BQ_CONN_ARGS = [
    "connections",
    "add",
    "--connection-name",
    BQ_CONN_NAME,
    "BigQuery",
    "--project-id",
    os.environ["PROJECT_ID"],
]
CLI_STORE_COLUMN_ARGS = [
    "run",
    "--type",
    "Column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]',
    "--sum",
    '["tripduration","start_station_name"]',
    "--count",
    '["tripduration","start_station_name"]',
    "--label",
    "test_label",
    "--config-file",
    CLI_CONFIG_FILE,
]
CLI_RUN_CONFIG_ARGS = ["run-config", "--config-file", CLI_CONFIG_FILE]

CLI_FIND_TABLES_ARGS = [
    "find-tables",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
]

STRING_MATCH_RESULT = '{"schema_name": "pso_data_validator", "table_name": "results", "target_schema_name": "pso_data_validator", "target_table_name": "results"}'


def test_count_validator():
    validator = data_validation.DataValidation(CONFIG_COUNT_VALID, verbose=True)
    df = validator.execute()

    count_value = df[df["validation_name"] == "count"]["source_agg_value"].values[0]
    count_tripduration_value = df[df["validation_name"] == "count_tripduration"][
        "source_agg_value"
    ].values[0]
    avg_tripduration_value = df[df["validation_name"] == "count_tripduration"][
        "source_agg_value"
    ].values[0]
    max_birth_year_value = df[df["validation_name"] == "max_birth_year"][
        "source_agg_value"
    ].values[0]
    min_birth_year_value = df[df["validation_name"] == "min_birth_year"][
        "source_agg_value"
    ].values[0]

    assert float(count_value) > 0
    assert float(count_tripduration_value) > 0
    assert float(avg_tripduration_value) > 0
    assert float(max_birth_year_value) > 0
    assert float(min_birth_year_value) > 0
    assert (
        df["source_agg_value"].astype(float).sum()
        == df["target_agg_value"].astype(float).sum()
    )


def test_grouped_count_validator():
    validator = data_validation.DataValidation(CONFIG_GROUPED_COUNT_VALID, verbose=True)
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


def test_cli_store_yaml_then_run():
    # Store BQ Connection
    _store_bq_conn()

    # Build validation and store to file
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_STORE_COLUMN_ARGS)
    main.run(mock_args)

    yaml_file_path = CLI_CONFIG_FILE
    with open(yaml_file_path, "r") as yaml_file:
        # The number of lines is not significant, except that it represents
        # the exact file expected to be created.  Any change to this value
        # is likely to be a breaking change and must be assessed.
        assert len(yaml_file.readlines()) == 28

    # Run generated config
    run_config_args = parser.parse_args(CLI_RUN_CONFIG_ARGS)
    config_managers = main.build_config_managers_from_yaml(run_config_args)
    main.run_validations(run_config_args, config_managers)

    os.remove(yaml_file_path)
    _remove_bq_conn()


def test_cli_find_tables():
    _store_bq_conn()

    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_FIND_TABLES_ARGS)
    tables_json = main.find_tables_using_string_matching(args)
    assert isinstance(tables_json, str)
    assert STRING_MATCH_RESULT in tables_json

    _remove_bq_conn()


def _store_bq_conn():
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(BQ_CONN_ARGS)
    main.run_connections(mock_args)


def _remove_bq_conn():
    file_path = cli_tools._get_connection_file(BQ_CONN_NAME)
    os.remove(file_path)
