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

from data_validation import __main__ as main
from data_validation import cli_tools, clients, consts, data_validation, state_manager
from data_validation.query_builder import random_row_builder
from data_validation.query_builder.query_builder import QueryBuilder

PROJECT_ID = os.environ["PROJECT_ID"]
os.environ[consts.ENV_DIRECTORY_VAR] = f"gs://{PROJECT_ID}/integration_tests/"
BQ_CONN = {"source_type": "BigQuery", "project_id": PROJECT_ID}
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
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}

CONFIG_GROUPED_COUNT_VALID = {
    # BigQuery Specific Connection Config
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
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
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}

CONFIG_TIMESTAMP_AGGS = {
    consts.CONFIG_TYPE: "Column",
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
    consts.CONFIG_TABLE_NAME: "bikeshare_trips",
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.san_francisco_bikeshare",
    consts.CONFIG_TARGET_SCHEMA_NAME: "bigquery-public-data.san_francisco_bikeshare",
    consts.CONFIG_TARGET_TABLE_NAME: "bikeshare_trips",
    consts.CONFIG_LABELS: [],
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FILTERS: [],
    consts.CONFIG_USE_RANDOM_ROWS: False,
    consts.CONFIG_RANDOM_ROW_BATCH_SIZE: None,
    consts.CONFIG_CALCULATED_FIELDS: [
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["start_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["start_date"],
            consts.CONFIG_FIELD_ALIAS: "cast_timestamp__start_date",
            consts.CONFIG_TYPE: "cast",
            consts.CONFIG_DEFAULT_CAST: "timestamp",
            consts.CONFIG_DEPTH: 0,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["cast_timestamp__start_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["cast_timestamp__start_date"],
            consts.CONFIG_FIELD_ALIAS: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TYPE: "epoch_seconds",
            consts.CONFIG_DEPTH: 1,
        },
    ],
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
            consts.CONFIG_TYPE: "count",
        },
        {
            consts.CONFIG_SOURCE_COLUMN: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TARGET_COLUMN: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_FIELD_ALIAS: "sum__epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TYPE: "sum",
        },
        {
            consts.CONFIG_SOURCE_COLUMN: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TARGET_COLUMN: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_FIELD_ALIAS: "avg__epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TYPE: "avg",
        },
        {
            consts.CONFIG_SOURCE_COLUMN: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TARGET_COLUMN: "epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_FIELD_ALIAS: "bit_xor__epoch_seconds__cast_timestamp__start_date",
            consts.CONFIG_TYPE: "bit_xor",
        },
        {
            consts.CONFIG_SOURCE_COLUMN: "start_date",
            consts.CONFIG_TARGET_COLUMN: "start_date",
            consts.CONFIG_FIELD_ALIAS: "min__start_date",
            consts.CONFIG_TYPE: "min",
        },
        {
            consts.CONFIG_SOURCE_COLUMN: "start_date",
            consts.CONFIG_TARGET_COLUMN: "start_date",
            consts.CONFIG_FIELD_ALIAS: "max__start_date",
            consts.CONFIG_TYPE: "max",
        },
    ],
}


# TODO: The definition for this table is stored in: ./tests/resources/
CONFIG_NUMERIC_AGG_VALID = {
    # BigQuery Specific Connection Config
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
    consts.CONFIG_TABLE_NAME: "test_data_types",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
        },
        {
            consts.CONFIG_TYPE: "sum",
            consts.CONFIG_SOURCE_COLUMN: "int_type",
            consts.CONFIG_TARGET_COLUMN: "decimal_type",
            consts.CONFIG_FIELD_ALIAS: "compare_int_decimal",
        },
        {
            consts.CONFIG_TYPE: "sum",
            consts.CONFIG_SOURCE_COLUMN: "bignumeric_type",
            consts.CONFIG_TARGET_COLUMN: "bignumeric_type",
            consts.CONFIG_FIELD_ALIAS: "compare_bignumeric",
        },
    ],
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}

CONFIG_SCHEMA_VALIDATION = {
    # BigQuery Specific Connection Config
    consts.CONFIG_SOURCE_CONN: BQ_CONN,
    consts.CONFIG_TARGET_CONN: BQ_CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Schema",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TABLE_NAME: "citibike_trips",
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
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
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.new_york_citibike.citibike_trips",
    "--sum",
    "tripduration,start_station_name",
    "--count",
    '["tripduration","start_station_name"]',
    "--labels",
    "name=test_label",
    "--threshold",
    0.0,
    "--config-file",
    CLI_CONFIG_FILE,
]
CLI_STORE_COLUMN_ARGS_DIR = [
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.new_york_citibike.citibike_trips",
    "--sum",
    "tripduration,start_station_name",
    "--count",
    '["tripduration","start_station_name"]',
    "--labels",
    "name=test_label",
    "--threshold",
    0.0,
    "--config-file",
    "./tmp_test_dir_dvt/example_test_dir.yaml",
]
CLI_STORE_COLUMN_ARGS_DIR2 = [
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.new_york_citibike.citibike_trips",
    "--sum",
    "tripduration,start_station_name",
    "--count",
    '["tripduration","start_station_name"]',
    "--labels",
    "name=test_label",
    "--threshold",
    0.0,
    "--config-file",
    "./tmp_test_dir_dvt/example_test_dir2.yaml",
]

EXPECTED_NUM_YAML_LINES = 48  # Expected number of lines for validation config generated by CLI_STORE_COLUMN_ARGS
CLI_RUN_CONFIG_ARGS = ["run-config", "--config-file", CLI_CONFIG_FILE]
CLI_CONFIGS_RUN_ARGS = ["configs", "run", "--config-file", CLI_CONFIG_FILE]
CLI_CONFIG_DIR_RUN_ARGS_LOCAL = [
    "configs",
    "run",
    "--config-dir",
    "./tmp_test_dir_dvt/",
]
CLI_CONFIG_DIR_RUN_ARGS_GCS = ["configs", "run", "--config-dir", "gs://"]

CLI_WILDCARD_STRING_ARGS = [
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.new_york_citibike.citibike_trips",
    "--sum",
    "*",
    "--wildcard-include-string-len",
    "--config-file",
    CLI_CONFIG_FILE,
]
EXPECTED_NUM_YAML_LINES_WILDCARD = 151

CLI_TIMESTAMP_MIN_MAX_ARGS = [
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.san_francisco_bikeshare.bikeshare_trips",
    "--min",
    "start_date",
    "--max",
    "end_date",
    "--config-file",
    CLI_CONFIG_FILE,
]
EXPECTED_NUM_YAML_LINES_TIMESTAMP_MIN_MAX = 30

CLI_TIMESTAMP_SUM_AVG_BITXOR_ARGS = [
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.san_francisco_bikeshare.bikeshare_trips",
    "--sum",
    "start_date",
    "--avg",
    "start_date",
    "--bit_xor",
    "start_date",
    "--config-file",
    CLI_CONFIG_FILE,
]
EXPECTED_NUM_YAML_LINES_TIMESTAMP_SUM_AVG_BITXOR = 49

CLI_BQ_DATETIME_SUM_AVG_BITXOR_ARGS = [
    "validate",
    "column",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--tables-list",
    "bigquery-public-data.new_york_citibike.citibike_stations",
    "--sum",
    "last_reported",
    "--avg",
    "last_reported",
    "--bit_xor",
    "last_reported",
    "--config-file",
    CLI_CONFIG_FILE,
]
EXPECTED_NUM_YAML_LINES_BQ_DATETIME_SUM_AVG_BITXOR = 49

CLI_FIND_TABLES_ARGS = [
    "find-tables",
    "--source-conn",
    BQ_CONN_NAME,
    "--target-conn",
    BQ_CONN_NAME,
    "--allowed-schemas",
    "pso_data_validator",
]

STRING_MATCH_RESULT = '{"schema_name": "pso_data_validator", "table_name": "results", "target_schema_name": "pso_data_validator", "target_table_name": "results"}'

EXPECTED_RANDOM_ROW_QUERY = """
SELECT `station_id`
FROM (
  SELECT *
  FROM `bigquery-public-data.new_york_citibike.citibike_stations`
  ORDER BY RAND()
  LIMIT 10
) t0
""".strip()


def test_count_validator():
    validator = data_validation.DataValidation(CONFIG_COUNT_VALID, verbose=True)
    df = validator.execute()

    count_value = df[df["validation_name"] == "count"]["source_agg_value"].values[0]
    count_tripduration_value = df[df["validation_name"] == "count_tripduration"][
        "source_agg_value"
    ].values[0]
    avg_tripduration_value = df[df["validation_name"] == "avg_tripduration"][
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


def test_numeric_types():
    validator = data_validation.DataValidation(CONFIG_NUMERIC_AGG_VALID, verbose=True)
    df = validator.execute()

    for validation in df.to_dict(orient="records"):
        assert float(validation["source_agg_value"]) == float(
            validation["target_agg_value"]
        )


def test_schema_validation():
    validator = data_validation.DataValidation(CONFIG_SCHEMA_VALIDATION, verbose=True)
    df = validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS


def test_cli_store_yaml_then_run_gcs():
    """Test storing and retrieving validation YAML when GCS env var is set."""
    # Store BQ Connection
    _store_bq_conn()

    # Build validation and store to file
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_STORE_COLUMN_ARGS)
    main.run(mock_args)

    # Look for YAML file in GCS env directory, since that has been set
    yaml_file_path = os.path.join(
        os.environ[consts.ENV_DIRECTORY_VAR], "validations/", CLI_CONFIG_FILE
    )

    # The number of lines is not significant, except that it represents
    # the exact file expected to be created.  Any change to this value
    # is likely to be a breaking change and must be assessed.
    mgr = state_manager.StateManager()
    validation_bytes = mgr._read_file(yaml_file_path)
    yaml_file_str = validation_bytes.decode("utf-8")
    assert len(yaml_file_str.splitlines()) == EXPECTED_NUM_YAML_LINES

    # Run generated config using 'run-config' command
    run_config_args = parser.parse_args(CLI_RUN_CONFIG_ARGS)
    config_file_path = main._get_arg_config_file(run_config_args)
    config_managers = main.build_config_managers_from_yaml(
        run_config_args, config_file_path
    )
    main.run_validations(run_config_args, config_managers)

    # Run generated config using 'configs run' command
    run_config_args = parser.parse_args(CLI_CONFIGS_RUN_ARGS)
    config_file_path = main._get_arg_config_file(run_config_args)
    config_managers = main.build_config_managers_from_yaml(
        run_config_args, config_file_path
    )
    main.run_validations(run_config_args, config_managers)

    # _remove_bq_conn()


def test_cli_store_yaml_then_run_local():
    """Test storing and retrieving validation YAML when GCS env var not set."""
    # Unset GCS env var so that YAML is saved locally
    gcs_path = os.environ[consts.ENV_DIRECTORY_VAR]
    os.environ[consts.ENV_DIRECTORY_VAR] = ""

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
        assert len(yaml_file.readlines()) == EXPECTED_NUM_YAML_LINES

    # Run generated config using 'run-config' command
    run_config_args = parser.parse_args(CLI_RUN_CONFIG_ARGS)
    config_file_path = main._get_arg_config_file(run_config_args)
    config_managers = main.build_config_managers_from_yaml(
        run_config_args, config_file_path
    )
    main.run_validations(run_config_args, config_managers)

    # Run generated config using 'configs run' command
    run_config_args = parser.parse_args(CLI_CONFIGS_RUN_ARGS)
    config_file_path = main._get_arg_config_file(run_config_args)
    config_managers = main.build_config_managers_from_yaml(
        run_config_args, config_file_path
    )
    main.run_validations(run_config_args, config_managers)

    os.remove(yaml_file_path)
    # _remove_bq_conn()

    # Re-set GCS env var
    os.environ[consts.ENV_DIRECTORY_VAR] = gcs_path


def test_cli_store_yaml_then_run_directory_gcs():
    """Test storing, retrieving, and executing two validation YAMLs in a provided directory path validation YAML when GCS env var is set."""
    # Store BQ Connection
    _store_bq_conn()

    # Build validation and store to file
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_STORE_COLUMN_ARGS_DIR)
    main.run(mock_args)
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_STORE_COLUMN_ARGS_DIR2)
    main.run(mock_args)

    # Run generated config using 'configs run --config-dir' command
    run_config_args = parser.parse_args(CLI_CONFIG_DIR_RUN_ARGS_LOCAL)
    main.config_runner(run_config_args)

    # Clean up
    mgr = state_manager.StateManager()
    yaml_file_path = (
        "integration_tests/validations/./tmp_test_dir_dvt/example_test_dir.yaml"
    )
    yaml_file_path2 = (
        "integration_tests/validations/./tmp_test_dir_dvt/example_test_dir2.yaml"
    )
    mgr.gcs_bucket.delete_blob(yaml_file_path)
    mgr.gcs_bucket.delete_blob(yaml_file_path2)


def test_cli_store_yaml_then_run_directory_local():
    """Test storing, retrieving, and executing two validation YAMLs in a provided directory path when GCS env var not set."""
    # Unset GCS env var so that YAML is saved locally
    gcs_path = os.environ[consts.ENV_DIRECTORY_VAR]
    os.environ[consts.ENV_DIRECTORY_VAR] = ""

    # Store BQ Connection
    _store_bq_conn()

    os.makedirs("./tmp_test_dir_dvt/", exist_ok=True)
    # Build validation and store to file
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_STORE_COLUMN_ARGS_DIR)
    main.run(mock_args)
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(CLI_STORE_COLUMN_ARGS_DIR2)
    main.run(mock_args)

    yaml_file_path = "./tmp_test_dir_dvt/example_test_dir.yaml"
    yaml_file_path2 = "./tmp_test_dir_dvt/example_test_dir2.yaml"

    # Run generated config using 'configs run --config-dir' command
    run_config_args = parser.parse_args(CLI_CONFIG_DIR_RUN_ARGS_LOCAL)
    main.config_runner(run_config_args)

    os.remove(yaml_file_path)
    os.remove(yaml_file_path2)
    os.removedirs("./tmp_test_dir_dvt/")

    # Re-set GCS env var
    os.environ[consts.ENV_DIRECTORY_VAR] = gcs_path


def test_wildcard_column_agg_yaml():
    """Test storing column validation YAML with string fields."""
    _test_cli_yaml_local_runner(
        CLI_WILDCARD_STRING_ARGS, EXPECTED_NUM_YAML_LINES_WILDCARD
    )


def test_timestamp_min_max_column_agg_yaml():
    """Test storing column validation YAML with timestamp fields for min, max aggregations."""
    _test_cli_yaml_local_runner(
        CLI_TIMESTAMP_MIN_MAX_ARGS, EXPECTED_NUM_YAML_LINES_TIMESTAMP_MIN_MAX
    )


def test_timestamp_sum_avg_bitxor_column_agg_yaml():
    """Test storing column validation YAML with timestamp fields for sum, avg, bit_xor aggregations."""
    _test_cli_yaml_local_runner(
        CLI_TIMESTAMP_SUM_AVG_BITXOR_ARGS,
        EXPECTED_NUM_YAML_LINES_TIMESTAMP_SUM_AVG_BITXOR,
    )


def test_bq_datetime_sum_avg_bitxor_column_agg_yaml():
    """Test storing column validation YAML with datetime fields for sum, avg, bit_xor aggregations, which are cast to timestamp when using the BigQuery client."""
    _test_cli_yaml_local_runner(
        CLI_BQ_DATETIME_SUM_AVG_BITXOR_ARGS,
        EXPECTED_NUM_YAML_LINES_BQ_DATETIME_SUM_AVG_BITXOR,
    )


def _test_cli_yaml_local_runner(cli_args, num_yaml_lines):
    """Test storing column validation YAML."""
    # Unset GCS env var so that YAML is saved locally
    gcs_path = os.environ[consts.ENV_DIRECTORY_VAR]
    os.environ[consts.ENV_DIRECTORY_VAR] = ""

    # Store BQ Connection
    _store_bq_conn()

    # Build validation and store to file
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(cli_args)
    main.run(mock_args)

    yaml_file_path = CLI_CONFIG_FILE
    with open(yaml_file_path, "r") as yaml_file:
        assert len(yaml_file.readlines()) == num_yaml_lines

    os.remove(yaml_file_path)
    # Re-set GCS env var
    os.environ[consts.ENV_DIRECTORY_VAR] = gcs_path


def test_timestamp_aggs():
    validator = data_validation.DataValidation(CONFIG_TIMESTAMP_AGGS)
    df = validator.execute()
    for validation in df.to_dict(orient="records"):
        assert validation["source_agg_value"] == validation["target_agg_value"]


def test_cli_find_tables():
    _store_bq_conn()

    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_FIND_TABLES_ARGS)
    tables_json = main.find_tables_using_string_matching(args)
    assert isinstance(tables_json, str)
    assert STRING_MATCH_RESULT in tables_json


def _store_bq_conn():
    parser = cli_tools.configure_arg_parser()
    mock_args = parser.parse_args(BQ_CONN_ARGS)
    main.run_connections(mock_args)


def test_random_row_query_builder():
    bq_client = clients.get_data_client(BQ_CONN)
    row_query_builder = random_row_builder.RandomRowBuilder(["station_id"], 10)
    query = row_query_builder.compile(
        bq_client,
        "bigquery-public-data.new_york_citibike",
        "citibike_stations",
        QueryBuilder([], [], [], [], [], None),
    )

    random_rows = bq_client.execute(query)

    assert query.compile() == EXPECTED_RANDOM_ROW_QUERY
    assert len(random_rows["station_id"]) == 10
    assert list(random_rows["station_id"]) != [
        4683,
        4676,
        4675,
        4674,
        4673,
        4671,
        4670,
        4666,
        4665,
        4664,
    ]


# add row validation tests for BQ


def test_bigquery_row():
    """Test row validaiton on bigquery"""
    config_row_valid = {
        consts.CONFIG_SOURCE_CONN: BQ_CONN,
        consts.CONFIG_TARGET_CONN: BQ_CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Row",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "pso-kokoro-resources.pso_data_validator",
        consts.CONFIG_TABLE_NAME: "test_data_types_bq_row",
        consts.CONFIG_COMPARISON_FIELDS: [
            {
                "source_column": "hash__all",
                "target_column": "hash__all",
                "field_alias": "hash__all",
                "cast": None,
            }
        ],
        consts.CONFIG_CALCULATED_FIELDS: [
            {
                "source_calculated_columns": ["string_col"],
                "target_calculated_columns": ["string_col"],
                "field_alias": "cast__string_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["integer_col"],
                "target_calculated_columns": ["integer_col"],
                "field_alias": "cast__integer_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["float_col"],
                "target_calculated_columns": ["float_col"],
                "field_alias": "cast__float_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["numeric_col"],
                "target_calculated_columns": ["numeric_col"],
                "field_alias": "cast__numeric_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["big_numeric_col"],
                "target_calculated_columns": ["big_numeric_col"],
                "field_alias": "cast__big_numeric_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["timestamp_col"],
                "target_calculated_columns": ["timestamp_col"],
                "field_alias": "cast__timestamp_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["date_col"],
                "target_calculated_columns": ["date_col"],
                "field_alias": "cast__date_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["cast__string_col"],
                "target_calculated_columns": ["cast__string_col"],
                "field_alias": "ifnull__cast__string_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__integer_col"],
                "target_calculated_columns": ["cast__integer_col"],
                "field_alias": "ifnull__cast__integer_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__float_col"],
                "target_calculated_columns": ["cast__float_col"],
                "field_alias": "ifnull__cast__float_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__numeric_col"],
                "target_calculated_columns": ["cast__numeric_col"],
                "field_alias": "ifnull__cast__numeric_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__big_numeric_col"],
                "target_calculated_columns": ["cast__big_numeric_col"],
                "field_alias": "ifnull__cast__big_numeric_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__timestamp_col"],
                "target_calculated_columns": ["cast__timestamp_col"],
                "field_alias": "ifnull__cast__timestamp_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__date_col"],
                "target_calculated_columns": ["cast__date_col"],
                "field_alias": "ifnull__cast__date_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["ifnull__cast__string_col"],
                "target_calculated_columns": ["ifnull__cast__string_col"],
                "field_alias": "rstrip__ifnull__cast__string_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__integer_col"],
                "target_calculated_columns": ["ifnull__cast__integer_col"],
                "field_alias": "rstrip__ifnull__cast__integer_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__float_col"],
                "target_calculated_columns": ["ifnull__cast__float_col"],
                "field_alias": "rstrip__ifnull__cast__float_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__numeric_col"],
                "target_calculated_columns": ["ifnull__cast__numeric_col"],
                "field_alias": "rstrip__ifnull__cast__numeric_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__big_numeric_col"],
                "target_calculated_columns": ["ifnull__cast__big_numeric_col"],
                "field_alias": "rstrip__ifnull__cast__big_numeric_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__timestamp_col"],
                "target_calculated_columns": ["ifnull__cast__timestamp_col"],
                "field_alias": "rstrip__ifnull__cast__timestamp_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__date_col"],
                "target_calculated_columns": ["ifnull__cast__date_col"],
                "field_alias": "rstrip__ifnull__cast__date_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__string_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__string_col"],
                "field_alias": "upper__rstrip__ifnull__cast__string_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__integer_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__integer_col"],
                "field_alias": "upper__rstrip__ifnull__cast__integer_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                "field_alias": "upper__rstrip__ifnull__cast__float_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__numeric_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__numeric_col"],
                "field_alias": "upper__rstrip__ifnull__cast__numeric_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__big_numeric_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__big_numeric_col"],
                "field_alias": "upper__rstrip__ifnull__cast__big_numeric_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__timestamp_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__timestamp_col"],
                "field_alias": "upper__rstrip__ifnull__cast__timestamp_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                "field_alias": "upper__rstrip__ifnull__cast__date_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": [
                    "upper__rstrip__ifnull__cast__string_col",
                    "upper__rstrip__ifnull__cast__integer_col",
                    "upper__rstrip__ifnull__cast__float_col",
                    "upper__rstrip__ifnull__cast__numeric_col",
                    "upper__rstrip__ifnull__cast__big_numeric_col",
                    "upper__rstrip__ifnull__cast__timestamp_col",
                    "upper__rstrip__ifnull__cast__date_col",
                ],
                "target_calculated_columns": [
                    "upper__rstrip__ifnull__cast__string_col",
                    "upper__rstrip__ifnull__cast__integer_col",
                    "upper__rstrip__ifnull__cast__float_col",
                    "upper__rstrip__ifnull__cast__numeric_col",
                    "upper__rstrip__ifnull__cast__big_numeric_col",
                    "upper__rstrip__ifnull__cast__timestamp_col",
                    "upper__rstrip__ifnull__cast__date_col",
                ],
                "field_alias": "concat__all",
                "type": "concat",
                "depth": 4,
            },
            {
                "source_calculated_columns": ["concat__all"],
                "target_calculated_columns": ["concat__all"],
                "field_alias": "hash__all",
                "type": "hash",
                "depth": 5,
            },
        ],
        consts.CONFIG_PRIMARY_KEYS: [
            {
                "source_column": "string_col",
                "target_column": "string_col",
                "field_alias": "string_col",
                "cast": None,
            }
        ],
        consts.CONFIG_FORMAT: "table",
    }

    data_validator = data_validation.DataValidation(
        config_row_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def test_custom_query():
    SAMPLE_CUSTOMQUERY_CONFIG = {
        "type": "Custom-query",
        "source_conn_name": BQ_CONN_NAME,
        "target_conn_name": BQ_CONN_NAME,
        "table_name": None,
        "schema_name": None,
        "target_schema_name": None,
        "target_table_name": None,
        "labels": [],
        "threshold": 0.0,
        "format": "table",
        "result_handler": None,
        "filters": [],
        "use_random_rows": False,
        "random_row_batch_size": None,
        "filter_status": None,
        "custom_query_type": "row",
        "source_query": "select object_id,faa_identifier from bigquery-public-data.faa.us_airports limit 100",
        "target_query": "select object_id,faa_identifier from bigquery-public-data.faa.us_airports limit 100",
        "comparison_fields": [
            {
                "source_column": "hash__all",
                "target_column": "hash__all",
                "field_alias": "hash__all",
                "cast": None,
            }
        ],
        "calculated_fields": [
            {
                "source_calculated_columns": ["object_id"],
                "target_calculated_columns": ["object_id"],
                "field_alias": "cast__object_id",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["faa_identifier"],
                "target_calculated_columns": ["faa_identifier"],
                "field_alias": "cast__faa_identifier",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["cast__object_id"],
                "target_calculated_columns": ["cast__object_id"],
                "field_alias": "ifnull__cast__object_id",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__faa_identifier"],
                "target_calculated_columns": ["cast__faa_identifier"],
                "field_alias": "ifnull__cast__faa_identifier",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["ifnull__cast__object_id"],
                "target_calculated_columns": ["ifnull__cast__object_id"],
                "field_alias": "rstrip__ifnull__cast__object_id",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__faa_identifier"],
                "target_calculated_columns": ["ifnull__cast__faa_identifier"],
                "field_alias": "rstrip__ifnull__cast__faa_identifier",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__object_id"],
                "target_calculated_columns": ["rstrip__ifnull__cast__object_id"],
                "field_alias": "upper__rstrip__ifnull__cast__object_id",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__faa_identifier"],
                "target_calculated_columns": ["rstrip__ifnull__cast__faa_identifier"],
                "field_alias": "upper__rstrip__ifnull__cast__faa_identifier",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": [
                    "upper__rstrip__ifnull__cast__object_id",
                    "upper__rstrip__ifnull__cast__faa_identifier",
                ],
                "target_calculated_columns": [
                    "upper__rstrip__ifnull__cast__object_id",
                    "upper__rstrip__ifnull__cast__faa_identifier",
                ],
                "field_alias": "concat__all",
                "type": "concat",
                "depth": 4,
            },
            {
                "source_calculated_columns": ["concat__all"],
                "target_calculated_columns": ["concat__all"],
                "field_alias": "hash__all",
                "type": "hash",
                "depth": 5,
            },
        ],
        "primary_keys": [
            {
                "source_column": "object_id",
                "target_column": "object_id",
                "field_alias": "object_id",
                "cast": None,
            }
        ],
    }

    client = data_validation.DataValidation(SAMPLE_CUSTOMQUERY_CONFIG)
    result_df = client.execute()
    assert result_df.source_agg_value.equals(result_df.target_agg_value)
