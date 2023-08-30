# Copyright 2023 Google LLC
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
from unittest import mock

from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts
from data_validation.partition_builder import PartitionBuilder
from tests.system.data_sources.common_functions import null_not_null_assertions
from tests.system.data_sources.test_bigquery import BQ_CONN


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "pso_data_validator/public")

CONN = {
    "source_type": "Snowflake",
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "password": SNOWFLAKE_PASSWORD,
    "database": SNOWFLAKE_DATABASE,
}

SNOWFLAKE_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "PSO_DATA_VALIDATOR.PUBLIC",
    consts.CONFIG_TABLE_NAME: "DVT_CORE_TYPES",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}


def test_count_validator():
    validator = data_validation.DataValidation(SNOWFLAKE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def mock_get_connection_config(*args):
    if args[1] in ("snowflake-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    "COURSE_ID < 'ALG001' OR COURSE_ID = 'ALG001' AND (QUARTER_ID < 3 OR QUARTER_ID = 3 AND (STUDENT_ID < 1234))",
    "(COURSE_ID > 'ALG001' OR COURSE_ID = 'ALG001' AND (QUARTER_ID > 3 OR QUARTER_ID = 3 AND (STUDENT_ID >= 1234)))"
    + " AND (COURSE_ID < 'GEO001' OR COURSE_ID = 'GEO001' AND (QUARTER_ID < 2 OR QUARTER_ID = 2 AND (STUDENT_ID < 5678)))",
    "(COURSE_ID > 'GEO001' OR COURSE_ID = 'GEO001' AND (QUARTER_ID > 2 OR QUARTER_ID = 2 AND (STUDENT_ID >= 5678)))"
    + " AND (COURSE_ID < 'TRI001' OR COURSE_ID = 'TRI001' AND (QUARTER_ID < 1 OR QUARTER_ID = 1 AND (STUDENT_ID < 9012)))",
    "COURSE_ID > 'TRI001' OR COURSE_ID = 'TRI001' AND (QUARTER_ID > 1 OR QUARTER_ID = 1 AND (STUDENT_ID >= 9012))",
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_snowflake_generate_table_partitions():
    """Test generate table partitions on Snowflake
    The unit tests, specifically test_add_partition_filters_to_config and test_store_yaml_partitions_local
    check that yaml configurations are created and saved in local storage. Partitions can only be created with
    a database that can handle SQL with ntile, hence doing this as part of system testing.
    What we are checking
    1. the shape of the partition list is 1, number of partitions (only one table in the list)
    2. value of the partition list matches what we expect.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "generate-table-partitions",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.TEST_GENERATE_PARTITIONS",
            "-pk=COURSE_ID,QUARTER_ID,STUDENT_ID",
            "-hash=*",
            "-cdir=/home/users/yaml",
            "-pn=4",
        ]
    )
    config_managers = main.build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()

    assert len(partition_filters) == 1  # only one pair of tables
    assert (
        len(partition_filters[0]) == partition_builder.args.partition_num
    )  # assume no of table rows > partition_num
    assert partition_filters[0] == EXPECTED_PARTITION_FILTER


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Snowflake to Snowflake dvt_core_types schema validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
            "--filter-status=fail",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_specific_types():
    """Snowflake to Snowflake test_specific_data_types schema validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.TEST_SPECIFIC_DATA_TYPES",
            "--filter-status=fail",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Snowflake to BigQuery dvt_core_types schema validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--exclusion-columns=id",
            (
                # Integral Snowflake NUMBERs to to BigQuery INT64.
                "--allow-list=decimal(38,0):int64,"
                # TODO When issue-706 is complete remove the timestamp line below
                "timestamp('UTC'):timestamp"
            ),
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_not_null_vs_nullable():
    """Compares a source table with a BigQuery target and ensure we match/fail on nnot null/nullable correctly."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PUBLIC.DVT_NULL_NOT_NULL=pso_data_validator.dvt_null_not_null",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    null_not_null_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    # TODO Change --sum/min/max to '*' when issue-916 is complete (support for col_tstz)
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
            "--filter-status=fail",
            "--sum=COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME",
            "--min=COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME",
            "--max=COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    # TODO Change --sum/min/max to '*' when issue-916 is complete (support for col_tstz)
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    # TODO Change --hash to '*' when issue-916 is complete (support for col_tstz)
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    # TODO Change --hash to '*' when issue-916 is complete (support for col_tstz)
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_validation_core_types():
    """Snowflake to Snowflake dvt_core_types custom-query validation"""
    parser = cli_tools.configure_arg_parser()
    # TODO Change to 'select *' when issue-916 is complete (support for col_tstz)
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "--source-query=select col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime from PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
            "--target-query=select col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime from PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
            "--filter-status=fail",
            "--count=*",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0
