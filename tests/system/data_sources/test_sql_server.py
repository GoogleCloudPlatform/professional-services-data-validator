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
from unittest import mock

import pytest

from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts
from data_validation.partition_builder import PartitionBuilder
from tests.system.data_sources.common_functions import null_not_null_assertions
from tests.system.data_sources.test_bigquery import BQ_CONN


# Local testing requires the Cloud SQL Proxy.
# https://cloud.google.com/sql/docs/sqlserver/connect-admin-proxy

# Cloud SQL Proxy listens on localhost
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "127.0.0.1")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "sqlserver")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")
CONN = {
    "source_type": "MSSQL",
    "host": SQL_SERVER_HOST,
    "user": SQL_SERVER_USER,
    "password": SQL_SERVER_PASSWORD,
    "port": 1433,
    "database": "guestbook",
}


@pytest.fixture
def cloud_sql(request):
    if not request.config.getoption("--no-cloud-sql"):
        mssql_instance = CloudSQLResourceManager(
            PROJECT_ID,
            "SQLSERVER_2017_STANDARD",
            "data-validator-mssql2017",
            SQL_SERVER_PASSWORD,
            database_id="guestbook",
            assign_public_ip=True,
            authorized_networks=None,
            cpu=1,
            memory="4GB",
            enable_bin_logs=False,
            already_exists=True,
        )

        # If instance already exists, returns host IP and does not add new data
        mssql_instance.setup()
        mssql_instance.add_data("gs://pso-kokoro-resources/mssql_data.sql")


def test_sql_server_count(cloud_sql):
    """Test count validation on SQL Server instance"""
    config_count_valid = {
        # BigQuery Specific Connection Config
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "dbo",
        consts.CONFIG_TABLE_NAME: "entries",
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

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def test_sql_server_row(cloud_sql):
    """Test row validation on SQL Server instance"""
    config_row_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Row",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "dbo",
        consts.CONFIG_TABLE_NAME: "entries",
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
                "source_calculated_columns": ["content"],
                "target_calculated_columns": ["content"],
                "field_alias": "cast__content",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["cast__content"],
                "target_calculated_columns": ["cast__content"],
                "field_alias": "ifnull__cast__content",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["ifnull__cast__content"],
                "target_calculated_columns": ["ifnull__cast__content"],
                "field_alias": "rstrip__ifnull__cast__content",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__content"],
                "target_calculated_columns": ["rstrip__ifnull__cast__content"],
                "field_alias": "upper__rstrip__ifnull__cast__content",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["upper__rstrip__ifnull__cast__content"],
                "target_calculated_columns": ["upper__rstrip__ifnull__cast__content"],
                "field_alias": "hash__all",
                "type": "hash",
                "depth": 4,
            },
        ],
        consts.CONFIG_PRIMARY_KEYS: [
            {
                "source_column": "entryID",
                "target_column": "entryID",
                "field_alias": "entryID",
                "cast": None,
            }
        ],
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
        consts.CONFIG_RANDOM_ROW_BATCH_SIZE: "5",
        consts.CONFIG_USE_RANDOM_ROWS: True,
    }

    data_validator = data_validation.DataValidation(
        config_row_valid,
        verbose=False,
    )
    df = data_validator.execute()
    assert df["source_agg_value"][0] == df["target_agg_value"][0]
    assert df.shape[0] == 5


def test_schema_validation():
    config = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        consts.CONFIG_TYPE: "Schema",
        consts.CONFIG_SCHEMA_NAME: "dbo",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
    }

    validator = data_validation.DataValidation(config, verbose=True)
    df = validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS


def mock_get_connection_config(*args):
    if args[1] in ("sql-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    " course_id < N'ALG001' OR course_id = N'ALG001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 )",
    " ( course_id > N'ALG001' OR course_id = N'ALG001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) )"
    + " AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 5678 ) )",
    " ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 5678 ) )"
    + " AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 9012 ) )",
    " course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 9012 )",
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_sqlserver_generate_table_partitions(cloud_sql):
    """Test generate table partitions on sqlserver
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
            "-tbls=dbo.test_generate_partitions=dbo.test_generate_partitions",
            "-pk=course_id,quarter_id,student_id",
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
        len(partition_filters[0][0]) == partition_builder.args.partition_num
    )  # assume no of table rows > partition_num
    assert partition_filters[0][0] == EXPECTED_PARTITION_FILTER


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
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
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--exclusion-columns=id",
            (
                # All SQL Server integers go to BigQuery INT64.
                "--allow-list=int8:int64,int16:int64,int32:int64,"
                # BigQuery does not have a float32 type.
                "float32:float64"
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
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_null_not_null=pso_data_validator.dvt_null_not_null",
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
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--grouped-columns=col_varchar_30",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_date,col_datetime,col_tstz,col_varchar_30,col_char_2,col_string",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_date,col_datetime,col_tstz,col_varchar_30,col_char_2,col_string",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_date,col_datetime,col_tstz,col_varchar_30,col_char_2,col_string",
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
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # We've excluded col_char_2 since the data stored in MSSQL has a trailing space which is counted in the LEN()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_float64,col_date,col_datetime,col_dec_10_2,col_dec_20,col_dec_38,col_varchar_30,col_char_2,col_string",
            "--min=col_int8,col_int16,col_int32,col_int64,col_float64,col_date,col_datetime,col_tstz,col_dec_10_2,col_dec_20,col_dec_38,col_varchar_30,col_char_2,col_string",
            "--max=col_int8,col_int16,col_int32,col_int64,col_float64,col_date,col_datetime,col_tstz,col_dec_10_2,col_dec_20,col_dec_38,col_varchar_30,col_char_2,col_string",
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
    # TODO When issue-834 is complete add col_string to --hash string below.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz,col_dec_20,col_dec_38",
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
    # TODO When issue-834 is complete add col_string to --hash string below.
    # TODO Change --hash string below to include col_tstz when issue-929 is complete.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime",
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
def test_row_validation_large_decimals_to_bigquery():
    """SQL Server to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    """
    parser = cli_tools.configure_arg_parser()
    # TODO When issue-1079 is complete add col_dec_38_30 to --hash string below.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_large_decimals",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=id,col_data,col_dec_18,col_dec_38,col_dec_38_9",
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
def test_custom_query_column_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types custom-query column validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "column",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "--source-query=select * from pso_data_validator.dvt_core_types",
            "--target-query=select * from pso_data_validator.dvt_core_types",
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


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types custom-query row comparison-fields validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "row",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "--source-query=select id,col_int64,COL_VARCHAR_30,col_date from pso_data_validator.dvt_core_types",
            "--target-query=select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--comparison-fields=col_int64,col_varchar_30,col_date",
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
def test_custom_query_row_hash_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types custom-query row hash validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "row",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "--source-query=select id,col_int64,COL_VARCHAR_30,col_date from pso_data_validator.dvt_core_types",
            "--target-query=select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int64,col_varchar_30,col_date",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0
