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
            (
                # All SQL Server integrals go to BigQuery INT64.
                "--allow-list=int8:int64,int16:int64,int32:int64,"
                # SQL Server decimals that map to BigQuery NUMERIC.
                "decimal(20,0):decimal(38,9),decimal(10,2):decimal(38,9),"
                # SQL Server decimals that map to BigQuery BIGNUMERIC.
                "decimal(38,0):decimal(76,38),"
                # BigQuery does not have a float32 type.
                "float32:float64,"
                # Ignore ID column, we're not testing that one.
                "!int32:int64"
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
    # TODO Change --sum/min/max strings below to include col_tstz when issue-917 is complete.
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
            "--min=col_int8,col_int16,col_int32,col_int64,col_float64,col_date,col_datetime,col_dec_10_2,col_dec_20,col_dec_38,col_varchar_30,col_char_2,col_string",
            "--max=col_int8,col_int16,col_int32,col_int64,col_float64,col_date,col_datetime,col_dec_10_2,col_dec_20,col_dec_38,col_varchar_30,col_char_2,col_string",
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
    # TODO Change --hash string below to include col_tstz when issue-917 is complete.
    # TODO Change --hash string below to include col_float32,col_float64 when issue-841 is complete.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_varchar_30,col_char_2,col_date,col_datetime",
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
    """SQL Server to SQL Server dvt_core_types custom-query validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
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
