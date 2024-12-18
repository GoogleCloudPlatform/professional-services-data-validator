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
import pathlib

from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from data_validation import cli_tools, data_validation, consts, find_tables
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    find_tables_assertions,
    id_type_test_assertions,
    null_not_null_assertions,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    partition_table_test,
    partition_query_test,
    schema_validation_test,
    column_validation_test,
    row_validation_test,
    custom_query_validation_test,
)
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


# Expected result from partitioning table on 3 keys, 9 partitions
EXPECTED_PARTITION_FILTER = [
    [
        " quarter_id <> 1111 AND ( course_id < N'ALG001' OR course_id = N'ALG001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'ALG001' OR course_id = N'ALG001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < N'ALG001' OR course_id = N'ALG001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'ALG001' OR course_id = N'ALG001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) )",
    ],
    [
        " quarter_id <> 1111 AND ( course_id < N'ALG001' OR course_id = N'ALG001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'ALG001' OR course_id = N'ALG001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < N'ALG001' OR course_id = N'ALG001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'ALG001' OR course_id = N'ALG001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < N'GEO001' OR course_id = N'GEO001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'GEO001' OR course_id = N'GEO001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < N'TRI001' OR course_id = N'TRI001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > N'TRI001' OR course_id = N'TRI001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) )",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(cloud_sql, tmp_path: pathlib.Path):
    """Test generate partitions on SQL Server, first on table, then on custom query"""
    partition_table_test(
        EXPECTED_PARTITION_FILTER, tables="dbo.test_generate_partitions"
    )
    partition_query_test(
        EXPECTED_PARTITION_FILTER, tmp_path, tables="dbo.test_generate_partitions"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """SQL Server to SQL Server dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # All SQL Server integers go to BigQuery INT64.
            "int8:int64,int16:int64,int32:int64,"
            # BigQuery does not have a float32 type.
            "float32:float64"
        ),
    )


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
    df = run_test_from_cli_args(args)
    null_not_null_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """SQL Server to SQL Server dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have a float32 type.
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz"
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """SQL Server to SQL Server dvt_core_types row validation"""
    row_validation_test(
        tc="mock-conn",
        # TODO When issue-834 is complete add col_string to --hash string below.
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz,col_dec_20,col_dec_38",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from SQL Server defined constraint."""
    row_validation_test(
        tc="mock-conn",
        hash="col_int8,col_int16",
        primary_keys=None,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types row validation"""
    row_validation_test(
        tc="bq-conn",
        # TODO When issue-834 is complete add col_string to --hash string below.
        # TODO When issue-1111 is complete add col_dec_10_2 to --hash string below.
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """SQL Server to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    """
    row_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        # TODO When issue-1079 is complete add col_dec_38_30 to --hash string below.
        hash="id,col_data,col_dec_18,col_dec_38,col_dec_38_9",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """SQL Server to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=sql-conn",
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
def test_row_validation_pangrams_to_bigquery():
    """SQL Server to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.

    This needs more investigation on how to handle unicode characters in SQL Server.
    """
    pytest.skip(
        "Skipping test_row_validation_pangrams_to_bigquery because failing on SQL Server."
    )
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=sql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_pangrams",
            "--primary-keys=id",
            "--concat=*",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types custom-query column validation"""
    custom_query_validation_test(tc="bq-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types custom-query row comparison-fields validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,COL_VARCHAR_30,col_date from pso_data_validator.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        comp_fields="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_hash_validation_core_types_to_bigquery():
    """SQL Server to BigQuery dvt_core_types custom-query row hash validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,COL_VARCHAR_30,col_date from pso_data_validator.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        hash="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """SQL Server to BigQuery test of find-tables command."""
    pytest.skip("Skipping test_find_tables until issue 1198 has been resolved.")
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "find-tables",
            "-sc=mock-conn",
            "-tc=bq-conn",
            "--allowed-schemas=pso_data_validator",
        ]
    )
    output = find_tables.find_tables_using_string_matching(args)
    find_tables_assertions(output)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """SQL Server dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(expected_config_managers=2)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """SQL Server dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(
        validation_type="custom-query", expected_config_managers=2
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_identifiers():
    """Test schema validation on a table with special characters in table and column names."""
    schema_validation_test(
        tables="pso_data_validator.dvt-identifier$_#",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_identifiers():
    """Test column validation on a table with special characters in table and column names."""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt-identifier$_#",
        count_cols="*",
        filters="'col#hash' IS NOT NULL",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_identifiers():
    """Test row validation on a table with special characters in table and column names."""
    row_validation_test(
        tables="pso_data_validator.dvt-identifier$_#",
        tc="mock-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_uuid_to_bigquery():
    """Test column validation with UUID column and primary key to BigQuery"""
    column_validation_test(
        tables="pso_data_validator.dvt_uuid_id",
        count_cols="*",
        sum_cols="*",
        min_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_uuid_hash_to_bigquery():
    """Test row validation with UUID column and primary key to BigQuery"""
    row_validation_test(
        tables="pso_data_validator.dvt_uuid_id",
        hash="*",
    )
