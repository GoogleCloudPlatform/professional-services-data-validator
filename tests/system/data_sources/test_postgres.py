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

from data_validation import (
    cli_tools,
    data_validation,
    consts,
)
from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    binary_key_assertions,
    find_tables_test,
    id_type_test_assertions,
    null_not_null_assertions,
    raw_query_test,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    partition_table_test,
    partition_query_test,
    row_validation_test,
    schema_validation_test,
    column_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN

# Local testing requires the Cloud SQL Proxy.
# https://cloud.google.com/sql/docs/postgres/connect-admin-proxy

# Cloud SQL proxy listens to localhost
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "guestbook")
PROJECT_ID = os.getenv("PROJECT_ID")

CONN = {
    "source_type": "Postgres",
    "host": POSTGRES_HOST,
    "user": "postgres",
    "password": POSTGRES_PASSWORD,
    "port": POSTGRES_PORT,
    "database": POSTGRES_DATABASE,
}

PG2PG_COLUMNS = [
    "id",
    "col_int2",
    "col_int4",
    "col_int8",
    "col_dec",
    "col_dec_10_2",
    "col_float32",
    "col_float64",
    "col_varchar_30",
    "col_char_2",
    "col_text",
    "col_date",
    "col_ts",
    "col_tstz",
    "col_time",
    "col_timetz",
    "col_binary",
    "col_bool",
    "col_uuid",
    "col_oid",
]


@pytest.fixture
def cloud_sql(request):
    if not request.config.getoption("--no-cloud-sql"):
        postgres_instance = CloudSQLResourceManager(
            PROJECT_ID,
            "POSTGRES_12",
            "data-validator-postgres12",
            POSTGRES_PASSWORD,
            database_id="guestbook",
            assign_public_ip=True,
            authorized_networks=None,
            cpu=1,
            memory="4GB",
            enable_bin_logs=False,
            already_exists=True,
        )

        # If instance already exists, returns host IP and does not add new data
        postgres_instance.setup()
        postgres_instance.add_data(
            "gs://pso-kokoro-resources/test/postgres/system_integration/postgres_data.sql"
        )
        postgres_instance.add_data(
            "gs://pso-kokoro-resources/test/postgres/system_integration/postgres_data_row.sql"
        )


def test_postgres_count(cloud_sql):
    """Test count validation on Postgres instance"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: "guestname",
                consts.CONFIG_TARGET_COLUMN: "guestname",
                consts.CONFIG_FIELD_ALIAS: "count_guestname",
            },
            {
                consts.CONFIG_TYPE: "sum",
                consts.CONFIG_SOURCE_COLUMN: "entryid",
                consts.CONFIG_TARGET_COLUMN: "entryid",
                consts.CONFIG_FIELD_ALIAS: "sum_entryid",
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

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]


def test_postgres_row(cloud_sql):
    """Test row validation on Postgres"""
    config_row_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Row",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "test_data_types_postgres_row",
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
                "source_calculated_columns": ["serial_col"],
                "target_calculated_columns": ["serial_col"],
                "field_alias": "cast__serial_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["int_col"],
                "target_calculated_columns": ["int_col"],
                "field_alias": "cast__int_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["text_col"],
                "target_calculated_columns": ["text_col"],
                "field_alias": "cast__text_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["char_col"],
                "target_calculated_columns": ["char_col"],
                "field_alias": "cast__char_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["varchar_col"],
                "target_calculated_columns": ["varchar_col"],
                "field_alias": "cast__varchar_col",
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
                "source_calculated_columns": ["cast__serial_col"],
                "target_calculated_columns": ["cast__serial_col"],
                "field_alias": "ifnull__cast__serial_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__int_col"],
                "target_calculated_columns": ["cast__int_col"],
                "field_alias": "ifnull__cast__int_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__text_col"],
                "target_calculated_columns": ["cast__text_col"],
                "field_alias": "ifnull__cast__text_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__char_col"],
                "target_calculated_columns": ["cast__char_col"],
                "field_alias": "ifnull__cast__char_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__varchar_col"],
                "target_calculated_columns": ["cast__varchar_col"],
                "field_alias": "ifnull__cast__varchar_col",
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
                "source_calculated_columns": ["ifnull__cast__serial_col"],
                "target_calculated_columns": ["ifnull__cast__serial_col"],
                "field_alias": "rstrip__ifnull__cast__serial_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__int_col"],
                "target_calculated_columns": ["ifnull__cast__int_col"],
                "field_alias": "rstrip__ifnull__cast__int_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__text_col"],
                "target_calculated_columns": ["ifnull__cast__text_col"],
                "field_alias": "rstrip__ifnull__cast__text_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__char_col"],
                "target_calculated_columns": ["ifnull__cast__char_col"],
                "field_alias": "rstrip__ifnull__cast__char_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__varchar_col"],
                "target_calculated_columns": ["ifnull__cast__varchar_col"],
                "field_alias": "rstrip__ifnull__cast__varchar_col",
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
                "source_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                "field_alias": "upper__rstrip__ifnull__cast__serial_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                "field_alias": "upper__rstrip__ifnull__cast__int_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                "field_alias": "upper__rstrip__ifnull__cast__text_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                "field_alias": "upper__rstrip__ifnull__cast__char_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                "field_alias": "upper__rstrip__ifnull__cast__varchar_col",
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
                    "upper__rstrip__ifnull__cast__serial_col",
                    "upper__rstrip__ifnull__cast__int_col",
                    "upper__rstrip__ifnull__cast__text_col",
                    "upper__rstrip__ifnull__cast__char_col",
                    "upper__rstrip__ifnull__cast__varchar_col",
                    "upper__rstrip__ifnull__cast__float_col",
                    "upper__rstrip__ifnull__cast__numeric_col",
                    "upper__rstrip__ifnull__cast__timestamp_col",
                    "upper__rstrip__ifnull__cast__date_col",
                ],
                "target_calculated_columns": [
                    "upper__rstrip__ifnull__cast__serial_col",
                    "upper__rstrip__ifnull__cast__int_col",
                    "upper__rstrip__ifnull__cast__text_col",
                    "upper__rstrip__ifnull__cast__char_col",
                    "upper__rstrip__ifnull__cast__varchar_col",
                    "upper__rstrip__ifnull__cast__float_col",
                    "upper__rstrip__ifnull__cast__numeric_col",
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
                "source_column": "serial_col",
                "target_column": "serial_col",
                "field_alias": "serial_col",
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


def mock_get_connection_config(*args):
    if args[1] in ("pg-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


# Expected result from partitioning table into 9 partitions on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        " quarter_id <> 1111 AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= TRUE ) ) AND ( course_id < 'ALG002' OR course_id = 'ALG002' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG002' OR course_id = 'ALG002' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= TRUE ) ) AND ( course_id < 'ALG003' OR course_id = 'ALG003' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG003' OR course_id = 'ALG003' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= FALSE ) ) AND ( course_id < 'ALG004' OR course_id = 'ALG004' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG004' OR course_id = 'ALG004' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= FALSE ) ) AND ( course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= TRUE ) ) AND ( course_id < 'St. John''s' OR course_id = 'St. John''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. John''s' OR course_id = 'St. John''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= TRUE ) ) AND ( course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= FALSE ) ) AND ( course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= FALSE ) )",
    ],
    [
        " quarter_id <> 1111 AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= TRUE ) ) AND ( course_id < 'ALG002' OR course_id = 'ALG002' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG002' OR course_id = 'ALG002' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= TRUE ) ) AND ( course_id < 'ALG003' OR course_id = 'ALG003' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG003' OR course_id = 'ALG003' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= FALSE ) ) AND ( course_id < 'ALG004' OR course_id = 'ALG004' AND ( quarter_id < 5678 OR quarter_id = 5678 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG004' OR course_id = 'ALG004' AND ( quarter_id > 5678 OR quarter_id = 5678 AND approved >= FALSE ) ) AND ( course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= TRUE ) ) AND ( course_id < 'St. John''s' OR course_id = 'St. John''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < TRUE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. John''s' OR course_id = 'St. John''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= TRUE ) ) AND ( course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= FALSE ) ) AND ( course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND ( quarter_id < 1234 OR quarter_id = 1234 AND approved < FALSE ) )",
        " quarter_id <> 1111 AND ( course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND ( quarter_id > 1234 OR quarter_id = 1234 AND approved >= FALSE ) )",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(cloud_sql, tmp_path: pathlib.Path):
    """Test generate partitions, first on table, then custom query on Postgres"""
    partition_table_test(
        EXPECTED_PARTITION_FILTER,
        tables="public.test_generate_partitions",
        pk="course_id,quarter_id,approved",
    )
    partition_query_test(
        EXPECTED_PARTITION_FILTER,
        tmp_path,
        tables="public.test_generate_partitions",
        pk="course_id,quarter_id,approved",
    )


def test_schema_validation(cloud_sql):
    """Test schema validation on Postgres instance"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        consts.CONFIG_TYPE: "Schema",
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_pg_types():
    """Test schema validation on most PostgreSQL scalar data types.
    This used to use the dvt_core_types table but that is covered by subsequent BigQuery
    testing therefore this test can cover off an extended list of data types.
    """
    schema_validation_test(tables="pso_data_validator.dvt_pg_types", tc="mock-conn")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # PostgreSQL integers go to BigQuery INT64.
            "int16:int64,int32:int64,"
            # BigQuery does not have a float32 type.
            "float32:float64"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_view_core_types_vw():
    """PostgreSQL to PostgreSQL view dvt_core_types_vw schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types_vw",
        tc="mock-conn",
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
            "-sc=pg-conn",
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
def test_schema_validation_bool():
    """PostgreSQL to PostgreSQL dvt_bool schema validation"""
    schema_validation_test(tables="pso_data_validator.dvt_bool", tc="mock-conn")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_pg_types():
    """Test column validation on most PostgreSQL scalar data types.
    Actual values are not well tested because this is PostgreSQL to PostgreSQL, this is shaking
    out the code path for all types. PostgreSQL to BigQuery and Oracle to PostgreSQL tests
    will cover testing of returned data values.
    This used to use the dvt_core_types table but that is covered by subsequent BigQuery
    testing therefore this test can cover off an extended list of data types.
    """
    count_cols = ",".join(PG2PG_COLUMNS)
    sum_cols = ",".join([_ for _ in PG2PG_COLUMNS if _ not in ("col_oid")])
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_pg_types",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
        count_cols=count_cols,
        sum_cols=sum_cols,
        min_cols=count_cols,
        max_cols=count_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # TODO Change --sum and --max options to include col_char_2 when issue-842 is complete.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_char_2")
        ]
    )
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
def test_column_validation_view_core_types_vw():
    """PostgreSQL to PostgreSQL view dvt_core_types_vw column validation"""
    # TODO Change --sum and --max options to include col_char_2 when issue-842 is complete.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_char_2")]
    )
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types_vw",
        count_cols=cols,
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pg_types():
    """Test row hash validation on most PostgreSQL scalar data types.
    Actual values are not well tested because this is PostgreSQL to PostgreSQL, this is shaking
    out the code path for all types. PostgreSQL to BigQuery and Oracle to PostgreSQL tests
    will cover testing of returned data values.
    This used to use the dvt_core_types table but that is covered by subsequent BigQuery
    testing therefore this test can cover off an extended list of data types.
    """
    row_validation_test(
        tables="pso_data_validator.dvt_pg_types",
        tc="mock-conn",
        hash="*",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from PostgreSQL defined constraint."""
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        hash="col_int8,col_int16",
        primary_keys=None,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_pg_types():
    """PostgreSQL to PostgreSQL dvt_pg_types row validation --comp-fields"""
    row_validation_test(
        tables="pso_data_validator.dvt_pg_types",
        tc="mock-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """PostgreSQL to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    """
    row_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        hash="id,col_data,col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """PostgreSQL to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=pg-conn",
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
def test_row_validation_char_pk_to_bigquery():
    """PostgreSQL to BigQuery dvt_char_id row validation.
    This is testing CHAR primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=pg-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_char_id",
            "--primary-keys=id",
            "--hash=id,other_data",
            "--use-random-row",
            "--random-row-batch-size=5",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pangrams_to_bigquery():
    """PostgreSQL to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=pg-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_pangrams",
            "--primary-keys=id",
            "--hash=*",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_validation_core_types():
    """PostgreSQL to PostgreSQL dvt_core_types custom-query validation"""
    custom_query_validation_test(tc="mock-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """PostgreSQL to BigQuery test of find-tables command."""
    find_tables_test()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_views_and_tables():
    """PostgreSQL to BigQuery test of find-tables command."""
    find_tables_test(include_views=True)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """PostgreSQL dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(expected_config_managers=5)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """PostgreSQL dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(
        validation_type="custom-query", expected_config_managers=5
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
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys)
