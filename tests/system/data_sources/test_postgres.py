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

import logging
import os
from unittest import mock

import pytest
import pathlib

from data_validation import (
    cli_tools,
    clients,
    consts,
    data_validation,
)
from data_validation.result_handlers.base_backend import RH_WRITE_MESSAGE
from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    DVT_TRICKY_DATES_COLUMNS,
    binary_key_assertions,
    column_validation_test,
    column_validation_test_args,
    custom_query_validation_test,
    find_tables_test,
    id_column_row_validation_test,
    id_column_query_row_validation_test,
    id_type_test_assertions,
    null_not_null_assertions,
    partition_table_test,
    partition_query_test,
    raw_query_rows,
    raw_query_test,
    row_validation_test,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    schema_validation_test,
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
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_POSTGRES,
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

SUM_EPOCH_COL_DATETIME = "3093527978590011259"

DVT_CORE_TYPES_RAW_DATA_TYPES = [
    ("id", "integer", None, 4, None, None, None),
    ("col_int8", "smallint", None, 2, None, None, None),
    ("col_int16", "smallint", None, 2, None, None, None),
    ("col_int32", "integer", None, 4, None, None, None),
    ("col_int64", "bigint", None, 8, None, None, None),
    ("col_dec_20", "numeric", None, 20, 20, 0, None),
    ("col_dec_38", "numeric", None, 38, 38, 0, None),
    ("col_dec_10_2", "numeric", None, 10, 10, 2, None),
    ("col_float32", "real", None, 4, None, None, None),
    ("col_float64", "double precision", None, 8, None, None, None),
    ("col_varchar_30", "character varying", None, 30, None, None, None),
    ("col_char_2", "character", None, 2, None, None, None),
    ("col_string", "text", None, -1, None, None, None),
    ("col_date", "date", None, 4, None, None, None),
    ("col_datetime", "timestamp without time zone", None, 8, None, None, None),
    ("col_tstz", "timestamp with time zone", None, 8, None, None, None),
]

EXPECTED_DATETIME_ID_PARTITION_FILTER = [
    [
        " ( NOT other_data IS NULL ) AND ( \"id\" < '2020-03-01T12:00:00' )",
        " ( NOT other_data IS NULL ) AND ( \"id\" >= '2020-03-01T12:00:00' )",
    ],
    [
        " ( NOT other_data IS NULL ) AND ( \"id\" < '2020-03-01T12:00:00' )",
        " ( NOT other_data IS NULL ) AND ( \"id\" >= '2020-03-01T12:00:00' )",
    ],
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
        consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df[consts.TARGET_AGG_VALUE])
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
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["int_col"],
                "target_calculated_columns": ["int_col"],
                "field_alias": "cast__int_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["text_col"],
                "target_calculated_columns": ["text_col"],
                "field_alias": "cast__text_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["char_col"],
                "target_calculated_columns": ["char_col"],
                "field_alias": "cast__char_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["varchar_col"],
                "target_calculated_columns": ["varchar_col"],
                "field_alias": "cast__varchar_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["float_col"],
                "target_calculated_columns": ["float_col"],
                "field_alias": "cast__float_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["numeric_col"],
                "target_calculated_columns": ["numeric_col"],
                "field_alias": "cast__numeric_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["timestamp_col"],
                "target_calculated_columns": ["timestamp_col"],
                "field_alias": "cast__timestamp_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["date_col"],
                "target_calculated_columns": ["date_col"],
                "field_alias": "cast__date_col",
                "type": consts.CALC_FIELD_CAST,
                "depth": 0,
            },
            {
                "source_calculated_columns": ["cast__serial_col"],
                "target_calculated_columns": ["cast__serial_col"],
                "field_alias": "ifnull__cast__serial_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__int_col"],
                "target_calculated_columns": ["cast__int_col"],
                "field_alias": "ifnull__cast__int_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__text_col"],
                "target_calculated_columns": ["cast__text_col"],
                "field_alias": "ifnull__cast__text_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__char_col"],
                "target_calculated_columns": ["cast__char_col"],
                "field_alias": "ifnull__cast__char_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__varchar_col"],
                "target_calculated_columns": ["cast__varchar_col"],
                "field_alias": "ifnull__cast__varchar_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__float_col"],
                "target_calculated_columns": ["cast__float_col"],
                "field_alias": "ifnull__cast__float_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__numeric_col"],
                "target_calculated_columns": ["cast__numeric_col"],
                "field_alias": "ifnull__cast__numeric_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__timestamp_col"],
                "target_calculated_columns": ["cast__timestamp_col"],
                "field_alias": "ifnull__cast__timestamp_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__date_col"],
                "target_calculated_columns": ["cast__date_col"],
                "field_alias": "ifnull__cast__date_col",
                "type": consts.CALC_FIELD_IFNULL,
                "depth": 1,
            },
            {
                "source_calculated_columns": ["ifnull__cast__serial_col"],
                "target_calculated_columns": ["ifnull__cast__serial_col"],
                "field_alias": "rstrip__ifnull__cast__serial_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__int_col"],
                "target_calculated_columns": ["ifnull__cast__int_col"],
                "field_alias": "rstrip__ifnull__cast__int_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__text_col"],
                "target_calculated_columns": ["ifnull__cast__text_col"],
                "field_alias": "rstrip__ifnull__cast__text_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__char_col"],
                "target_calculated_columns": ["ifnull__cast__char_col"],
                "field_alias": "rstrip__ifnull__cast__char_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__varchar_col"],
                "target_calculated_columns": ["ifnull__cast__varchar_col"],
                "field_alias": "rstrip__ifnull__cast__varchar_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__float_col"],
                "target_calculated_columns": ["ifnull__cast__float_col"],
                "field_alias": "rstrip__ifnull__cast__float_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__numeric_col"],
                "target_calculated_columns": ["ifnull__cast__numeric_col"],
                "field_alias": "rstrip__ifnull__cast__numeric_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__timestamp_col"],
                "target_calculated_columns": ["ifnull__cast__timestamp_col"],
                "field_alias": "rstrip__ifnull__cast__timestamp_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__date_col"],
                "target_calculated_columns": ["ifnull__cast__date_col"],
                "field_alias": "rstrip__ifnull__cast__date_col",
                "type": consts.CALC_FIELD_RSTRIP,
                "depth": 2,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                "field_alias": "upper__rstrip__ifnull__cast__serial_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                "field_alias": "upper__rstrip__ifnull__cast__int_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                "field_alias": "upper__rstrip__ifnull__cast__text_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                "field_alias": "upper__rstrip__ifnull__cast__char_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                "field_alias": "upper__rstrip__ifnull__cast__varchar_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                "field_alias": "upper__rstrip__ifnull__cast__float_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__numeric_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__numeric_col"],
                "field_alias": "upper__rstrip__ifnull__cast__numeric_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__timestamp_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__timestamp_col"],
                "field_alias": "upper__rstrip__ifnull__cast__timestamp_col",
                "type": consts.CALC_FIELD_UPPER,
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                "field_alias": "upper__rstrip__ifnull__cast__date_col",
                "type": consts.CALC_FIELD_UPPER,
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
                "type": consts.CALC_FIELD_CONCAT,
                "depth": 4,
            },
            {
                "source_calculated_columns": ["concat__all"],
                "target_calculated_columns": ["concat__all"],
                "field_alias": "hash__all",
                "type": consts.CALC_FIELD_HASH,
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
        consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
        consts.CONFIG_FILTER_STATUS: None,
        consts.CONFIG_RANDOM_ROW_BATCH_SIZE: "5",
        consts.CONFIG_USE_RANDOM_ROWS: True,
    }

    data_validator = data_validation.DataValidation(
        config_row_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"][0] == df[consts.TARGET_AGG_VALUE][0]


def mock_get_connection_config(*args):
    if args[1] in ("pg-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


# Expected result from partitioning table into 9 partitions on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false)))) AND (course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false))))",
    ],
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false)))) AND (course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false))))",
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
    )
    partition_query_test(
        EXPECTED_PARTITION_FILTER,
        tmp_path,
    )


def test_schema_validation(cloud_sql):
    """Test schema validation on Postgres instance"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        consts.CONFIG_TYPE: "Schema",
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation[consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_SUCCESS


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
        avg_cols=sum_cols,
        std_cols=sum_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_float32")]
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery():
    """PostgreSQL to BigQuery dvt_large_decimals column validation."""
    # TODO Add col_dec_38 to cols when issue-1360 has been resolved.
    cols = "col_dec_18,col_dec_38_9,col_dec_38_30"
    column_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        count_cols=cols,
        min_cols=cols,
        sum_cols=cols,
        avg_cols=cols,
        std_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery_mismatch():
    """PostgreSQL to BigQuery dvt_large_decimals column validation on columns we expect to have a mismatch.

    Regression test for:
      https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1007
    """
    cols = "col_dec_18_fail,col_dec_18_1_fail"
    df = column_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        count_cols=cols,
        sum_cols=cols,
        expected_rows=2,
    )
    assert "sum__col_dec_18_fail" in df[consts.VALIDATION_NAME].values
    assert "sum__col_dec_18_1_fail" in df[consts.VALIDATION_NAME].values


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_view_core_types_vw():
    """PostgreSQL to PostgreSQL view dvt_core_types_vw column validation"""
    cols = ",".join([_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id",)])
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
def test_row_validation_comp_fields_binary_values_to_bigquery():
    """dvt_binary row validation with comparison fields."""
    row_validation_test(
        tables="pso_data_validator.dvt_binary",
        tc="bq-conn",
        primary_keys="int_id",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_fixed_char_pk_row_validation_to_bigquery():
    """Test fixed char primary keys"""
    id_column_row_validation_test("pso_data_validator.dvt_fixed_char_id")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_varchar_pk_row_validation_to_bigquery():
    """Test varchar primary keys"""
    id_column_row_validation_test("pso_data_validator.dvt_varchar_id")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_fixed_char_pk_query_row_validation_to_bigquery():
    """Test fixed char primary keys on custom query"""
    id_column_query_row_validation_test("pso_data_validator.dvt_fixed_char_id")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_varchar_pk_query_row_validation_to_bigquery():
    """Test varchar primary keys on custom query"""
    id_column_query_row_validation_test("pso_data_validator.dvt_varchar_id")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_datetime_pk_to_bigquery():
    """Test datetime primary key join columns"""
    # TODO Remove use_randow_row option below when issue-1445 is actioned.
    id_column_row_validation_test(
        "pso_data_validator.dvt_datetime_id",
        use_randow_row=False,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions_datetime_pk():
    """Test generate partitions on datetime primary key"""
    pytest.skip("Skipping test_generate_partitions_datetime_pk due to issue-1443.")
    partition_table_test(
        EXPECTED_DATETIME_ID_PARTITION_FILTER,
        pk="id",
        tables="pso_data_validator.dvt_datetime_id",
        filters="other_data IS NOT NULL",
        partition_num=2,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_pangrams_to_bigquery():
    """PostgreSQL to BigQuery dvt_pangrams column validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_pangrams",
        sum_cols="words",
        min_cols="words",
        max_cols="words",
    )


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
def test_column_validation_many_columns():
    """dvt_many_cols column validation."""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_many_cols",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns_standard():
    """dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(expected_config_managers=5)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns_exclude():
    """dvt_many_cols row validation including --exclude-columns.

    We have this specific test for --exclude-columns as a regression test for issue-1542.
    The code being tested is not PostgreSQL specific therefore we do not need this in other test files.
    """
    row_validation_many_columns_test(
        expected_config_managers=5,
        columns="col_004,col_005,col_006",
        exclude_columns=True,
    )


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
def test_column_validation_group_by_timestamp():
    """Test that --grouped-columns on Timestamps works correctly"""
    args = column_validation_test_args(
        tables="pso_data_validator.dvt_group_by_timestamp",
        grouped_columns="col_datetime",
        filter_status=None,
    )
    df = run_test_from_cli_args(args)
    # We expect 3 groups in the data set even though there are 6 records, due to Timestamp to Date cast.
    assert len(df) == 3
    # All groups should be a successful validation.
    assert all(
        _ == "success" for _ in df[consts.VALIDATION_STATUS]
    ), "Not all records are marked as success"


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_high_epoch_seconds():
    """Test column validation on a table with an extreme result from sum(epoch seconds)."""
    df = column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_high_epoch_seconds=pso_data_validator.dvt_high_epoch_seconds2",
        sum_cols="col_datetime,col_datetime_fail",
        filter_status=None,
        # We expect two rows:
        #   success for simple count
        #   success for col_datetime
        #   failure for col_datetime_fail (which has an intentional data error)
        expected_rows=3,
    )
    status_dict = dict(zip(df[consts.VALIDATION_NAME], df[consts.VALIDATION_STATUS]))
    value_dict = dict(zip(df[consts.VALIDATION_NAME], df["source_agg_value"]))
    assert (
        status_dict["sum__epoch_seconds__col_datetime"]
        == consts.VALIDATION_STATUS_SUCCESS
    ), 'sum__epoch_seconds__col_datetime should have status "success"'
    assert (
        status_dict["sum__epoch_seconds__col_datetime_fail"]
        == consts.VALIDATION_STATUS_FAIL
    ), 'sum__epoch_seconds__col_datetime_fail should have status "fail"'
    assert (
        str(value_dict["sum__epoch_seconds__col_datetime"]) == SUM_EPOCH_COL_DATETIME
    ), f"sum__epoch_seconds__col_datetime != SUM_EPOCH_COL_DATETIME ({SUM_EPOCH_COL_DATETIME})"


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    cols = ",".join(DVT_TRICKY_DATES_COLUMNS)
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_tricky_dates",
        min_cols=cols,
        max_cols=cols,
        sum_cols=cols,
        grouped_columns="id",
        wildcard_include_timestamp=True,
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
def test_schema_validation_reserved_words():
    """Test schema validation on a table with reserved words in column names."""
    schema_validation_test(
        tables="pso_data_validator.dvt_reserved_word_columns",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_reserved_words():
    """Test column validation on a table with reserved words in column names."""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_reserved_word_columns",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_reserved_words():
    """Test row validation on a table with reserved words in column names."""
    row_validation_test(
        tables="pso_data_validator.dvt_reserved_word_columns",
        tc="mock-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_reserved_words():
    """Test row validation on a table with reserved words in column names."""
    row_validation_test(
        tables="pso_data_validator.dvt_reserved_word_columns",
        tc="mock-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    cols = ",".join(DVT_TRICKY_DATES_COLUMNS)
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        hash=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    cols = ",".join(DVT_TRICKY_DATES_COLUMNS)
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        comp_fields=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_strings_to_bigquery():
    """Test with string values containing special characters."""
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_strings",
        tc="bq-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_decimals_no_precision():
    """Test column validation on a table with decimals columns with no defined precision."""
    column_validation_test(
        tables="pso_data_validator.dvt_decimals_no_precision",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
        avg_cols="*",
        std_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_decimals_no_precision():
    """Test row validation on a table with decimals columns with no defined precision."""
    row_validation_test(
        tables="pso_data_validator.dvt_decimals_no_precision",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_intervals():
    """Test schema validation on a table with columns of type INTERVAL."""
    schema_validation_test(
        tables="pso_data_validator.dvt_intervals",
        tc="bq-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_intervals():
    """Test column validation on a table with columns of type INTERVAL."""
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_intervals",
        count_cols="col_interval_ds,col_interval_ym",
        sum_cols="col_interval_ds,col_interval_ym",
        min_cols="col_interval_ds,col_interval_ym",
        max_cols="col_interval_ds,col_interval_ym",
        wildcard_include_timestamp=True,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_intervals():
    """Test row validation on a table with columns of type INTERVAL."""
    pytest.skip("Skipping test_row_validation_intervals due to issue-1214.")
    row_validation_test(
        tables="pso_data_validator.dvt_intervals",
        tc="bq-conn",
        hash="col_interval_ds,col_interval_ym",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
@mock.patch(
    "data_validation.state_manager.StateManager.list_connections",
    return_value="mock-conn",
)
def test_result_handler_postgres(mock_list, caplog):
    """Test result handler using dvt_core_types schema validation."""
    table_id = "pso_data_validator_results.results_data"
    caplog.set_level(logging.INFO)
    df = schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        filter_status=None,
        result_handler=f"mock-conn.{table_id}",
    )
    assert any(_ for _ in caplog.records if RH_WRITE_MESSAGE in _.msg)
    run_id = df[consts.CONFIG_RUN_ID][0]

    # Hijacking DVT raw query to query the results the table.
    # Labels columns should be empty.
    rows = raw_query_rows(
        f"SELECT COUNT(*) FROM {table_id} WHERE run_id = '{run_id}' AND labels = '{{}}'",
        conn="mock-conn",
    )
    # Ensure that we added the data to the results table.
    assert len(df) == rows[0][0]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
@mock.patch(
    "data_validation.state_manager.StateManager.list_connections",
    return_value="mock-conn",
)
def test_result_handler_postgres_with_labels(mock_list, caplog):
    """Test result handler with validation labels using dvt_core_types schema validation."""
    table_id = "pso_data_validator_results.results_data"
    caplog.set_level(logging.INFO)
    df = schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        filter_status=None,
        result_handler=f"mock-conn.{table_id}",
        labels="a=1,b=2",
    )
    assert any(_ for _ in caplog.records if RH_WRITE_MESSAGE in _.msg)
    run_id = df[consts.CONFIG_RUN_ID][0]

    # Hijacking DVT raw query to query the results the table.
    # Labels columns should NOT be empty.
    rows = raw_query_rows(
        f"SELECT COUNT(*) FROM {table_id} WHERE run_id = '{run_id}' AND array_length(labels, 1) = 2",
        conn="mock-conn",
    )
    # Ensure that we added the data to the results table.
    assert len(df) == rows[0][0]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys)


def test_raw_column_metadata():
    """Test that get_raw_data_types custom Backend method returns expected results."""
    client = clients.get_data_client(CONN)
    raw_types = list(
        client.raw_column_metadata(
            database="pso_data_validator", table="dvt_core_types"
        )
    )
    assert raw_types == DVT_CORE_TYPES_RAW_DATA_TYPES
