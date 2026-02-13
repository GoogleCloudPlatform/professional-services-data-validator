# Copyright 2025 Google LLC
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
import pathlib
from unittest import mock

import pytest

from data_validation import cli_tools, consts
from tests.system.data_sources.common_functions import (
    DVT_TRICKY_DATES_COLUMNS,
    binary_key_assertions,
    column_validation_test,
    find_tables_test,
    generate_and_run_table_partitions_test,
    id_column_row_validation_test,
    id_type_test_assertions,
    null_not_null_assertions,
    raw_query_test,
    row_validation_many_columns_test,
    row_validation_test,
    run_test_from_cli_args,
    schema_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import DVT_CORE_TYPES_COLUMNS


# Our Sybase test infra has a habit of failing to connect but then working on retry.
pytestmark = pytest.mark.flaky(
    reruns=1,
    reruns_delay=2,
    only_rerun=["DataClientConnectionFailure"],
)

SYBASE_HOST = os.getenv("SYBASE_HOST", "127.0.0.1")
SYBASE_USER = os.getenv("SYBASE_USER", "sqlserver")
SYBASE_PASSWORD = os.getenv("SYBASE_PASSWORD")
SYBASE_DATABASE = os.getenv("SYBASE_DATABASE", "master")
SYBASE_ODBC_DRIVER = os.getenv("SYBASE_ODBC_DRIVER", "FreeTDS")
PROJECT_ID = os.getenv("PROJECT_ID")
CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SYBASE,
    "host": SYBASE_HOST,
    "user": SYBASE_USER,
    "password": SYBASE_PASSWORD,
    "database": SYBASE_DATABASE,
    "odbc_driver": SYBASE_ODBC_DRIVER,
    "query": '{"autocommit": "True"}',
}

# Expected result from partitioning table on 3 keys, 9 partitions
EXPECTED_PARTITION_FILTER = [
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'St. Edward''''s' OR course_id = 'St. Edward''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''''s' OR course_id = 'St. Edward''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. John''''s' OR course_id = 'St. John''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. John''''s' OR course_id = 'St. John''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. Jude''''s' OR course_id = 'St. Jude''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''''s' OR course_id = 'St. Jude''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false)))) AND (course_id < 'St. Paul''''s' OR course_id = 'St. Paul''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''''s' OR course_id = 'St. Paul''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false))))",
    ],
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= true)))) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678 OR quarter_id = 5678 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '2023-08-23' OR registration_date = '2023-08-23' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678 OR quarter_id = 5678 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '2023-08-23' OR registration_date = '2023-08-23' AND approved >= false)))) AND (course_id < 'St. Edward''''s' OR course_id = 'St. Edward''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''''s' OR course_id = 'St. Edward''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. John''''s' OR course_id = 'St. John''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < true))))",
        "quarter_id != 1111 AND (course_id > 'St. John''''s' OR course_id = 'St. John''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-26 16:00:00' OR recd_timestamp = '2023-08-26 16:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= true)))) AND (course_id < 'St. Jude''''s' OR course_id = 'St. Jude''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''''s' OR course_id = 'St. Jude''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false)))) AND (course_id < 'St. Paul''''s' OR course_id = 'St. Paul''''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND (recd_timestamp < '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date < '1969-07-20' OR registration_date = '1969-07-20' AND approved < false))))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''''s' OR course_id = 'St. Paul''''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND (recd_timestamp > '2023-08-27 15:00:00' OR recd_timestamp = '2023-08-27 15:00:00' AND (registration_date > '1969-07-20' OR registration_date = '1969-07-20' AND approved >= false))))",
    ],
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


def mock_get_connection_config(*args):
    if args[1] in ("sybase-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


##########################
# SCHEMA VALIDATION TESTS
##########################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Sybase dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # All Sybase integers go to BigQuery INT64.
            "int8:int64,int16:int64,int32:int64,!int32:!int64,"
            # BigQuery does not have a float32 type.
            "float32:float64,"
            # Sybase does not have a time zoned timestamp.
            "timestamp:timestamp('UTC'),"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_view_core_types_vw():
    """Sybase view dvt_core_types_vw schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types_vw",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_not_null_vs_nullable():
    """Compares a source table with a BigQuery target and ensure we match/fail on not null/nullable correctly."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_null_not_null",
            "--allow-list=timestamp:timestamp('UTC'),",
        ]
    )
    df = run_test_from_cli_args(args)
    null_not_null_assertions(df)


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
def test_schema_validation_reserved_words():
    """Test schema validation on a table with reserved words in column names."""
    schema_validation_test(
        tables="pso_data_validator.dvt_reserved_word_columns",
        tc="mock-conn",
    )


##########################
# COLUMN VALIDATION TESTS
##########################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Sybase dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
        count_cols="*",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
        avg_cols="*",
        std_cols="*",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have a float32 type.
    # TODO Change cols to include col_char_2 when issue-1514 is complete.
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
        avg_cols=cols,
        std_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_view_core_types_vw():
    """Sybase view dvt_core_types_vw column validation"""
    cols = ",".join([_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id")])
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
def test_column_validation_large_decimals_to_bigquery():
    """Sybase to BigQuery dvt_large_decimals column validation."""
    cols = "col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30"
    std_cols = "col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30"
    # TODO Add col_dec_38 to avg_cols below when issue-1551 is complete.
    avg_cols = "col_dec_18,col_dec_38_9,col_dec_38_30"
    column_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        count_cols=cols,
        min_cols=cols,
        sum_cols=cols,
        avg_cols=avg_cols,
        std_cols=std_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery_mismatch():
    """Oracle to BigQuery dvt_large_decimals column validation on columns we expect to have a mismatch.

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
    # The columns below have mismatching data and should be in the Dataframe.
    assert "sum__col_dec_18_fail" in df[consts.VALIDATION_NAME].values
    assert "sum__col_dec_18_1_fail" in df[consts.VALIDATION_NAME].values


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
        wildcard_include_timestamp=True,
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
def test_column_validation_reserved_words():
    """Test column validation on a table with reserved words in column names."""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_reserved_word_columns",
        count_cols="*",
    )


###########################
# ROW VALIDATION TESTS
###########################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Sybase dvt_core_types concat row validation.

    Sybase does not have a SHA256 hash function therefore this test uses concat."""
    # Excluded col_string due us not yet having support for text data type.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_string")]
    )
    row_validation_test(
        tc="mock-conn",
        concat=cols,
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from Sybase defined constraint."""
    row_validation_test(
        tc="mock-conn",
        concat="col_int8,col_int16",
        primary_keys=None,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types concat row validation.

    Sybase does not have a SHA256 hash function therefore this test uses concat."""
    # Excluded col_float32 because BigQuery does not have a float32 type.
    # Excluded col_float64 due to the lossy nature of float data type, it matches
    #   Oracle BINARY_DOUBLE but does not match BigQuery FLOAT64.
    # Excluded col_string due us not yet having support for text data type.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_float64", "col_string")
        ]
    )
    row_validation_test(
        tc="bq-conn", concat=cols, use_random_row=True, random_row_batch_size=5
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types_to_bigquery():
    """Sybase to BigQuery extended data type row validation using comparison fields"""
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _
            not in (
                "id",
                # Excluded col_float32 because BigQuery does not have a float32 type.
                "col_float32",
                # TODO Change cols to include col_char_2 when issue-1514 is complete.
                "col_char_2",
            )
        ]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        comp_fields=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """Sybase to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.

    Sybase does not have a SHA256 hash function therefore this test uses concat.
    """
    row_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        concat="id,col_data,col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31.

    Sybase does not have a SHA256 hash function therefore this test uses concat."""
    cols = ",".join(DVT_TRICKY_DATES_COLUMNS)
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        concat=cols,
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
def test_row_validation_binary_pk_to_bigquery():
    """Sybase to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test..

    Sybase does not have a SHA256 hash function therefore this test uses concat.
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
            "--concat=int_id,other_data",
            # Uncomment line below when working on issue-1633.
            # "--use-random-row",
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
def test_row_validation_datetime_pk_to_bigquery():
    """Test datetime primary key join columns.

    Sybase does not have a SHA256 hash function therefore this test uses concat."""
    # TODO Remove use_random_row option below when issue-1445 is actioned.
    id_column_row_validation_test(
        "pso_data_validator.dvt_datetime_id",
        use_random_row=False,
        concat="id,other_data",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pangrams_to_bigquery():
    """Sybase to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.

    This needs more investigation on how to handle unicode characters in Sybase.
    """
    pytest.skip(
        "Skipping test_row_validation_pangrams_to_bigquery because we don't yet understand unicode."
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
def test_row_validation_tricky_strings_to_bigquery():
    """Test with string values containing special characters."""
    pytest.skip(
        "Skipping test_row_validation_tricky_strings_to_bigquery because the version of Sybase we have does not support rtrim of all whitespace."
    )
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_strings",
        tc="bq-conn",
        concat="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """Sybase dvt_many_cols row validation"""
    row_validation_many_columns_test(concat_arg="concat")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_identifiers():
    """Test row validation on a table with special characters in table and column names."""
    row_validation_test(
        tables="pso_data_validator.dvt-identifier$_#",
        tc="mock-conn",
        concat="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_reserved_words():
    """Test row validation on a table with reserved words in column names.

    Sybase does not have a SHA256 hash function therefore this test uses concat."""
    row_validation_test(
        tables="pso_data_validator.dvt_reserved_word_columns",
        tc="mock-conn",
        concat="*",
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


################################
# CUSTOM-QUERY VALIDATION TESTS
################################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types custom-query column validation"""
    custom_query_validation_test(tc="bq-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types custom-query row comparison-fields validation.

    Sybase identifiers are case sensitive therefore no mixed case in source_query below which
    differs from this test in other engines."""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,col_varchar_30,col_date from pso_data_validator.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        concat="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_comp_fields_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types custom-query row comparison-fields validation.

    Sybase identifiers are case sensitive therefore no mixed case in source_query below which
    differs from this test in other engines."""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,col_varchar_30,col_date from pso_data_validator.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        comp_fields="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Sybase dvt_many_cols custom-query row validation"""
    row_validation_many_columns_test(
        validation_type="custom-query", concat_arg="concat"
    )


############################
# GENERATE-PARTITIONS TESTS
#
# Because Sybase does not support window functions we cannot test partition filters using:
#   test_generate_partitions and test_generate_partitions_datetime_pk
#
# Instead we run end-to-end partition tests using BigQuery as the source with Sybase as the target.
############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_and_run_partitions(tmp_path: pathlib.Path):
    """Test generate and execute partition configs."""
    pytest.skip("Skipping test_generate_and_run_partitions due to issue-1613.")
    generate_and_run_table_partitions_test(
        tmp_path,
        sc="bq-conn",
        tc="mock-conn",
        concat="*",
        # Add approved into pk below when issue-1618 is resolved.
        pk="course_id,quarter_id,recd_timestamp,registration_date",
    )


##############################
# FIND-TABLE VALIDATION TESTS
##############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Sybase to BigQuery test of find-tables command."""
    find_tables_test()


##################
# RAW QUERY TESTS
##################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys)
