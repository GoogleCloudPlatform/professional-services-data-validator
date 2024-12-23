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

import pytest
import pathlib

from data_validation import cli_tools, data_validation, consts
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    find_tables_test,
    id_type_test_assertions,
    null_not_null_assertions,
    raw_query_test,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    schema_validation_test,
    column_validation_test,
    row_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import (
    partition_table_test,
    partition_query_test,
)

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
    [
        ' quarter_id <> 1111 AND ( "COURSE_ID" < \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" < 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" > 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" < 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" > 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" < 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" > 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" < 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" > 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" < 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" > 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" < 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" > 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" < 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" > 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" < 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" > 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" >= 1234 ) )',
    ],
    [
        ' quarter_id <> 1111 AND ( "COURSE_ID" < \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" < 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" > 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" < 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'ALG001\' OR "COURSE_ID" = \'ALG001\' AND ( "QUARTER_ID" > 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" < 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" > 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" < 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" > 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" < 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'GEO001\' OR "COURSE_ID" = \'GEO001\' AND ( "QUARTER_ID" > 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" < 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" > 1 OR "QUARTER_ID" = 1 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" < 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" > 2 OR "QUARTER_ID" = 2 AND "STUDENT_ID" >= 1234 ) ) AND ( "COURSE_ID" < \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" < 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" < 1234 ) )',
        ' quarter_id <> 1111 AND ( "COURSE_ID" > \'TRI001\' OR "COURSE_ID" = \'TRI001\' AND ( "QUARTER_ID" > 3 OR "QUARTER_ID" = 3 AND "STUDENT_ID" >= 1234 ) )',
    ],
]

QUERY_PARTITION_FILTER = [
    [
        ' "quarter_id" <> 1111 AND ( "course_id" < \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" < 2 OR "quarter_id" = 2 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" > 2 OR "quarter_id" = 2 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" < 3 OR "quarter_id" = 3 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" > 3 OR "quarter_id" = 3 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" < 1 OR "quarter_id" = 1 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" > 1 OR "quarter_id" = 1 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" < 2 OR "quarter_id" = 2 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" > 2 OR "quarter_id" = 2 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" < 3 OR "quarter_id" = 3 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" > 3 OR "quarter_id" = 3 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" < 1 OR "quarter_id" = 1 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" > 1 OR "quarter_id" = 1 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" < 2 OR "quarter_id" = 2 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" > 2 OR "quarter_id" = 2 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" < 3 OR "quarter_id" = 3 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" > 3 OR "quarter_id" = 3 AND "student_id" >= 1234 ) )',
    ],
    [
        ' "quarter_id" <> 1111 AND ( "course_id" < \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" < 2 OR "quarter_id" = 2 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" > 2 OR "quarter_id" = 2 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" < 3 OR "quarter_id" = 3 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'ALG001\' OR "course_id" = \'ALG001\' AND ( "quarter_id" > 3 OR "quarter_id" = 3 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" < 1 OR "quarter_id" = 1 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" > 1 OR "quarter_id" = 1 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" < 2 OR "quarter_id" = 2 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" > 2 OR "quarter_id" = 2 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" < 3 OR "quarter_id" = 3 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'GEO001\' OR "course_id" = \'GEO001\' AND ( "quarter_id" > 3 OR "quarter_id" = 3 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" < 1 OR "quarter_id" = 1 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" > 1 OR "quarter_id" = 1 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" < 2 OR "quarter_id" = 2 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" > 2 OR "quarter_id" = 2 AND "student_id" >= 1234 ) ) AND ( "course_id" < \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" < 3 OR "quarter_id" = 3 AND "student_id" < 1234 ) )',
        ' "quarter_id" <> 1111 AND ( "course_id" > \'TRI001\' OR "course_id" = \'TRI001\' AND ( "quarter_id" > 3 OR "quarter_id" = 3 AND "student_id" >= 1234 ) )',
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate table partitions on Snowflake"""
    partition_table_test(
        EXPECTED_PARTITION_FILTER,
        tables="PSO_DATA_VALIDATOR.PUBLIC.TEST_GENERATE_PARTITIONS",
    )
    partition_query_test(
        QUERY_PARTITION_FILTER,
        tmp_path,
        tables="PSO_DATA_VALIDATOR.PUBLIC.TEST_GENERATE_PARTITIONS",
        filters='"quarter_id" <> 1111',
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Snowflake to Snowflake dvt_core_types schema validation"""
    schema_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES", tc="mock-conn"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_specific_types():
    """Snowflake to Snowflake test_specific_data_types schema validation"""
    schema_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.TEST_SPECIFIC_DATA_TYPES", tc="mock-conn"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Snowflake to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES=pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # Integer Snowflake NUMBERs to to BigQuery INT64.
            "decimal(38,0):int64"
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
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PUBLIC.DVT_NULL_NOT_NULL=pso_data_validator.dvt_null_not_null",
        ]
    )
    df = run_test_from_cli_args(args)
    null_not_null_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Snowflake to Snowflake dvt_core_types column validation"""
    # TODO Change --sum/min/max to '*' when issue-916 is complete (support for col_tstz)
    cols = "COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME"
    column_validation_test(
        tc="mock-conn",
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Snowflake to BigQuery dvt_core_types column validation"""
    # TODO Change --sum/min/max to '*' when issue-916 is complete (support for col_tstz)
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime"
    column_validation_test(
        tc="bq-conn",
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES=pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Snowflake to Snowflake dvt_core_types row validation"""
    row_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
        tc="mock-conn",
        filters="id>0 AND col_int8>0",
        # TODO Change --hash to '*' when issue-916 is complete (support for col_tstz)
        hash="COL_INT8,COL_INT16,COL_INT32,COL_INT64,COL_DEC_20,COL_DEC_38,COL_DEC_10_2,COL_FLOAT32,COL_FLOAT64,COL_VARCHAR_30,COL_CHAR_2,COL_STRING,COL_DATE,COL_DATETIME",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from Snowflake defined constraint."""
    row_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
        tc="mock-conn",
        hash="col_int8,col_int16",
        primary_keys=None,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Snowflake to BigQuery dvt_core_types row validation"""
    row_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES=pso_data_validator.dvt_core_types",
        tc="bq-conn",
        # TODO Change --hash to '*' when issue-916 is complete (support for col_tstz)
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """Snowflake to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_BINARY=pso_data_validator.dvt_binary",
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
    """Snowflake to BigQuery dvt_char_id row validation - not executed.
    This is testing CHAR primary key join columns.
    Includes random row filter test.

    Snowflake currently deviates from common CHAR semantics in that strings
    shorter than the maximum length are not space-padded at the end.
    This test is skipped.
    """
    pytest.skip(
        "Skipping test_row_validation_char_pk_to_bigquery because of Snowflake CHAR semantics"
    )
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_CHAR_ID=pso_data_validator.dvt_char_id",
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
    """Snowflake to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=snowflake-conn",
            "-tc=bq-conn",
            "-tbls=PSO_DATA_VALIDATOR.PUBLIC.DVT_PANGRAMS=pso_data_validator.dvt_pangrams",
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
    """Snowflake to Snowflake dvt_core_types custom-query validation"""
    custom_query_validation_test(
        tc="mock-conn",
        # TODO Change to 'select *' when issue-916 is complete (support for col_tstz)
        source_query="select col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime from PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
        target_query="select col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime from PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Snowflake to Snowflake test of find-tables command."""
    pytest.skip("Skipping test_find_tables until issue 1198 has been resolved.")
    find_tables_test(tc="mock-conn", allowed_schema="PSO_DATA_VALIDATOR.PUBLIC")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """Snowflake dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(
        schema="PSO_DATA_VALIDATOR.PUBLIC",
        table="DVT_MANY_COLS",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Snowflake dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(
        validation_type="custom-query",
        schema="PSO_DATA_VALIDATOR.PUBLIC",
        table="DVT_MANY_COLS",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_identifiers():
    """Test schema validation on a table with special characters in table and column names."""
    schema_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT-IDENTIFIER$_#",
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
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT-IDENTIFIER$_#",
        count_cols="*",
        filters="'COL#HASH' IS NOT NULL",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_identifiers():
    """Test row validation on a table with special characters in table and column names."""
    row_validation_test(
        tables="PSO_DATA_VALIDATOR.PUBLIC.DVT-IDENTIFIER$_#",
        tc="mock-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys, table="PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES")
