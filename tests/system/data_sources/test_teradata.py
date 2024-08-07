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

from data_validation import cli_tools, data_validation, consts
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    id_type_test_assertions,
    null_not_null_assertions,
    run_many_columns_test_from_cli_args,
    run_test_from_cli_args,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import generate_partitions_test


TERADATA_USER = os.getenv("TERADATA_USER", "udf")
TERADATA_PASSWORD = os.getenv("TERADATA_PASSWORD")
TERADATA_HOST = os.getenv("TERADATA_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")

CONN = {
    "source_type": "Teradata",
    "host": TERADATA_HOST,
    "user_name": TERADATA_USER,
    "password": TERADATA_PASSWORD,
    "port": 1025,
}


TERADATA_COLUMN_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TABLE_NAME: "CALENDAR",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "year_of_calendar",
            consts.CONFIG_TARGET_COLUMN: "year_of_calendar",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
    consts.CONFIG_FILTERS: [
        {
            consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,
            consts.CONFIG_FILTER_SOURCE_COLUMN: "year_of_calendar",
            consts.CONFIG_FILTER_SOURCE_VALUE: 2010,
            consts.CONFIG_FILTER_TARGET_COLUMN: "year_of_calendar",
            consts.CONFIG_FILTER_TARGET_VALUE: 2010,
        },
    ],
}

TERADATA_ROW_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Row",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TABLE_NAME: "CALENDAR",
    consts.CONFIG_TARGET_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TARGET_TABLE_NAME: "CALENDAR",
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
    consts.CONFIG_FILTERS: [],
    consts.CONFIG_USE_RANDOM_ROWS: False,
    consts.CONFIG_COMPARISON_FIELDS: [
        {
            consts.CONFIG_SOURCE_COLUMN: "concat__all",
            consts.CONFIG_TARGET_COLUMN: "concat__all",
            consts.CONFIG_FIELD_ALIAS: "concat__all",
            consts.CONFIG_CAST: None,
        }
    ],
    consts.CONFIG_CALCULATED_FIELDS: [
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "cast__calendar_date",
            consts.CONFIG_TYPE: "custom",
            consts.CONFIG_DEPTH: 0,
            consts.CONFIG_CUSTOM_IBIS_EXPR: "ibis.expr.types.TemporalValue.strftime",
            consts.CONFIG_CUSTOM_PARAMS: [{"format_str": "%Y-%m-%d"}],
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "cast__day_of_week",
            consts.CONFIG_TYPE: "cast",
            consts.CONFIG_DEPTH: 0,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["cast__calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["cast__calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: "ifnull",
            consts.CONFIG_DEPTH: 1,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["cast__day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["cast__day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: "ifnull",
            consts.CONFIG_DEPTH: 1,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["ifnull__cast__calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["ifnull__cast__calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "rstrip__ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: "rstrip",
            consts.CONFIG_DEPTH: 2,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["ifnull__cast__day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["ifnull__cast__day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "rstrip__ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: "rstrip",
            consts.CONFIG_DEPTH: 2,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [
                "rstrip__ifnull__cast__calendar_date"
            ],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [
                "rstrip__ifnull__cast__calendar_date"
            ],
            consts.CONFIG_FIELD_ALIAS: "upper__rstrip__ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: "upper",
            consts.CONFIG_DEPTH: 3,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [
                "rstrip__ifnull__cast__day_of_week"
            ],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [
                "rstrip__ifnull__cast__day_of_week"
            ],
            consts.CONFIG_FIELD_ALIAS: "upper__rstrip__ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: "upper",
            consts.CONFIG_DEPTH: 3,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [
                "upper__rstrip__ifnull__cast__calendar_date",
                "upper__rstrip__ifnull__cast__day_of_week",
            ],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [
                "upper__rstrip__ifnull__cast__calendar_date",
                "upper__rstrip__ifnull__cast__day_of_week",
            ],
            consts.CONFIG_FIELD_ALIAS: "concat__all",
            consts.CONFIG_TYPE: "concat",
            consts.CONFIG_DEPTH: 4,
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_SOURCE_COLUMN: "calendar_date",
            consts.CONFIG_TARGET_COLUMN: "calendar_date",
            consts.CONFIG_FIELD_ALIAS: "calendar_date",
            consts.CONFIG_CAST: None,
        }
    ],
}


def test_count_validator():
    validator = data_validation.DataValidation(TERADATA_COLUMN_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def test_row_validator():
    validator = data_validation.DataValidation(TERADATA_ROW_CONFIG, verbose=True)
    df = validator.execute()
    assert df["validation_status"][0] == "success"


def mock_get_connection_config(*args):
    if args[1] in ("td-conn", "mock-conn"):
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
            "-tbls=udf.dvt_core_types",
            "--filter-status=fail",
        ]
    )
    df = run_test_from_cli_args(args)
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
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--exclusion-columns=id",
            (
                # Teradata integers go to BigQuery INT64.
                "--allow-list=int8:int64,int16:int64,int32:int64"
            ),
        ]
    )
    df = run_test_from_cli_args(args)
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
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_null_not_null=pso_data_validator.dvt_null_not_null",
        ]
    )
    df = run_test_from_cli_args(args)
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
            "-tbls=udf.dvt_core_types",
            "--filters=id>0 AND col_int8>0",
            "--filter-status=fail",
            "--grouped-columns=col_varchar_30",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38,col_dec_10_2,col_datetime",
            "--min=col_int8,col_int16,col_int32,col_int64,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38,col_dec_10_2,col_datetime",
            "--max=col_int8,col_int16,col_int32,col_int64,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38,col_dec_10_2,col_datetime",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    # Excluded col_string because LONG VARCHAR column causes exception regardless of column contents:
    # [Error 3798] A column or character expression is larger than the max size.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=udf.dvt_core_types",
            "--filters=id>0 AND col_int8>0",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" < \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" < 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" > 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" < 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" > 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" < 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" > 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" < 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" > 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" < 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" > 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" < 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" > 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" < 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" > 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" < 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" > 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" >= 1234 ) ) ) ) )',
    ],
    [
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" < \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" < 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" > 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" < 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'ALG001\' ) OR ( ( "course_id" = \'ALG001\' ) AND ( ( "quarter_id" > 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" < 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" > 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" < 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" > 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" < 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'GEO001\' ) OR ( ( "course_id" = \'GEO001\' ) AND ( ( "quarter_id" > 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" < 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" > 1 ) OR ( ( "quarter_id" = 1 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" < 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" > 2 ) OR ( ( "quarter_id" = 2 ) AND ( "student_id" >= 1234 ) ) ) ) ) AND ( ( "course_id" < \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" < 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" < 1234 ) ) ) ) )',
        ' ( quarter_id <> 1111 ) AND ( ( "course_id" > \'TRI001\' ) OR ( ( "course_id" = \'TRI001\' ) AND ( ( "quarter_id" > 3 ) OR ( ( "quarter_id" = 3 ) AND ( "student_id" >= 1234 ) ) ) ) )',
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_teradata_generate_table_partitions():
    """Test generate table partitions on BigQuery"""
    generate_partitions_test(
        EXPECTED_PARTITION_FILTER,
        tables="udf.test_generate_partitions",
        filters="quarter_id <> 1111",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    # Excluded col_string because LONG VARCHAR column causes exception regardless of column contents:
    # [Error 3798] A column or character expression is larger than the max size.
    # TODO Change --hash option to include col_tstz when issue-929 is complete.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """Teradata to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_large_decimals=pso_data_validator.dvt_large_decimals",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=id,col_data,col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """Teradata to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_binary=pso_data_validator.dvt_binary",
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
def test_row_validation_string_pk_to_bigquery():
    """Teradata to BigQuery dvt_string_id row validation.
    This is testing string primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_string_id=pso_data_validator.dvt_string_id",
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
def test_row_validation_char_pk_to_bigquery():
    """Teradata to BigQuery dvt_char_id row validation.
    This is testing CHAR primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_char_id=pso_data_validator.dvt_char_id",
            "--primary-keys=id",
            "--hash=id,other_data",
            # We need to trim padded string PKs due to a Teradata client "quirk".
            "--trim-string-pks",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_char_comp_field_to_bigquery():
    """Teradata to BigQuery char comparison field validation.
    Due to a Teradata client "quirk" comparison fields should add an rstrip()
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "-comp-fields=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df, expected_rows=45)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pangrams_to_bigquery():
    """Teradata to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_pangrams=pso_data_validator.dvt_pangrams",
            "--primary-keys=id",
            # Using concat because the hash_sha256 UDF is not unicode compliant.
            "--concat=*",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns_to_bigquery():
    """Teradata to BigQuery dvt_many_cols row validation.
    This is testing many columns logic for --concat, there's a test in Oracle for --hash.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_many_cols=pso_data_validator.dvt_many_cols",
            "--primary-keys=id",
            "--hash=*",
            "--filter-status=fail",
        ]
    )
    run_many_columns_test_from_cli_args(args)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types custom-query validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "column",
            "-sc=td-conn",
            "-tc=bq-conn",
            "--source-query=select * from udf.dvt_core_types",
            "--target-query=select * from pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--count=*",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types custom-query row validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "--source-query=select id,col_int64,COL_VARCHAR_30,col_date from udf.dvt_core_types",
            "--target-query=select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--comparison-fields=col_int64,col_varchar_30,col_date",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_hash_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types custom-query row validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "--source-query=select id,col_int64,COL_VARCHAR_30,col_date from udf.dvt_core_types",
            "--target-query=select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int64,col_varchar_30,col_date",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0
