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

from data_validation import cli_tools, data_validation, consts
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    DVT_TRICKY_DATES_COLUMNS,
    binary_key_assertions,
    column_validation_test,
    custom_query_validation_test,
    find_tables_test,
    id_column_row_validation_test,
    id_column_query_row_validation_test,
    id_type_test_assertions,
    null_not_null_assertions,
    partition_table_test,
    partition_query_test,
    raw_query_test,
    row_validation_many_columns_test,
    row_validation_test,
    run_test_from_cli_args,
    schema_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN

TERADATA_USER = os.getenv("TERADATA_USER", "udf")
TERADATA_PASSWORD = os.getenv("TERADATA_PASSWORD")
TERADATA_HOST = os.getenv("TERADATA_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")

CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_TERADATA,
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
    consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
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
    consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
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
            consts.CONFIG_TYPE: consts.CALC_FIELD_IFNULL,
            consts.CONFIG_DEPTH: 1,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["cast__day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["cast__day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: consts.CALC_FIELD_IFNULL,
            consts.CONFIG_DEPTH: 1,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["ifnull__cast__calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["ifnull__cast__calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "rstrip__ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: consts.CALC_FIELD_RSTRIP,
            consts.CONFIG_DEPTH: 2,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["ifnull__cast__day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["ifnull__cast__day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "rstrip__ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: consts.CALC_FIELD_RSTRIP,
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
            consts.CONFIG_TYPE: consts.CALC_FIELD_UPPER,
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
            consts.CONFIG_TYPE: consts.CALC_FIELD_UPPER,
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
            consts.CONFIG_TYPE: consts.CALC_FIELD_CONCAT,
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

DVT_CORE_TYPES_RAW_DATA_TYPES = [
    ("id", "BIGINT", None, 8, None, None, False),
    ("col_int8", "BYTEINT", None, 1, None, None, True),
    ("col_int16", "SMALLINT", None, 2, None, None, True),
    ("col_int32", "INTEGER", None, 4, None, None, True),
    ("col_int64", "BIGINT", None, 8, None, None, True),
    ("col_dec_20", "NUMBER", None, 18, 20, 0, True),
    ("col_dec_38", "NUMBER", None, 18, 38, 0, True),
    ("col_dec_10_2", "NUMBER", None, 18, 10, 2, True),
    ("col_float32", "FLOAT", None, 8, None, None, True),
    ("col_float64", "FLOAT", None, 8, None, None, True),
    ("col_varchar_30", "VARCHAR", None, 60, None, None, True),
    ("col_char_2", "CHAR", None, 4, None, None, True),
    ("col_string", "LONG VARCHAR", None, 64000, None, None, True),
    ("col_date", "DATE", None, 4, None, None, True),
    ("col_datetime", "TIMESTAMP", None, 23, 0, 3, True),
    ("col_tstz", "TIMESTAMP WITH TIME ZONE", None, 29, 0, 3, True),
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


def test_count_validator():
    validator = data_validation.DataValidation(TERADATA_COLUMN_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df[consts.SOURCE_AGG_VALUE][0] == df[consts.TARGET_AGG_VALUE][0]


def test_row_validator():
    validator = data_validation.DataValidation(TERADATA_ROW_CONFIG, verbose=True)
    df = validator.execute()
    assert df[consts.VALIDATION_STATUS][0] == "success"


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
    """Teradata to Teradata dvt_core_types schema validation"""
    schema_validation_test(
        tables="udf.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="udf.dvt_core_types=pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # Teradata integers go to BigQuery INT64.
            "--allow-list=int8:int64,int16:int64,int32:int64"
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
    """Teradata to Teradata dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="udf.dvt_core_types",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types column validation"""
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _
            not in (
                "id",
                "col_float32",
                "col_tstz",
            )
        ]
    )
    column_validation_test(
        tc="bq-conn",
        tables="udf.dvt_core_types=pso_data_validator.dvt_core_types",
        # TODO Change --sum/min/max to '*' when issue-916 is complete (support for col_tstz)
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_time_table_to_bigquery():
    """Teradata to BigQuery dvt_time_table column validation."""
    column_validation_test(
        tc="bq-conn",
        tables="udf.dvt_time_table=pso_data_validator.dvt_time_table",
        # Unlike other temporal types, count is the only column validation supported for time
        count_cols="col_time",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery():
    """Teradata to BigQuery dvt_large_decimals column validation."""
    # TODO Add col_dec_38 to cols when issue-1360 has been resolved.
    cols = "col_dec_18,col_dec_38_9,col_dec_38_30"
    column_validation_test(
        tables="udf.dvt_large_decimals=pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        count_cols=cols,
        min_cols=cols,
        sum_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery_mismatch():
    """Teradata to BigQuery dvt_large_decimals column validation on columns we expect to have a mismatch.

    Regression test for:
      https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1007
    """
    cols = "col_dec_18_fail,col_dec_18_1_fail"
    df = column_validation_test(
        tables="udf.dvt_large_decimals=pso_data_validator.dvt_large_decimals",
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
def test_column_validation_view_core_types_vw():
    """Teradata to Teradata view dvt_core_types_vw column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="udf.dvt_core_types_vw",
        count_cols="*",
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
def test_column_validation_tricky_dates_to_bigquery():
    """
    Test with date values that are at the extremes, e.g. 9999-12-31.

    # Excluded col_ts_high below because I'm unable to correctly insert desired literal.
    #   https://support.teradata.com/knowledge?id=kb_article_view&sys_kb_id=0e81918ac36da9103eb2d88f05013138
    """
    cols = ",".join(_ for _ in DVT_TRICKY_DATES_COLUMNS if _ != "col_ts_high")
    column_validation_test(
        tc="bq-conn",
        tables="udf.dvt_tricky_dates=pso_data_validator.dvt_tricky_dates",
        min_cols=cols,
        max_cols=cols,
        sum_cols=cols,
        wildcard_include_timestamp=True,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_time_table():
    """Teradata to BigQuery dvt_time_table row validation."""
    row_validation_test(
        tables="udf.dvt_time_table=pso_data_validator.dvt_time_table", hash="*"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Validate core types against themselves in Teradata"""
    # Excluded col_string because LONG VARCHAR column causes exception regardless of column contents:
    # [Error 3798] A column or character expression is larger than the max size.
    row_validation_test(
        tables="udf.dvt_core_types", tc="mock-conn", filters="id>0 AND col_int8>0"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from Teradata defined constraint.

    Tests this with comp-fields, some other engines test with hash validation."""
    row_validation_test(
        tables="udf.dvt_core_types",
        tc="mock-conn",
        comp_fields="col_int8,col_int16",
        primary_keys=None,
    )


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" < 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0))))))))) AND ((\"course_id\" < 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0))))))))) AND ((\"course_id\" < 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0))))))))) AND ((\"course_id\" < 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0)))))))))",
    ],
    [
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" < 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Edward''s') OR ((\"course_id\" = 'St. Edward''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" < '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" < 1)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. John''s') OR ((\"course_id\" = 'St. John''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-26T16:00:00') OR ((\"recd_timestamp\" = '2023-08-26T16:00:00') AND ((\"registration_date\" > '2023-08-23') OR ((\"registration_date\" = '2023-08-23') AND (\"approved\" >= 1))))))))) AND ((\"course_id\" < 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0))))))))) AND ((\"course_id\" < 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Jude''s') OR ((\"course_id\" = 'St. Jude''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0))))))))) AND ((\"course_id\" < 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" < 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" > 1234) OR ((\"quarter_id\" = 1234) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0))))))))) AND ((\"course_id\" < 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" < 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" < '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" < '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" < 0)))))))))",
        "(course_id LIKE 'St. %''s') AND ((\"course_id\" > 'St. Paul''s') OR ((\"course_id\" = 'St. Paul''s') AND ((\"quarter_id\" > 5678) OR ((\"quarter_id\" = 5678) AND ((\"recd_timestamp\" > '2023-08-27T15:00:00') OR ((\"recd_timestamp\" = '2023-08-27T15:00:00') AND ((\"registration_date\" > '1969-07-20') OR ((\"registration_date\" = '1969-07-20') AND (\"approved\" >= 0)))))))))",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate partitions, first on table, then custom query on Teradata."""
    partition_table_test(
        EXPECTED_PARTITION_FILTER,
        tables="udf.test_generate_partitions_v2",
        filters="course_id LIKE 'St. %''s'",
    )
    partition_query_test(
        EXPECTED_PARTITION_FILTER,
        tmp_path,
        tables="udf.test_generate_partitions_v2",
        filters="course_id LIKE 'St. %''s'",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    # Excluded col_string because LONG VARCHAR column causes exception regardless of column contents:
    # [Error 3798] A column or character expression is larger than the max size.
    row_validation_test(
        tables="udf.dvt_core_types=pso_data_validator.dvt_core_types",
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """Teradata to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    """
    row_validation_test(
        tables="udf.dvt_large_decimals=pso_data_validator.dvt_large_decimals",
        hash="id,col_data,col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_latin_to_bigquery():
    """Teradata to BigQuery dvt latin columns in teradata vs STRING columns in BigQuery"""
    row_validation_test(tables="udf.dvt_latin=pso_data_validator.dvt_latin", hash="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pangrams_hash_to_bigquery():
    """Teradata to BigQuery dvt Unicode columns (non European languages) in teradata vs STRING columns in BigQuery"""
    row_validation_test(
        tables="udf.dvt_pangrams=pso_data_validator.dvt_pangrams", hash="*"
    )


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
def test_row_validation_comp_fields_binary_values_to_bigquery():
    """dvt_binary row validation with comparison fields."""
    row_validation_test(
        tables="udf.dvt_binary=pso_data_validator.dvt_binary",
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
    id_column_row_validation_test(
        "udf.dvt_fixed_char_id=pso_data_validator.dvt_fixed_char_id"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_varchar_pk_row_validation_to_bigquery():
    """Test varchar primary keys"""
    id_column_row_validation_test(
        "udf.dvt_varchar_id=pso_data_validator.dvt_varchar_id"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_fixed_char_pk_query_row_validation_to_bigquery():
    """Test fixed char primary keys on custom query"""
    id_column_query_row_validation_test(
        "udf.dvt_fixed_char_id=pso_data_validator.dvt_fixed_char_id"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_varchar_pk_query_row_validation_to_bigquery():
    """Test varchar primary keys on custom query"""
    id_column_query_row_validation_test(
        "udf.dvt_varchar_id=pso_data_validator.dvt_varchar_id"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_datetime_pk_to_bigquery():
    """Test datetime primary key join columns"""
    # TODO Remove use_randow_row option below when issue-1445 is actioned.
    id_column_row_validation_test(
        "udf.dvt_datetime_id=pso_data_validator.dvt_datetime_id",
        use_randow_row=False,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions_datetime_pk():
    """Test generate partitions on datetime primary key"""
    pytest.skip("Skipping test_generate_partitions_datetime_pk because of issue-1443")
    partition_table_test(
        EXPECTED_DATETIME_ID_PARTITION_FILTER,
        pk="id",
        tables="udf.dvt_datetime_id",
        filters="other_data IS NOT NULL",
        partition_num=2,
    )


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
def test_custom_query_column_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types custom-query validation"""
    custom_query_validation_test(
        tc="bq-conn", source_query="select * from udf.dvt_core_types", count_cols="*"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types custom-query row validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,COL_VARCHAR_30,col_date from udf.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        comp_fields="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_hash_validation_core_types_to_bigquery():
    """Teradata to BigQuery dvt_core_types custom-query row validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,COL_VARCHAR_30,col_date from udf.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        hash="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Teradata to Teradata test of find-tables command."""
    find_tables_test(tc="mock-conn", allowed_schema="udf")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """Teradata dvt_many_cols row validation.
    This is testing many columns logic for --concat, there are other tests for --hash.
    """
    row_validation_many_columns_test(schema="udf", concat_arg="concat")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Teradata dvt_many_cols custom-query row validation.
    This is testing many columns logic for --concat, there are other tests for --hash.
    """
    row_validation_many_columns_test(
        schema="udf", validation_type="custom-query", concat_arg="concat"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_identifiers():
    """Test schema validation on a table with special characters in table and column names."""
    pytest.skip("Skipping test_row_validation_identifiers because of issue-1271")
    schema_validation_test(
        tables="udf.dvt-identifier$_#",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_identifiers():
    """Test column validation on a table with special characters in table and column names."""
    pytest.skip("Skipping test_row_validation_identifiers because of issue-1271")
    column_validation_test(
        tc="mock-conn",
        tables="udf.dvt-identifier$_#",
        count_cols="*",
        filters="COL#HASH IS NOT NULL",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_identifiers():
    """Test row validation on a table with special characters in table and column names."""
    pytest.skip("Skipping test_row_validation_identifiers because of issue-1271")
    row_validation_test(
        tables="udf.dvt-identifier$_#",
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
        tables="udf.dvt_reserved_word_columns",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_reserved_words():
    """Test column validation on a table with reserved words in column names."""
    pytest.skip("Skipping test_column_validation_reserved_words because of issue-1436")
    column_validation_test(
        tc="mock-conn",
        tables="udf.dvt_reserved_word_columns",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_reserved_words():
    """Test row validation on a table with reserved words in column names."""
    pytest.skip("Skipping test_row_validation_reserved_words because of issue-1436")
    row_validation_test(
        tables="udf.dvt_reserved_word_columns",
        tc="mock-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_reserved_words():
    """Test row validation on a table with reserved words in column names."""
    pytest.skip(
        "Skipping test_row_validation_comp_fields_reserved_words because of issue-1436"
    )
    row_validation_test(
        tables="udf.dvt_reserved_word_columns",
        tc="mock-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_dates_to_bigquery():
    """
    Test with date values that are at the extremes, e.g. 9999-12-31.

    Excluded col_ts_high below because I'm unable to correctly insert desired literal.
      https://support.teradata.com/knowledge?id=kb_article_view&sys_kb_id=0e81918ac36da9103eb2d88f05013138
    """
    cols = ",".join(_ for _ in DVT_TRICKY_DATES_COLUMNS if _ != "col_ts_high")
    row_validation_test(
        tables="udf.dvt_tricky_dates=pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        hash=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_tricky_dates_to_bigquery():
    """
    Test with date values that are at the extremes, e.g. 9999-12-31.

    Excluded col_ts_high below because I'm unable to correctly insert desired literal.
      https://support.teradata.com/knowledge?id=kb_article_view&sys_kb_id=0e81918ac36da9103eb2d88f05013138
    """
    cols = ",".join(_ for _ in DVT_TRICKY_DATES_COLUMNS if _ != "col_ts_high")
    row_validation_test(
        tables="udf.dvt_tricky_dates=pso_data_validator.dvt_tricky_dates",
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
        tables="udf.dvt_tricky_strings=pso_data_validator.dvt_tricky_strings",
        tc="bq-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_hash_bool_to_bigquery():
    """Test row validation on a table with bool data types in the target, Teradata does not have a bool type."""
    row_validation_test(
        tables="udf.dvt_bool=pso_data_validator.dvt_bool",
        tc="bq-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_bool_to_bigquery():
    """Test row validation -comp-fields on a table with bool data types in the target, Teradata does not have a bool type."""
    row_validation_test(
        tables="udf.dvt_bool=pso_data_validator.dvt_bool",
        tc="bq-conn",
        comp_fields="col_bool_dec,col_bool_int,col_bool_ch1,col_bool_chy",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys, table="udf.dvt_core_types")


def test_raw_column_metadata():
    """Test that get_raw_data_types custom Backend method returns expected results."""
    from data_validation import clients

    client = clients.get_data_client(CONN)
    raw_types = list(client.raw_column_metadata(database="udf", table="dvt_core_types"))
    assert raw_types == DVT_CORE_TYPES_RAW_DATA_TYPES
