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
    DVT_TRICKY_DATES_COLUMNS,
    binary_key_assertions,
    column_validation_test,
    column_validation_test_args,
    column_validation_test_config_managers,
    find_tables_test,
    id_column_row_validation_test,
    id_column_query_row_validation_test,
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
from tests.system.data_sources.test_postgres import CONN as PG_CONN
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    partition_table_test,
    partition_query_test,
)


ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PORT = os.getenv("ORACLE_PORT", "1521")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DATABASE = os.getenv("ORACLE_DATABASE", "XEPDB1")

CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_ORACLE,
    "host": ORACLE_HOST,
    "user": "SYSTEM",
    "password": ORACLE_PASSWORD,
    "port": int(ORACLE_PORT),
    "database": ORACLE_DATABASE,
}


ORACLE_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
    consts.CONFIG_TABLE_NAME: "items_price",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "price",
            consts.CONFIG_TARGET_COLUMN: "price",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
    consts.CONFIG_FILTER_STATUS: None,
}

ORA2PG_COLUMNS = [
    "id",
    "col_num_4",
    "col_num_9",
    "col_num_18",
    "col_num_38",
    "col_num",
    "col_num_10_2",
    "col_num_float",
    "col_float32",
    "col_float64",
    "col_varchar_30",
    "col_char_2",
    "col_nvarchar_30",
    "col_nchar_2",
    "col_date",
    "col_ts",
    "col_tstz",
    "col_interval_ds",
    "col_raw",
    "col_long_raw",
    "col_blob",
    "col_clob",
    "col_nclob",
    "col_uuid",
    "col_json",
    "col_jsonb",
]

DVT_CORE_TYPES_RAW_DATA_TYPES = [
    ("ID", "NUMBER", 9, None, 8, 0, 0),
    ("COL_INT8", "NUMBER", 3, None, 2, 0, 1),
    ("COL_INT16", "NUMBER", 5, None, 4, 0, 1),
    ("COL_INT32", "NUMBER", 10, None, 9, 0, 1),
    ("COL_INT64", "NUMBER", 19, None, 18, 0, 1),
    ("COL_DEC_20", "NUMBER", 21, None, 20, 0, 1),
    ("COL_DEC_38", "NUMBER", 39, None, 38, 0, 1),
    ("COL_DEC_10_2", "NUMBER", 14, None, 10, 2, 1),
    ("COL_FLOAT32", "BINARY_FLOAT", 127, None, None, None, 1),
    ("COL_FLOAT64", "BINARY_DOUBLE", 127, None, None, None, 1),
    ("COL_VARCHAR_30", "VARCHAR", 30, 30, None, None, 1),
    ("COL_CHAR_2", "CHAR", 2, 2, None, None, 1),
    ("COL_STRING", "VARCHAR", 4000, 4000, None, None, 1),
    ("COL_DATE", "DATE", 23, None, None, None, 1),
    ("COL_DATETIME", "TIMESTAMP", 23, None, 0, 3, 1),
    ("COL_TSTZ", "TIMESTAMP_TZ", 23, None, 0, 3, 1),
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
    validator = data_validation.DataValidation(ORACLE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df[consts.TARGET_AGG_VALUE][0]


def mock_get_connection_config(*args):
    if args[1] in ("ora-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN
    elif args[1] == "pg-conn":
        return PG_CONN


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'Y')) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'Y')) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'N')) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'N')) AND (course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'Y')) AND (course_id < 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'Y')) AND (course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'N')) AND (course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'N'))",
    ],
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'Y')) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'Y')) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'N')) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678 OR quarter_id = 5678 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678 OR quarter_id = 5678 AND approved >= 'N')) AND (course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'Y')) AND (course_id < 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'Y')) AND (course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'N')) AND (course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id < 1234 OR quarter_id = 1234 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id > 1234 OR quarter_id = 1234 AND approved >= 'N'))",
    ],
]

# For some reason integers from subqueries are converted to decimals
QUERY_PARTITION_FILTER = [
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'Y')) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'Y')) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'N')) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'N')) AND (course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'Y')) AND (course_id < 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'Y')) AND (course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'N')) AND (course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'N'))",
    ],
    [
        "quarter_id != 1111 AND (course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'Y')) AND (course_id < 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'ALG002  t0.' OR course_id = 'ALG002  t0.' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'Y')) AND (course_id < 'ALG003' OR course_id = 'ALG003' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG003' OR course_id = 'ALG003' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'N')) AND (course_id < 'ALG004' OR course_id = 'ALG004' AND (quarter_id < 5678.0 OR quarter_id = 5678.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'ALG004' OR course_id = 'ALG004' AND (quarter_id > 5678.0 OR quarter_id = 5678.0 AND approved >= 'N')) AND (course_id < 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. Edward''s' OR course_id = 'St. Edward''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'Y')) AND (course_id < 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'Y'))",
        "quarter_id != 1111 AND (course_id > 'St. John''s' OR course_id = 'St. John''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'Y')) AND (course_id < 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Jude''s' OR course_id = 'St. Jude''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'N')) AND (course_id < 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id < 1234.0 OR quarter_id = 1234.0 AND approved < 'N'))",
        "quarter_id != 1111 AND (course_id > 'St. Paul''s' OR course_id = 'St. Paul''s' AND (quarter_id > 1234.0 OR quarter_id = 1234.0 AND approved >= 'N'))",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate partitions on Oracle, first on table, then on custom query"""
    partition_table_test(
        EXPECTED_PARTITION_FILTER,
        pk="course_id,quarter_id,approved",
    )
    partition_query_test(
        QUERY_PARTITION_FILTER,
        tmp_path,
        pk="course_id,quarter_id,approved",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Oracle to Oracle dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # Integer Oracle NUMBERS go to BigQuery INT64.
            "decimal(2,0):int64,decimal(4,0):int64,decimal(9,0):int64,decimal(18,0):int64,"
            # BigQuery does not have a float32 type.
            "float32:float64"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_view_core_types_vw():
    """Oracle to Oracle view dvt_core_types_vw schema validation"""
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
            "-sc=ora-conn",
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
def test_schema_validation_oracle_to_postgres():
    """Oracle to PostgreSQL schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="pg-conn",
        allow_list_file="samples/allow_list/oracle_to_postgres.yaml",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Oracle to Oracle dvt_core_types column validation"""
    cols = ",".join([_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id")])
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
        count_cols=cols,
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=cols,
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_float32")]
    )
    # Excluded col_float64 from std_cols due to stddev_samp inconsistent results. See issue-1540.
    std_cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_float64")
        ]
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=std_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_oracle_to_postgres():
    count_cols = ",".join([_ for _ in ORA2PG_COLUMNS if _ not in ("col_long_raw")])
    # TODO Change min_cols below to include col_interval_ds when issue-1214 is complete.
    # TODO Change min_cols below to include col_json/col_jsonb when issue-1338 is complete.
    sum_cols = ",".join([_ for _ in ORA2PG_COLUMNS if _ not in ("col_long_raw",)])
    min_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _
            not in (
                "col_long_raw",
                "col_interval_ds",
                "col_json",
                "col_jsonb",
            )
        ]
    )
    column_validation_test(
        tc="pg-conn",
        tables="pso_data_validator.dvt_ora2pg_types",
        count_cols=count_cols,
        sum_cols=sum_cols,
        min_cols=min_cols,
        max_cols=min_cols,
        avg_cols=sum_cols,
        std_cols=sum_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery():
    """Oracle to BigQuery dvt_large_decimals column validation."""
    cols = "col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30"
    # Excluded col_dec_38 from std_cols due to stddev_samp inconsistent results. See issue-1540.
    std_cols = "col_dec_18,col_dec_38_9,col_dec_38_30"
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
def test_column_validation_view_core_types_vw():
    """Oracle to Oracle view dvt_core_types_vw column validation"""
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
def test_column_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    # We cannot test sum(col_dt_low) and sum(col_ts_low) on Oracle because there are days
    # missing from October 1582 in the Gregorian calendar which are not reflected in BigQuery
    # or PostgreSQL calendars. This gap is discussed on Wikipedia page for 1582.
    cols = ",".join(DVT_TRICKY_DATES_COLUMNS)
    sum_cols = ",".join(
        _ for _ in DVT_TRICKY_DATES_COLUMNS if _ not in ("col_dt_low", "col_ts_low")
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_tricky_dates",
        min_cols=cols,
        max_cols=cols,
        sum_cols=sum_cols,
        grouped_columns="id",
        wildcard_include_timestamp=True,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Oracle to Oracle dvt_core_types row validation"""
    row_validation_test(
        tc="mock-conn",
        hash="*",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from Oracle defined constraint."""
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
    """Oracle to BigQuery dvt_core_types row validation"""
    # Excluded col_float32,col_float64 due to the lossy nature of BINARY_FLOAT/DOUBLE.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_float64")
        ]
    )
    row_validation_test(
        tc="bq-conn",
        hash=cols,
        use_randow_row=True,
        random_row_batch_size=5,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types():
    """Oracle to Oracle dvt_core_types row validation with --comp-fields"""
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_oracle_to_postgres():
    # TODO Change hash_cols below to include col_nvarchar_30,col_nchar_2 when issue-772 is complete.
    # TODO Change hash_cols below to include col_interval_ds when issue-1214 is complete.
    # TODO Change hash_cols below to include col_clob/col_nclob/col_blob when issue-1364 is complete.
    # TODO Change hash_cols below to include col_json/col_jsonb when issue-1338 is complete.
    # Excluded col_float32,col_float64 due to the lossy nature of BINARY_FLOAT/DOUBLE.
    # Excluded col_long_raw because LONG types are not supported.
    hash_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _
            not in (
                "col_blob",
                "col_clob",
                "col_nclob",
                "col_long_raw",
                "col_float32",
                "col_float64",
                "col_nvarchar_30",
                "col_nchar_2",
                "col_interval_ds",
                "col_json",
                "col_jsonb",
            )
        ]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_ora2pg_types",
        tc="pg-conn",
        hash=hash_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_oracle_to_postgres():
    # TODO Change cols below to include col_num_38 when issue-1454 is complete.
    # TODO Change cols below to include col_json/col_jsonb when issue-1338 is complete.
    # Excluded col_float32,col_float64 due to the lossy nature of BINARY_FLOAT/DOUBLE.
    # Excluded col_long_raw because LONG types are not supported.
    cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _
            not in (
                "col_long_raw",
                "col_float32",
                "col_float64",
                "col_num_38",
                "col_json",
                "col_jsonb",
            )
        ]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_ora2pg_types",
        tc="pg-conn",
        comp_fields=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """Oracle to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    """
    # TODO Uncomment randow row args below when working on issue-1455.
    row_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        hash="id,col_data,col_dec_18,col_dec_38,col_dec_38_9,col_dec_38_30",
        # use_randow_row=True,
        # random_row_batch_size=5,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """Oracle to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=ora-conn",
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
    """Oracle to BigQuery dvt_pangrams column validation.
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
    """Oracle to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=ora-conn",
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
def test_custom_query_column_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types custom-query column validation"""
    custom_query_validation_test(tc="bq-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types custom-query row comparison-fields validation"""
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
    """Oracle to BigQuery dvt_core_types custom-query row hash validation"""
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
def test_custom_query_invalid_long_decimal():
    """Oracle to BigQuery of comparisons of decimals that exceed precision of 18 (int64 & float64).
    We used to have an issue where we would see false success because long numbers would lose precision
    and look the same even if they differed slightly.
    See: https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/900
    This is the regression test.
    """
    # Notice the two numeric values balow have a different final digit, we expect a failure status.
    custom_query_validation_test(
        source_query="select to_number(1234567890123456789012345) as dec_25 from dual",
        target_query="select cast('1234567890123456789012340' as numeric) as dec_25",
        min_cols="dec_25",
        max_cols="dec_25",
        sum_cols="dec_25",
        assert_df_not_empty=True,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_oracle_to_postgres():
    # TODO Change hash_cols below to include col_nvarchar_30,col_nchar_2 when issue-772 is complete.
    # TODO Change hash_cols below to include col_interval_ds when issue-1214 is complete.
    # TODO Change hash_cols below to include col_clob/col_nclob/col_blob when issue-1364 is complete.
    # TODO Change hash_cols below to include col_json/col_jsonb when issue-1338 is complete.
    # Excluded col_float32,col_float64 due to the lossy nature of BINARY_FLOAT/DOUBLE.
    # Excluded col_long_raw because LONG types are not supported.
    hash_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _
            not in (
                "col_blob",
                "col_clob",
                "col_nclob",
                "col_long_raw",
                "col_float32",
                "col_float64",
                "col_nvarchar_30",
                "col_nchar_2",
                "col_interval_ds",
                "col_json",
                "col_jsonb",
            )
        ]
    )
    custom_query_validation_test(
        validation_type="row",
        tc="pg-conn",
        source_query=f"select {hash_cols} from pso_data_validator.dvt_ora2pg_types",
        target_query=f"select {hash_cols} from pso_data_validator.dvt_ora2pg_types",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Oracle to BigQuery test of find-tables command."""
    find_tables_test()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_views_and_tables():
    """Oracle to BigQuery test of find-tables command."""
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
def test_row_validation_many_columns():
    """dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(expected_config_managers=4)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_many_columns():
    """dvt_many_cols row validation using comparison fields"""
    df = row_validation_test(
        tables="pso_data_validator.dvt_many_cols",
        tc="mock-conn",
        comp_fields="*",
        filter_status=None,
    )
    # There should be a result per column per row = 399 for this table.
    assert len(df) == 399


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Oracle dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(
        validation_type="custom-query", expected_config_managers=4
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_multi_table_config_managers():
    """Oracle to BigQuery test of multi-table validation command.

    No need to actually execute the validations to confirm we get the correct number of config managers.
    """
    config_managers = column_validation_test_config_managers(
        tables="pso_data_validator.dvt_core_types,pso_data_validator.dvt_large_decimals",
        count_cols="*",
    )
    assert len(config_managers) == 2
    assert "dvt_core_types" in [_.source_table.lower() for _ in config_managers]
    assert "dvt_large_decimals" in [_.source_table.lower() for _ in config_managers]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_multi_table_all_config_managers():
    """Oracle to PostgreSQL test of multi-table schema.* validation command.

    No need to actually execute the validations to confirm we get the correct number of config managers.
    """
    config_managers = column_validation_test_config_managers(
        tc="pg-conn",
        tables="pso_data_validator.*",
        count_cols="*",
    )
    assert len(config_managers) > 2
    assert "dvt_core_types" in [_.source_table.lower() for _ in config_managers]
    assert "dvt_large_decimals" in [_.source_table.lower() for _ in config_managers]
    assert "dvt_pangrams" in [_.source_table.lower() for _ in config_managers]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_identifiers():
    """Test schema validation on a table with special characters in table and column names."""
    schema_validation_test(
        tables="PSO_DATA_VALIDATOR.DVT-IDENTIFIER$_#",
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
        tables="PSO_DATA_VALIDATOR.DVT-IDENTIFIER$_#",
        count_cols="*",
        filters="COL#HASH IS NOT NULL",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_identifiers():
    """Test row validation on a table with special characters in table and column names."""
    pytest.skip(
        "Skipping test_row_validation_identifiers because concat_all expression does not enquote identifier names, issue-1271."
    )
    row_validation_test(
        tables="pso_data_validator.dvt-identifier$_#",
        tc="mock-conn",
        hash="*",
        filters="id>0 AND col_int8>0",
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
def test_row_validation_hash_bool_to_postgres():
    """Test row validation --hash on a table with bool data types in the target, Oracle does not have a bool type."""
    row_validation_test(
        tables="pso_data_validator.dvt_bool",
        tc="pg-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_bool_to_postgres():
    """Test row validation -comp-fields on a table with bool data types in the target, Oracle does not have a bool type."""
    row_validation_test(
        tables="pso_data_validator.dvt_bool",
        tc="pg-conn",
        comp_fields="col_bool_dec,col_bool_int,col_bool_ch1,col_bool_chy",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_uuid_oracle_to_postgres():
    """Test column validation with UUID columns to PostgreSQL"""
    column_validation_test(
        tc="pg-conn",
        tables="pso_data_validator.dvt_uuid_id",
        count_cols="*",
        sum_cols="*",
        min_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_uuid_hash_oracle_to_postgres():
    """Test row validation with UUID column and primary key to PostgreSQL"""
    row_validation_test(
        tables="pso_data_validator.dvt_uuid_id",
        tc="pg-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_uuid_comp_oracle_to_postgres():
    row_validation_test(
        tables="pso_data_validator.dvt_uuid_id",
        tc="pg-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_uuid_rr_oracle_to_postgres():
    pytest.skip(
        "Skipping test_row_validation_uuid_rr_oracle_to_postgres until we support random-rows on UUIDs: issue-1366."
    )
    row_validation_test(
        tables="pso_data_validator.dvt_uuid_id",
        tc="pg-conn",
        hash="*",
        use_randow_row=True,
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
def test_column_validation_group_by_timestamp():
    """Test that --grouped-columns on Timestamps works correctly.

    DVT casts TIMESTAMP grouped columns to DATE, Oracle DATE includes a time element
    which should be removed in SQL otherwise groups will not match Pandas.
    """
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
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_long_string(capsys):
    """Test data-validation query command with very long string output.

    We don't need to test this for each engine, just one will suffice."""
    raw_query_test(
        capsys,
        query="""SELECT RPAD('some-long-string',256,'x') c FROM dual UNION ALL
                 SELECT RPAD('some-long-string',512,'y') c FROM dual""",
        expected_rows=2,
    )


def test_raw_column_metadata():
    """Test that get_raw_data_types custom Backend method returns expected results."""
    from data_validation import clients

    client = clients.get_data_client(CONN)
    raw_types = list(
        client.raw_column_metadata(
            database="pso_data_validator", table="dvt_core_types"
        )
    )
    assert raw_types == DVT_CORE_TYPES_RAW_DATA_TYPES
