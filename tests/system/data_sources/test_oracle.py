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

from data_validation import cli_tools, data_validation, consts, find_tables
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    column_validation_test,
    column_validation_test_config_managers,
    find_tables_assertions,
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
    partition_table_test,
    partition_query_test,
)


ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DATABASE = os.getenv("ORACLE_DATABASE", "XEPDB1")

CONN = {
    "source_type": "Oracle",
    "host": ORACLE_HOST,
    "user": "SYSTEM",
    "password": ORACLE_PASSWORD,
    "port": 1521,
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
    consts.CONFIG_FORMAT: "table",
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
    "col_json",
    "col_jsonb",
]


def test_count_validator():
    validator = data_validation.DataValidation(ORACLE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


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
        " quarter_id <> 1111 AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) )",
    ],
    [
        " quarter_id <> 1111 AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 1234 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 1234 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) )",
    ],
]

QUERY_PARTITION_FILTER = [
    [
        " quarter_id <> 1111 AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 2.0 OR quarter_id = 2.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 2.0 OR quarter_id = 2.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 3.0 OR quarter_id = 3.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 3.0 OR quarter_id = 3.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 1.0 OR quarter_id = 1.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 1.0 OR quarter_id = 1.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 2.0 OR quarter_id = 2.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 2.0 OR quarter_id = 2.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 3.0 OR quarter_id = 3.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 3.0 OR quarter_id = 3.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 1.0 OR quarter_id = 1.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 1.0 OR quarter_id = 1.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 2.0 OR quarter_id = 2.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 2.0 OR quarter_id = 2.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 3.0 OR quarter_id = 3.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 3.0 OR quarter_id = 3.0 AND student_id >= 1234.0 ) )",
    ],
    [
        " quarter_id <> 1111 AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 2.0 OR quarter_id = 2.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 2.0 OR quarter_id = 2.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 3.0 OR quarter_id = 3.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 3.0 OR quarter_id = 3.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 1.0 OR quarter_id = 1.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 1.0 OR quarter_id = 1.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 2.0 OR quarter_id = 2.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 2.0 OR quarter_id = 2.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 3.0 OR quarter_id = 3.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 3.0 OR quarter_id = 3.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 1.0 OR quarter_id = 1.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 1.0 OR quarter_id = 1.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 2.0 OR quarter_id = 2.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 2.0 OR quarter_id = 2.0 AND student_id >= 1234.0 ) ) AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 3.0 OR quarter_id = 3.0 AND student_id < 1234.0 ) )",
        " quarter_id <> 1111 AND ( course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 3.0 OR quarter_id = 3.0 AND student_id >= 1234.0 ) )",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate partitions on Oracle, first on table, then on custom query"""
    partition_table_test(EXPECTED_PARTITION_FILTER)
    partition_query_test(QUERY_PARTITION_FILTER, tmp_path)


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
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz"
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
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
def test_column_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
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
def test_column_validation_oracle_to_postgres():
    count_cols = ",".join([_ for _ in ORA2PG_COLUMNS if _ not in ("col_long_raw")])
    # TODO Change sum_cols and min_cols to include col_char_2,col_nchar_2 when issue-842 is complete.
    # TODO Change sum_cols to include col_num_18 when issue-1007 is complete.
    sum_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _ not in ("col_char_2", "col_nchar_2", "col_num_18", "col_long_raw")
        ]
    )
    min_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _ not in ("col_char_2", "col_nchar_2", "col_long_raw")
        ]
    )
    column_validation_test(
        tc="pg-conn",
        tables="pso_data_validator.dvt_ora2pg_types",
        count_cols=count_cols,
        sum_cols=sum_cols,
        min_cols=min_cols,
        max_cols=min_cols,
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
    row_validation_test(
        tc="bq-conn",
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
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
    # TODO Change hash_cols below to include col_tstz when issue-706 is complete.
    # TODO col_raw/col_long_raw are blocked by issue-773 (is it even reasonable to expect binary columns to work here?)
    # TODO Change hash_cols below to include col_nvarchar_30,col_nchar_2 when issue-772 is complete.
    # TODO Change hash_cols below to include col_interval_ds when issue-1214 is complete.
    # TODO Change hash_cols below to include col_clob/col_nclob/col_blob/col_json/col_jsonb when issue-1364 is complete.
    # Excluded col_float32,col_float64 due to the lossy nature of BINARY_FLOAT/DOUBLE.
    hash_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _
            not in (
                "col_blob",
                "col_clob",
                "col_nclob",
                "col_raw",
                "col_long_raw",
                "col_float32",
                "col_float64",
                "col_tstz",
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
def test_row_validation_large_decimals_to_bigquery():
    """Oracle to BigQuery dvt_large_decimals row validation.
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
def test_row_validation_string_pk_to_bigquery():
    """Oracle to BigQuery dvt_string_id row validation.
    This is testing string primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=ora-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_string_id",
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
    """Oracle to BigQuery dvt_char_id row validation.
    This is testing CHAR primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=ora-conn",
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
    # TODO Change hash_cols below to include col_tstz when issue-706 is complete.
    # TODO col_raw/col_long_raw are blocked by issue-773 (is it even reasonable to expect binary columns to work here?)
    # TODO Change hash_cols below to include col_nvarchar_30,col_nchar_2 when issue-772 is complete.
    # TODO Change hash_cols below to include col_interval_ds when issue-1214 is complete.
    # TODO Change hash_cols below to include col_clob/col_nclob/col_blob/col_json/col_jsonb when issue-1364 is complete.
    # Excluded col_float32,col_float64 due to the lossy nature of BINARY_FLOAT/DOUBLE.
    hash_cols = ",".join(
        [
            _
            for _ in ORA2PG_COLUMNS
            if _
            not in (
                "col_blob",
                "col_clob",
                "col_nclob",
                "col_raw",
                "col_long_raw",
                "col_float32",
                "col_float64",
                "col_tstz",
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
    """Oracle dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(expected_config_managers=4)


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
