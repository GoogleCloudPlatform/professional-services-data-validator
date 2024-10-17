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
import pytest
import pathlib
from unittest import mock

from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts, exceptions
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    id_type_test_assertions,
    null_not_null_assertions,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    schema_validation_test,
    row_validation_test,
    column_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import (
    partition_table_test,
    partition_query_test,
)


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "dvt")
CONN = {
    "source_type": "MySQL",
    "host": MYSQL_HOST,
    "user": MYSQL_USER,
    "password": os.getenv("MYSQL_PASSWORD"),
    "port": 3306,
    "database": "pso_data_validator",
    "driver": "pymysql",
}
CONFIG_COUNT_VALID = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
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


def mock_get_connection_config(*args):
    if args[1] in ("mysql-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


def test_mysql_count_invalid_host():
    try:
        data_validator = data_validation.DataValidation(
            CONFIG_COUNT_VALID,
            verbose=False,
        )
        df = data_validator.execute()
        assert df["source_agg_value"][0] == df["target_agg_value"][0]
    except exceptions.DataClientConnectionFailure:
        # Local Testing will not work for MySQL
        pass


# Expected result from partitioning table on 3 keys, 9 partitions
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


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate partitions on MySQL, first on table, then on custom query"""
    partition_table_test(EXPECTED_PARTITION_FILTER)
    partition_query_test(EXPECTED_PARTITION_FILTER, tmp_path)


EXPECTED_DRY_RUN = "SELECT t0.hash__all, t0.id \nFROM (SELECT t1.id AS id, t1.col_int8 AS col_int8, t1.col_int16 AS col_int16, t1.col_int32 AS col_int32, t1.col_int64 AS col_int64, t1.col_dec_20 AS col_dec_20, t1.col_dec_38 AS col_dec_38, t1.col_dec_10_2 AS col_dec_10_2, t1.col_float32 AS col_float32, t1.col_float64 AS col_float64, t1.col_varchar_30 AS col_varchar_30, t1.col_char_2 AS col_char_2, t1.col_string AS col_string, t1.col_date AS col_date, t1.col_datetime AS col_datetime, t1.col_tstz AS col_tstz, t1.cast__col_string AS cast__col_string, t1.ifnull__cast__col_string AS ifnull__cast__col_string, t1.rstrip__ifnull__cast__col_string AS rstrip__ifnull__cast__col_string, t1.concat__all AS concat__all, sha2(t1.concat__all, '256') AS hash__all \nFROM (SELECT t2.id AS id, t2.col_int8 AS col_int8, t2.col_int16 AS col_int16, t2.col_int32 AS col_int32, t2.col_int64 AS col_int64, t2.col_dec_20 AS col_dec_20, t2.col_dec_38 AS col_dec_38, t2.col_dec_10_2 AS col_dec_10_2, t2.col_float32 AS col_float32, t2.col_float64 AS col_float64, t2.col_varchar_30 AS col_varchar_30, t2.col_char_2 AS col_char_2, t2.col_string AS col_string, t2.col_date AS col_date, t2.col_datetime AS col_datetime, t2.col_tstz AS col_tstz, t2.cast__col_string AS cast__col_string, t2.ifnull__cast__col_string AS ifnull__cast__col_string, t2.rstrip__ifnull__cast__col_string AS rstrip__ifnull__cast__col_string, concat_ws('', t2.rstrip__ifnull__cast__col_string) AS concat__all \nFROM (SELECT t3.id AS id, t3.col_int8 AS col_int8, t3.col_int16 AS col_int16, t3.col_int32 AS col_int32, t3.col_int64 AS col_int64, t3.col_dec_20 AS col_dec_20, t3.col_dec_38 AS col_dec_38, t3.col_dec_10_2 AS col_dec_10_2, t3.col_float32 AS col_float32, t3.col_float64 AS col_float64, t3.col_varchar_30 AS col_varchar_30, t3.col_char_2 AS col_char_2, t3.col_string AS col_string, t3.col_date AS col_date, t3.col_datetime AS col_datetime, t3.col_tstz AS col_tstz, t3.cast__col_string AS cast__col_string, t3.ifnull__cast__col_string AS ifnull__cast__col_string, TRIM(TRAILING '\x0c' FROM TRIM(TRAILING '\x0b' FROM TRIM(TRAILING '\r' FROM TRIM(TRAILING '\n' FROM TRIM(TRAILING '\t' FROM TRIM(TRAILING ' ' FROM (t3.ifnull__cast__col_string))))))) AS rstrip__ifnull__cast__col_string \nFROM (SELECT t4.id AS id, t4.col_int8 AS col_int8, t4.col_int16 AS col_int16, t4.col_int32 AS col_int32, t4.col_int64 AS col_int64, t4.col_dec_20 AS col_dec_20, t4.col_dec_38 AS col_dec_38, t4.col_dec_10_2 AS col_dec_10_2, t4.col_float32 AS col_float32, t4.col_float64 AS col_float64, t4.col_varchar_30 AS col_varchar_30, t4.col_char_2 AS col_char_2, t4.col_string AS col_string, t4.col_date AS col_date, t4.col_datetime AS col_datetime, t4.col_tstz AS col_tstz, t4.cast__col_string AS cast__col_string, coalesce(t4.cast__col_string, 'DEFAULT_REPLACEMENT_STRING') AS ifnull__cast__col_string \nFROM (SELECT t5.id AS id, t5.col_int8 AS col_int8, t5.col_int16 AS col_int16, t5.col_int32 AS col_int32, t5.col_int64 AS col_int64, t5.col_dec_20 AS col_dec_20, t5.col_dec_38 AS col_dec_38, t5.col_dec_10_2 AS col_dec_10_2, t5.col_float32 AS col_float32, t5.col_float64 AS col_float64, t5.col_varchar_30 AS col_varchar_30, t5.col_char_2 AS col_char_2, t5.col_string AS col_string, t5.col_date AS col_date, t5.col_datetime AS col_datetime, t5.col_tstz AS col_tstz, t5.col_string AS cast__col_string \nFROM dvt_core_types AS t5) AS t4) AS t3) AS t2) AS t1) AS t0"


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_mysql_dry_run(capsys):
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--hash=col_string",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    with mock.patch("json.dumps") as mock_dump:
        main.run_validation(config_managers[0], dry_run=True)
        actual = mock_dump.call_args.args[0]["source_query"]
        out, err = capsys.readouterr()
        assert err == ""
        assert actual == EXPECTED_DRY_RUN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """MySQL to MySQL dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """MySQL to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # MySQL integers go to BigQuery INT64.
            "int8:int64,int16:int64,int32:int64,"
            # BigQuery does not have a float32 type.
            "float32:float64,"
            # TODO Review this mapping since this column is nullable on MySQL at the moment
            # and should not be identified as not null
            "!timestamp('UTC'):timestamp('UTC')"
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
            "-sc=mysql-conn",
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
    """MySQL to MySQL dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
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
    """MySQL to BigQuery dvt_core_types column validation"""
    # TODO Change --sum, --min and --max options to include col_char_2 when issue-842 is complete.
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime,col_tstz"
    column_validation_test(
        tc="bq-conn",
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
def test_row_validation_core_types():
    """MySQL to MySQL dvt_core_types row validation"""
    row_validation_test(
        tc="mock-conn",
        hash="*",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """MySQL to BigQuery dvt_core_types row validation"""
    row_validation_test(
        tc="bq-conn",
        # TODO Change --hash string below to include col_float32,col_float64 when issue-841 is complete.
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """MySQL to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mysql-conn",
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
    """MySQL to BigQuery dvt_char_id row validation.
    This is testing CHAR primary key join columns.
    Includes random row filter test.

    Note that this test will fail unless PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled.
    """
    pytest.skip(
        "Skipping test_row_validation_char_pk_to_bigquery because PAD_CHAR_TO_FULL_LENGTH SQL mode needs to be enabled."
    )
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mysql-conn",
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
    """MySQL to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.

    Note, we are skipping this test because unicode characters are being converted to standard ascii.
    """
    pytest.skip(
        "Skipping test_row_validation_pangrams_to_bigquery because failing on MySQL."
    )
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mysql-conn",
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
    """MySQL to MySQL dvt_core_types custom-query validation"""
    custom_query_validation_test(tc="mock-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """MySQL dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """MySQL dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(validation_type="custom-query")
