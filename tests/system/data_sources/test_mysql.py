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

import json
import os
from unittest import mock

from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts, exceptions
from data_validation.partition_builder import PartitionBuilder
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    null_not_null_assertions,
    run_test_from_cli_args,
)
from tests.system.data_sources.test_bigquery import BQ_CONN


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


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    " course_id < 'ALG001' OR course_id = 'ALG001' AND ( quarter_id < 3 OR quarter_id = 3 AND student_id < 1234 )",
    " ( course_id > 'ALG001' OR course_id = 'ALG001' AND ( quarter_id > 3 OR quarter_id = 3 AND student_id >= 1234 ) )"
    + " AND ( course_id < 'GEO001' OR course_id = 'GEO001' AND ( quarter_id < 2 OR quarter_id = 2 AND student_id < 5678 ) )",
    " ( course_id > 'GEO001' OR course_id = 'GEO001' AND ( quarter_id > 2 OR quarter_id = 2 AND student_id >= 5678 ) )"
    + " AND ( course_id < 'TRI001' OR course_id = 'TRI001' AND ( quarter_id < 1 OR quarter_id = 1 AND student_id < 9012 ) )",
    " course_id > 'TRI001' OR course_id = 'TRI001' AND ( quarter_id > 1 OR quarter_id = 1 AND student_id >= 9012 )",
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_mysql_generate_table_partitions():
    """Test generate table partitions on mysql
    The unit tests, specifically test_add_partition_filters_to_config and test_store_yaml_partitions_local
    check that yaml configurations are created and saved in local storage. Partitions can only be created with
    a database that can handle SQL with ntile, hence doing this as part of system testing.
    What we are checking
    1. the shape of the partition list is 1, number of partitions (only one table in the list)
    2. value of the partition list matches what we expect.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "generate-table-partitions",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.test_generate_partitions=pso_data_validator.test_generate_partitions",
            "-pk=course_id,quarter_id,student_id",
            "-hash=*",
            "-cdir=/home/users/yaml",
            "-pn=4",
        ]
    )
    config_managers = main.build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()

    assert len(partition_filters) == 1  # only one pair of tables
    assert (
        len(partition_filters[0][0]) == partition_builder.args.partition_num
    )  # assume no of table rows > partition_num
    assert partition_filters[0][0] == EXPECTED_PARTITION_FILTER


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
    main.run_validation(config_managers[0], dry_run=True)
    out, err = capsys.readouterr()
    assert err == ""
    dry_run = json.loads(out)
    assert (
        dry_run["source_query"]
        == "SELECT t0.hash__all, t0.id \nFROM (SELECT t1.id AS id, t1.col_int8 AS col_int8, t1.col_int16 AS col_int16, t1.col_int32 AS col_int32, t1.col_int64 AS col_int64, t1.col_dec_20 AS col_dec_20, t1.col_dec_38 AS col_dec_38, t1.col_dec_10_2 AS col_dec_10_2, t1.col_float32 AS col_float32, t1.col_float64 AS col_float64, t1.col_varchar_30 AS col_varchar_30, t1.col_char_2 AS col_char_2, t1.col_string AS col_string, t1.col_date AS col_date, t1.col_datetime AS col_datetime, t1.col_tstz AS col_tstz, t1.cast__col_string AS cast__col_string, t1.ifnull__cast__col_string AS ifnull__cast__col_string, t1.rstrip__ifnull__cast__col_string AS rstrip__ifnull__cast__col_string, t1.upper__rstrip__ifnull__cast__col_string AS upper__rstrip__ifnull__cast__col_string, t1.concat__all AS concat__all, sha2(t1.concat__all, '256') AS hash__all \nFROM (SELECT t2.id AS id, t2.col_int8 AS col_int8, t2.col_int16 AS col_int16, t2.col_int32 AS col_int32, t2.col_int64 AS col_int64, t2.col_dec_20 AS col_dec_20, t2.col_dec_38 AS col_dec_38, t2.col_dec_10_2 AS col_dec_10_2, t2.col_float32 AS col_float32, t2.col_float64 AS col_float64, t2.col_varchar_30 AS col_varchar_30, t2.col_char_2 AS col_char_2, t2.col_string AS col_string, t2.col_date AS col_date, t2.col_datetime AS col_datetime, t2.col_tstz AS col_tstz, t2.cast__col_string AS cast__col_string, t2.ifnull__cast__col_string AS ifnull__cast__col_string, t2.rstrip__ifnull__cast__col_string AS rstrip__ifnull__cast__col_string, t2.upper__rstrip__ifnull__cast__col_string AS upper__rstrip__ifnull__cast__col_string, concat_ws('', t2.upper__rstrip__ifnull__cast__col_string) AS concat__all \nFROM (SELECT t3.id AS id, t3.col_int8 AS col_int8, t3.col_int16 AS col_int16, t3.col_int32 AS col_int32, t3.col_int64 AS col_int64, t3.col_dec_20 AS col_dec_20, t3.col_dec_38 AS col_dec_38, t3.col_dec_10_2 AS col_dec_10_2, t3.col_float32 AS col_float32, t3.col_float64 AS col_float64, t3.col_varchar_30 AS col_varchar_30, t3.col_char_2 AS col_char_2, t3.col_string AS col_string, t3.col_date AS col_date, t3.col_datetime AS col_datetime, t3.col_tstz AS col_tstz, t3.cast__col_string AS cast__col_string, t3.ifnull__cast__col_string AS ifnull__cast__col_string, t3.rstrip__ifnull__cast__col_string AS rstrip__ifnull__cast__col_string, upper(t3.rstrip__ifnull__cast__col_string) AS upper__rstrip__ifnull__cast__col_string \nFROM (SELECT t4.id AS id, t4.col_int8 AS col_int8, t4.col_int16 AS col_int16, t4.col_int32 AS col_int32, t4.col_int64 AS col_int64, t4.col_dec_20 AS col_dec_20, t4.col_dec_38 AS col_dec_38, t4.col_dec_10_2 AS col_dec_10_2, t4.col_float32 AS col_float32, t4.col_float64 AS col_float64, t4.col_varchar_30 AS col_varchar_30, t4.col_char_2 AS col_char_2, t4.col_string AS col_string, t4.col_date AS col_date, t4.col_datetime AS col_datetime, t4.col_tstz AS col_tstz, t4.cast__col_string AS cast__col_string, t4.ifnull__cast__col_string AS ifnull__cast__col_string, TRIM(TRAILING '\f' FROM TRIM(TRAILING '\u000b' FROM TRIM(TRAILING '\r' FROM TRIM(TRAILING '\n' FROM TRIM(TRAILING '\t' FROM TRIM(TRAILING ' ' FROM (t4.ifnull__cast__col_string))))))) AS rstrip__ifnull__cast__col_string \nFROM (SELECT t5.id AS id, t5.col_int8 AS col_int8, t5.col_int16 AS col_int16, t5.col_int32 AS col_int32, t5.col_int64 AS col_int64, t5.col_dec_20 AS col_dec_20, t5.col_dec_38 AS col_dec_38, t5.col_dec_10_2 AS col_dec_10_2, t5.col_float32 AS col_float32, t5.col_float64 AS col_float64, t5.col_varchar_30 AS col_varchar_30, t5.col_char_2 AS col_char_2, t5.col_string AS col_string, t5.col_date AS col_date, t5.col_datetime AS col_datetime, t5.col_tstz AS col_tstz, t5.cast__col_string AS cast__col_string, coalesce(t5.cast__col_string, 'DEFAULT_REPLACEMENT_STRING') AS ifnull__cast__col_string \nFROM (SELECT t6.id AS id, t6.col_int8 AS col_int8, t6.col_int16 AS col_int16, t6.col_int32 AS col_int32, t6.col_int64 AS col_int64, t6.col_dec_20 AS col_dec_20, t6.col_dec_38 AS col_dec_38, t6.col_dec_10_2 AS col_dec_10_2, t6.col_float32 AS col_float32, t6.col_float64 AS col_float64, t6.col_varchar_30 AS col_varchar_30, t6.col_char_2 AS col_char_2, t6.col_string AS col_string, t6.col_date AS col_date, t6.col_datetime AS col_datetime, t6.col_tstz AS col_tstz, t6.col_string AS cast__col_string \nFROM dvt_core_types AS t6) AS t5) AS t4) AS t3) AS t2) AS t1) AS t0"
    )


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
            "-tbls=pso_data_validator.dvt_core_types",
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
            "-sc=mysql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--exclusion-columns=id",
            "--filter-status=fail",
            (
                # MySQL integers go to BigQuery INT64.
                "--allow-list=int8:int64,int16:int64,int32:int64,"
                # BigQuery does not have a float32 type.
                "float32:float64,"
                # TODO Review this mapping since this column is nullable on MySQL at the moment
                # and should not be identified as not null
                "!timestamp('UTC'):timestamp('UTC')"
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
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--grouped-columns=col_varchar_30",
            "--sum=*",
            "--min=*",
            "--max=*",
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
    # TODO Change --sum, --min and --max options to include col_char_2 when issue-842 is complete.
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mysql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime,col_tstz",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime,col_tstz",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime,col_tstz",
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
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=*",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    # TODO Change --hash string below to include col_float32,col_float64 when issue-841 is complete.
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mysql-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
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
    """MySQL to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
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
        ]
    )
    df = run_test_from_cli_args(args)
    binary_key_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_validation_core_types():
    """MySQL to MySQL dvt_core_types custom-query validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "--source-query=select * from pso_data_validator.dvt_core_types",
            "--target-query=select * from pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--count=*",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0
