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

from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts
from data_validation.partition_builder import PartitionBuilder
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    null_not_null_assertions,
    run_test_from_cli_args,
)
from tests.system.data_sources.test_bigquery import BQ_CONN


HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "default")


CONN = {
    "source_type": "Impala",
    "host": HIVE_HOST,
    "port": 10000,
    "database": HIVE_DATABASE,
}


HIVE_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "default",
    consts.CONFIG_TABLE_NAME: "hive_data",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "title",
            consts.CONFIG_TARGET_COLUMN: "title",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}


def test_count_validator():
    validator = data_validation.DataValidation(HIVE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def mock_get_connection_config(*args):
    if args[1] in ("hive-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def disabled_test_schema_validation_core_types():
    """
    Disabled this test in favour of test_schema_validation_core_types_to_bigquery().
    The Hive integration tests are too slow and timing out but I believe
    test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    """
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


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        " ( quarter_id <> 1111 ) AND ( ( `course_id` < 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` < 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` > 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` < 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` > 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` < 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` > 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` < 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` > 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` < 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` > 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` < 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` > 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` < 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` > 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` < 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` > 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` >= 1234 ) ) ) ) )",
    ],
    [
        " ( quarter_id <> 1111 ) AND ( ( `course_id` < 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` < 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` > 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` < 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'ALG001' ) OR ( ( `course_id` = 'ALG001' ) AND ( ( `quarter_id` > 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` < 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` > 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` < 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` > 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` < 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'GEO001' ) OR ( ( `course_id` = 'GEO001' ) AND ( ( `quarter_id` > 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` < 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` > 1 ) OR ( ( `quarter_id` = 1 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` < 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` > 2 ) OR ( ( `quarter_id` = 2 ) AND ( `student_id` >= 1234 ) ) ) ) ) AND ( ( `course_id` < 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` < 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` < 1234 ) ) ) ) )",
        " ( quarter_id <> 1111 ) AND ( ( `course_id` > 'TRI001' ) OR ( ( `course_id` = 'TRI001' ) AND ( ( `quarter_id` > 3 ) OR ( ( `quarter_id` = 3 ) AND ( `student_id` >= 1234 ) ) ) ) )",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_bigquery_generate_table_partitions():
    """Test generate table partitions on BigQuery
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
            "-sc=hive-conn",
            "-tc=hive-conn",
            "-tbls=pso_data_validator.test_generate_partitions=pso_data_validator.test_generate_partitions",
            "-pk=course_id,quarter_id,student_id",
            "-hash=*",
            "-cdir=/home/users/yaml",
            "-pn=9",
            "-parts-per-file=5",
            "-filters=quarter_id != 1111",
        ]
    )
    config_managers = main.build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()
    yaml_configs_list = partition_builder._add_partition_filters(partition_filters)

    assert len(partition_filters) == 1  # only one pair of tables
    # Number of partitions is as requested - assume table rows > partitions requested
    assert len(partition_filters[0][0]) == partition_builder.args.partition_num
    assert partition_filters[0] == EXPECTED_PARTITION_FILTER

    # Next, that the partitions were split into the files correctly
    # 2 files were created with upto 5 validations in each file
    assert len(yaml_configs_list[0]["partitions"]) == 2
    # 5 validations in the first file
    assert len(yaml_configs_list[0]["partitions"][0]["yaml_config"]["validations"]) == 5
    # 4 validations in the second file
    assert len(yaml_configs_list[0]["partitions"][1]["yaml_config"]["validations"]) == 4


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
            "-sc=hive-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--exclusion-columns=id",
            (
                # All Hive integers go to BigQuery INT64.
                "--allow-list=int8:int64,int16:int64,int32:int64,"
                # Hive does not have a time zoned
                "timestamp:timestamp('UTC'),"
                # BigQuery does not have a float32 type.
                "float32:float64"
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
def disabled_test_schema_validation_not_null_vs_nullable():
    """
    Disabled this test because we don't currently pull nullable from Hive.
      https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/934
    Compares a source table with a BigQuery target and ensure we match/fail on nnot null/nullable correctly.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=hive-conn",
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
def disabled_test_column_validation_core_types():
    """
    Disabled this test in favour of test_column_validation_core_types_to_bigquery().
    The Hive integration tests are too slow and timing out but I believe
    test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    """
    parser = cli_tools.configure_arg_parser()
    # Hive tests are really slow so I've excluded --min below assuming that --max is
    # effectively the same test when comparing an engine back to itself.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=*",
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
    # Hive tests are really slow so I've excluded --min below assuming that --max is effectively the same test.
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # TODO Change --sum and --max options to include col_char_2 when issue-842 is complete.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=hive-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filters=id>0 AND col_int8>0",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime,col_tstz",
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
def disabled_test_row_validation_core_types():
    """
    Disabled this test in favour of test_row_validation_core_types_to_bigquery().
    The Hive integration tests are too slow and timing out but I believe
    test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    """
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
    parser = cli_tools.configure_arg_parser()
    # col_float64 is excluded below because there is no way to control the format when casting to string.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=hive-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filters=id>0 AND col_int8>0",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
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
    """Hive to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=hive-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_binary",
            "--primary-keys=binary_id",
            "--hash=int_id,other_data",
            # We have a bug in our test Hive instance that returns
            # zero rows on binary IN lists with >1 element.
            # "--use-random-row",
            # "--random-row-batch-size=5",
        ]
    )
    df = run_test_from_cli_args(args)
    binary_key_assertions(df)
