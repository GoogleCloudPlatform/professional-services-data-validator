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
    find_tables_test,
    id_type_test_assertions,
    null_not_null_assertions,
    row_validation_many_columns_test,
    row_validation_test,
    run_test_from_cli_args,
    schema_validation_test,
    custom_query_validation_test,
    column_validation_test,
    raw_query_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import (
    partition_table_test,
    partition_query_test,
)

HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "default")


CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_IMPALA,
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
    consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
    consts.CONFIG_FILTER_STATUS: None,
}


def test_count_validator():
    validator = data_validation.DataValidation(HIVE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df[consts.TARGET_AGG_VALUE][0]


def mock_get_connection_config(*args):
    if args[1] in ("hive-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """
    Disabled this test in favour of test_schema_validation_core_types_to_bigquery().
    The Hive integration tests are too slow and timing out but I believe
    test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    """
    pytest.skip(
        "Skipping test_schema_validation_core_types in favour of test_schema_validation_core_types_to_bigquery (due to elapsed time)."
    )
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        "(quarter_id != 1111) AND ((`course_id` < 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Edwards') OR ((`course_id` = 'St. Edwards') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Edwards') OR ((`course_id` = 'St. Edwards') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. Johns') OR ((`course_id` = 'St. Johns') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Johns') OR ((`course_id` = 'St. Johns') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. Judes') OR ((`course_id` = 'St. Judes') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Judes') OR ((`course_id` = 'St. Judes') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Pauls') OR ((`course_id` = 'St. Pauls') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Pauls') OR ((`course_id` = 'St. Pauls') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE)))))))))",
    ],
    [
        "(quarter_id != 1111) AND ((`course_id` < 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Edwards') OR ((`course_id` = 'St. Edwards') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Edwards') OR ((`course_id` = 'St. Edwards') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. Johns') OR ((`course_id` = 'St. Johns') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Johns') OR ((`course_id` = 'St. Johns') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-26T16:00:00') OR ((`recd_timestamp` = '2023-08-26T16:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. Judes') OR ((`course_id` = 'St. Judes') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Judes') OR ((`course_id` = 'St. Judes') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Pauls') OR ((`course_id` = 'St. Pauls') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Pauls') OR ((`course_id` = 'St. Pauls') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > '2023-08-27T15:00:00') OR ((`recd_timestamp` = '2023-08-27T15:00:00') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE)))))))))",
    ],
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate partitions on Hive, first on table, then on custom query"""
    partition_table_test(
        EXPECTED_PARTITION_FILTER,
    )
    partition_query_test(
        EXPECTED_PARTITION_FILTER,
        tmp_path,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    schema_validation_test(
        tc="bq-conn",
        allow_list=(
            # All Hive integers go to BigQuery INT64.
            "int8:int64,int16:int64,int32:int64,"
            # Hive does not have a time zoned timestamp.
            "timestamp:timestamp('UTC'),"
            # BigQuery does not have a float32 type.
            "float32:float64"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_not_null_vs_nullable():
    """
    Disabled this test because we don't currently pull nullable from Hive.
      https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/934
    Compares a source table with a BigQuery target and ensure we match/fail on not null/nullable correctly.
    """
    pytest.skip(
        "Skipping test_schema_validation_not_null_vs_nullable because we don't currently pull nullable from Hive."
    )
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
def test_schema_validation_bool():
    """Hive to Hive dvt_bool schema validation"""
    schema_validation_test(tables="pso_data_validator.dvt_bool", tc="mock-conn")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """
    Disabled this test in favour of test_column_validation_core_types_to_bigquery().
    The Hive integration tests are too slow and timing out but I believe
    test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    """
    pytest.skip(
        "Skipping test_column_validation_core_types in favour of test_column_validation_core_types_to_bigquery (due to elapsed time)."
    )
    # Hive tests are really slow so I've excluded --min below assuming that --max is
    # effectively the same test when comparing an engine back to itself.
    column_validation_test(
        tc="mock-conn",
        sum_cols="*",
        max_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    # Hive tests are really slow so I've excluded --min below assuming that --max is effectively the same test.
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # Excluded decimals larger than 64bit int/float because HiveQL stddev_samp returns nan.
    # TODO Change --sum and --max options to include col_char_2 when issue-1514 is complete.
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime,col_tstz"
    # Excluded col_float64 from std_cols due to stddev_samp inconsistent results. See issue-1540.
    std_cols = "col_int8,col_int16,col_int32,col_int64,col_dec_10_2"
    column_validation_test(
        tc="bq-conn",
        filters="id>0 AND col_int8>0",
        sum_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=std_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """
    Disabled this test in favour of test_row_validation_core_types_to_bigquery().
    The Hive integration tests are too slow and timing out but I believe
    test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    """
    pytest.skip(
        "Skipping test_row_validation_core_types in favour of test_row_validation_core_types_to_bigquery (due to elapsed time)."
    )
    row_validation_test(
        tc="mock-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    # col_float64 is excluded below because there is no way to control the format when casting to string.
    row_validation_test(
        tc="bq-conn",
        filters="id>0 AND col_int8>0",
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
    )


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


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pangrams_to_bigquery():
    """Hive to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=hive-conn",
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
def test_custom_query_validation_core_types_to_bigquery():
    """Hive to BigQuery dvt_core_types custom-query validation
    Using BigQuery target because Hive queries are really slow."""
    custom_query_validation_test(tc="bq-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """Hive dvt_many_cols row validation.
    Using BigQuery target because Hive queries are really slow.
    When executed individually this test passes but when executed during a full
    integration test run I get:
      impala.error.HiveServer2Error: java.lang.NullPointerException
    This must be down to minimal resources for our small Hive instance, disabling for now.
    """
    pytest.skip("Skipping test_row_validation_many_columns due to resource issues.")
    # TODO Enable this test once we have access to a less flakey Hive cluster.
    row_validation_many_columns_test(target_conn="bq-conn")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Hive dvt_many_cols custom-query row validation.
    Using BigQuery target because Hive queries are really slow.
    I can't get this test to complete on our test infrastructure due to:
      java.lang.OutOfMemoryError: Java heap space
    """
    pytest.skip("Skipping test_row_validation_many_columns due to resource issues.")
    # TODO Enable this test once we have access to a less flakey Hive cluster.
    row_validation_many_columns_test(
        validation_type="custom-query",
        target_conn="bq-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_hash_bool_to_bigquery():
    """Test row validation on a table with bool data types."""
    row_validation_test(
        tables="pso_data_validator.dvt_bool",
        tc="bq-conn",
        hash="*",
    )


@pytest.mark.slow
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31.
    This test is marked as slow and will not run unless --run-slow is included.
    It is duplicated in our Impala testing which mostly tests the same code path,
    but not the same SQL engine.
    """
    # We cannot test col_dt_low because of an issue in early versions of Hive:
    # https://docs.cloudera.com/runtime/7.3.1/impala-sql-reference/topics/impala-date.html
    cols = ",".join(_ for _ in DVT_TRICKY_DATES_COLUMNS if _ != "col_dt_low")
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        hash=cols,
    )


@pytest.mark.slow
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31.
    This test is marked as slow and will not run unless --run-slow is included.
    It is duplicated in our Impala testing which mostly tests the same code path,
    but not the same SQL engine.
    """
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        comp_fields=",".join(DVT_TRICKY_DATES_COLUMNS),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Hive to BigQuery test of find-tables command."""
    # check_for_view=False because there is no practical way to exclude views on Hive.
    find_tables_test(check_for_view=False)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys)
