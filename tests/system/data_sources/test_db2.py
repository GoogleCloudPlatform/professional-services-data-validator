# Copyright 2024 Google LLC
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

from data_validation import cli_tools, consts
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    find_tables_test,
    schema_validation_test,
    column_validation_test,
    run_test_from_cli_args,
    null_not_null_assertions,
    row_validation_test,
    custom_query_validation_test,
    raw_query_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN


# Our Db2 test infra has a habit of failing to connect but then working on retry.
pytestmark = pytest.mark.flaky(
    reruns=1,
    reruns_delay=1,
    only_rerun=["DataClientConnectionFailure"],
)

DB2_HOST = os.getenv("DB2_HOST", "localhost")
DB2_PASSWORD = os.getenv("DB2_PASSWORD")

CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_DB2,
    "host": DB2_HOST,
    "user": "db2inst1",
    "password": DB2_PASSWORD,
    "port": 50000,
    "database": "testdb",
}


def mock_get_connection_config(*args):
    if args[1] in ("db2-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Db2 to Db2 dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Db2 to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # SMALLINT, INTEGER is equivalent to BigQuery INT64.
            "int16:int64,int32:int64,"
            # BigQuery does not have decimal, float32 types.
            "decimal:float64,float32:float64,"
            # Unable to create col_tstz with time zone on our DB2 database therefore test data is adjusted.
            "timestamp:timestamp('UTC'),"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_db2_types_to_bigquery():
    """Db2 to BigQuery dvt_db2_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_db2_types",
        tc="bq-conn",
        allow_list=("int16:int64,int32:int64," "decimal:decimal(38,9)"),
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
            "-sc=db2-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_null_not_null",
        ]
    )
    df = run_test_from_cli_args(args)
    null_not_null_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """DB2 to DB2 dvt_core_types column validation"""
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
    """Db2 to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # Excluded col_tstz since it is not possible to set time zone at this column on Db2
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime"
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
def test_column_validation_db2_types_to_bigquery():
    """Db2 to BigQuery dvt_db2_types column validation"""
    cols = "*"
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_db2_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=cols,
        wildcard_include_timestamp=True,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery():
    """Db2 to BigQuery dvt_large_decimals column validation.

    Only includes decimal(18) columns due to Db2 maximum precision for DECIMAL of 31 digits.
    """
    cols = "id,col_dec_18"
    column_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        count_cols=cols,
        min_cols=cols,
        sum_cols=cols,
        avg_cols=cols,
        std_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_large_decimals_to_bigquery_mismatch():
    """Db2 to BigQuery dvt_large_decimals column validation on columns we expect to have a mismatch.

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
def test_row_validation_core_types():
    """Db2 to Db2 dvt_core_types row validation"""
    # Exclude col_string because it is unbound and causes overflow error for HEX function.
    # TODO: When issue-1638 is complete remove col_char_2 from exclusion list below.
    # TODO: When issue-1634 is complete remove columns tagged with issue-1634 from exclusion list below.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _
            not in (
                "id",
                "col_int8",  # issue-1634
                "col_int16",  # issue-1634
                "col_int32",  # issue-1634
                "col_dec_20",  # issue-1634
                "col_dec_38",  # issue-1634
                "col_dec_10_2",  # issue-1634
                "col_float32",  # issue-1634
                "col_float64",  # issue-1634
                "col_char_2",
                "col_string",
            )
        ]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        hash=cols,
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from DB2 defined constraint."""
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        hash="col_string",
        primary_keys=None,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Db2 to BigQuery dvt_core_types row validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    # Exclude col_string because it is unbound and causes overflow error for HEX function.
    # TODO: When issue-1638 is complete remove col_char_2 from exclusion list below.
    # TODO: When issue-1634 is complete remove columns tagged with issue-1634 from exclusion list below.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _
            not in (
                "id",
                "col_int8",  # issue-1634
                "col_int16",  # issue-1634
                "col_int32",  # issue-1634
                "col_dec_20",  # issue-1634
                "col_dec_38",  # issue-1634
                "col_dec_10_2",  # issue-1634
                "col_float32",
                "col_float64",  # issue-1634
                "col_char_2",
                "col_string",
                "col_tstz",
            )
        ]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        hash=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_db2_types_to_bigquery():
    """Db2 to BigQuery dvt_db2_types row validation"""
    # Excluded col_clob,col_nclob,col_xml because they are incompatible with hex() function (due to potential length).
    # TODO: When issue-1296 is complete change col to "*" below.
    # TODO Add col_blob to list below once issue-1354 is complete.
    # TODO Add col_char_2 to list below once issue-1354 is complete.
    # TODO Add col_char_bit,col_varchar_bit to list below once issue-1655 is complete.
    cols = "col_nvarchar_30,col_time"
    row_validation_test(
        tables="pso_data_validator.dvt_db2_types",
        tc="bq-conn",
        hash=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_large_decimals_to_bigquery():
    """Db2 to BigQuery dvt_large_decimals row validation.
    See https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/956
    This is testing large decimals for the primary key join column plus the hash columns.
    Only includes decimal(18) columns due to Db2 maximum precision for DECIMAL of 31 digits.
    """
    # Add id,col_data into hash value below once issue-1634 has been fixed.
    row_validation_test(
        tables="pso_data_validator.dvt_large_decimals",
        tc="bq-conn",
        hash="col_dec_18",
        use_randow_row=True,
        random_row_batch_size=5,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types custom-query column validation"""
    custom_query_validation_test(
        source_query="select * from pso_data_validator.dvt_core_types", count_cols="*"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types custom-query row comparison-fields validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,col_varchar_30 from pso_data_validator.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30 from pso_data_validator.dvt_core_types",
        comp_fields="col_int64,col_varchar_30",
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
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys, table="pso_data_validator.dvt_core_types")
