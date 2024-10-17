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

from data_validation import cli_tools
from tests.system.data_sources.common_functions import (
    schema_validation_test,
    column_validation_test,
    run_test_from_cli_args,
    null_not_null_assertions,
    row_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN


DB2_HOST = os.getenv("DB2_HOST", "localhost")
DB2_PASSWORD = os.getenv("DB2_PASSWORD")

CONN = {
    "source_type": "DB2",
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
    """DB2 to DB2 dvt_core_types schema validation"""
    schema_validation_test(
        tables="db2inst1.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="db2inst1.dvt_core_types=pso_data_validator.dvt_core_types",
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
def test_schema_validation_not_null_vs_nullable():
    """Compares a source table with a BigQuery target and ensure we match/fail on not null/nullable correctly."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=db2-conn",
            "-tc=bq-conn",
            "-tbls=db2inst1.dvt_null_not_null=pso_data_validator.dvt_null_not_null",
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
        tables="db2inst1.dvt_core_types",
        count_cols=cols,
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        filters="id>0 AND col_int8>0",
        # TODO When issue-1295 is complete uncomment --grouped_columns parameter below.
        # grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # Excluded col_tstz since it is not possible to set time zone at this column on DB2
    cols = "col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime"
    column_validation_test(
        tc="bq-conn",
        tables="db2inst1.dvt_core_types=pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """DB2 to DB2 dvt_core_types row validation"""
    row_validation_test(
        tables="db2inst1.dvt_core_types",
        tc="mock-conn",
        # OBS: Only passing this column because SYSIBM.HEX function accepts a max length of 16336 bytes (https://www.ibm.com/docs/en/db2/11.5?topic=functions-hex)
        hash="col_string",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types row validation"""
    row_validation_test(
        tables="db2inst1.dvt_core_types=pso_data_validator.dvt_core_types",
        tc="bq-conn",
        # OBS: Only passing this column because SYSIBM.HEX function accepts a max length of 16336 bytes (https://www.ibm.com/docs/en/db2/11.5?topic=functions-hex)
        # TODO: When issue-1296 is complete change to col_date,col_datetime,col_tstz instead
        hash="col_string",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types custom-query column validation"""
    custom_query_validation_test(
        source_query="select * from db2inst1.dvt_core_types", count_cols="*"
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """DB2 to BigQuery dvt_core_types custom-query row comparison-fields validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,col_varchar_30 from db2inst1.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30 from pso_data_validator.dvt_core_types",
        comp_fields="col_int64,col_varchar_30",
    )
