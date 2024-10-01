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

from data_validation import cli_tools, data_validation, consts, find_tables
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    column_validation_test,
    column_validation_test_config_managers,
    find_tables_assertions,
    id_type_test_assertions,
    null_not_null_assertions,
    row_validation_many_columns_test,
    run_test_from_cli_args,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import generate_partitions_test


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
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=dvt_core_types",
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
    """DB2 to BigQuery dvt_core_types schema validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=db2-conn",
            "-tc=bq-conn",
            "-tbls=dvt_core_types=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--exclusion-columns=id",
            # (
            #     # Integer Oracle NUMBERS go to BigQuery INT64.
            #     "--allow-list=decimal(2,0):int64,decimal(4,0):int64,decimal(9,0):int64,decimal(18,0):int64,"
            #     # BigQuery does not have a float32 type.
            #     "float32:float64"
            # ),
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0