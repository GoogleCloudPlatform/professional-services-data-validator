# Copyright 2025 Google LLC
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
    raw_query_rows,
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


SYBASE_HOST = os.getenv("SYBASE_HOST", "127.0.0.1")
SYBASE_USER = os.getenv("SYBASE_USER", "sqlserver")
SYBASE_PASSWORD = os.getenv("SYBASE_PASSWORD")
SYBASE_DATABASE = os.getenv("SYBASE_DATABASE", "master")
PROJECT_ID = os.getenv("PROJECT_ID")
CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SYBASE,
    "host": SYBASE_HOST,
    "user": SYBASE_USER,
    "password": SYBASE_PASSWORD,
    "database": SYBASE_DATABASE,
}


def mock_get_connection_config(*args):
    if args[1] in ("sybase-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Sybase dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # All Sybase integers go to BigQuery INT64.
            "int8:int64,int16:int64,int32:int64,!int32:!int64,"
            # BigQuery does not have a float32 type.
            "float32:float64,"
            # Sybase TIMESTAMP type has scale=7 on Ibis which does not happen in BigQuery.
            # "timestamp(7):timestamp,!timestamp(7):!timestamp,timestamp(7, 'UTC'):timestamp('UTC'),"
        ),
    )
