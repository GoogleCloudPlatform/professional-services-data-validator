# Copyright 2026 Google LLC
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

import logging
import os
from unittest import mock

import pytest
import pathlib

from data_validation import (
    cli_tools,
    clients,
    consts,
    data_validation,
)
from data_validation.result_handlers.base_backend import RH_WRITE_MESSAGE
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    DVT_TRICKY_DATES_COLUMNS,
    binary_key_assertions,
    column_validation_test,
    column_validation_test_args,
    connections_add_test,
    custom_query_validation_test,
    find_tables_test,
    generate_and_run_table_partitions_test,
    id_column_row_validation_test,
    id_column_query_row_validation_test,
    id_type_test_assertions,
    null_not_null_assertions,
    partition_table_test,
    partition_query_test,
    raw_query_rows,
    raw_query_test,
    row_validation_test,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    schema_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN

# TODO Add PGAdapter to ci.
# Local testing requires the PGAdapter.
# https://docs.cloud.google.com/spanner/docs/pgadapter-start#sidecar-proxy

# PGAdapter listens to localhost
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PROJECT_ID = os.getenv("PROJECT_ID")

CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SPANNER_POSTGRES,
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
}


def mock_get_connection_config(*args):
    if args[1] in ("pg-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


##########################
# SCHEMA VALIDATION TESTS
##########################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Spanner PostgreSQL to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # BigQuery does not have a float32 type.
            "float32:float64,"
            # Spanner PostgreSQL does not support timezone-unaware timestamps.
            "timestamp('UTC'):timestamp,"
            # Spanner does not allow decimal(p,s).
            "decimal(38,9):decimal(20,0),decimal(38,9):decimal(38,0),decimal(38,9):decimal(10,2)"
        ),
    )


##########################
# COLUMN VALIDATION TESTS
##########################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Spanner PostgreSQL to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_float32")]
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=cols,
        wildcard_include_timestamp=True,
        wildcard_include_string=True,
    )
