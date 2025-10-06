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
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    partition_table_test,
    partition_query_test,
)


SYBASE_HOST = os.getenv("SYBASE_HOST", "127.0.0.1")
SYBASE_USER = os.getenv("SYBASE_USER", "sqlserver")
SYBASE_PASSWORD = os.getenv("SYBASE_PASSWORD")
SYBASE_DATABASE = os.getenv("SYBASE_DATABASE", "master")
SYBASE_ODBC_DRIVER = os.getenv("SYBASE_ODBC_DRIVER", "FreeTDS")
PROJECT_ID = os.getenv("PROJECT_ID")
CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SYBASE,
    "host": SYBASE_HOST,
    "user": SYBASE_USER,
    "password": SYBASE_PASSWORD,
    "database": SYBASE_DATABASE,
    "odbc_driver": SYBASE_ODBC_DRIVER,
    "query": '{"autocommit": "True"}',
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
            # Sybase does not have a time zoned timestamp.
            "timestamp:timestamp('UTC'),"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_view_core_types_vw():
    """Sybase view dvt_core_types_vw schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types_vw",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Sybase dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
        avg_cols="*",
        std_cols="*",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have a float32 type.
    # TODO Change cols to include col_char_2 when issue-1514 is complete.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_char_2")
        ]
    )
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
def test_column_validation_view_core_types_vw():
    """Sybase view dvt_core_types_vw column validation"""
    cols = ",".join([_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id")])
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types_vw",
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
def test_row_validation_core_types():
    """Sybase dvt_core_types concat row validation.

    Sybase does not have a SHA256 hash function."""
    # TODO Add col_string to cols below when issue-xyz is actioned.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_string")]
    )
    row_validation_test(
        tc="mock-conn",
        concat=cols,
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_auto_pks():
    """Test auto population of -pks from Sybase defined constraint."""
    row_validation_test(
        tc="mock-conn",
        concat="col_int8,col_int16",
        primary_keys=None,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types concat row validation.

    Sybase does not have a SHA256 hash function."""
    # TODO Add col_string to cols below when issue-xyz is actioned.
    # TODO Add col_dec_10_2 to cols below when issue-xyz is actioned.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_string", "col_dec_10_2")
        ]
    )
    row_validation_test(
        tc="bq-conn",
        concat=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types_to_bigquery():
    """Sybase to BigQuery extended data type row validation using comparison fields"""
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _
            not in (
                "id",
                # Excluded col_float32 because BigQuery does not have a float32 type.
                "col_float32",
            )
        ]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        comp_fields=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types custom-query column validation"""
    custom_query_validation_test(tc="bq-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types custom-query row comparison-fields validation"""
    custom_query_validation_test(
        validation_type="row",
        source_query="select id,col_int64,COL_VARCHAR_30,col_date from pso_data_validator.dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        concat="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_comp_fields_core_types_to_bigquery():
    """Sybase to BigQuery dvt_core_types custom-query row comparison-fields validation"""
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
def test_find_tables():
    """Sybase to BigQuery test of find-tables command."""
    find_tables_test()
