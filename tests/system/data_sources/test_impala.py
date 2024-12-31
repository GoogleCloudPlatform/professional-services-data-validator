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
    binary_key_assertions,
    column_validation_test,
    column_validation_test_config_managers,
    find_tables_test,
    id_type_test_assertions,
    null_not_null_assertions,
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


IMPALA_HOST = os.getenv("IMPALA_HOST", "localhost")
IMPALA_PORT = os.getenv("IMPALA_PORT", "21050")
IMPALA_DATABASE = os.getenv("IMPALA_DATABASE", "default")
IMPALA_AUTH_MECH = os.getenv("IMPALA_AUTH_MECH", "NOSASL")


CONN = {
    "source_type": "Impala",
    "host": IMPALA_HOST,
    "port": IMPALA_PORT,
    "database": IMPALA_DATABASE,
    "auth_mechanism": IMPALA_AUTH_MECH,
}


def mock_get_connection_config(*args):
    if args[1] in ("impala-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Impala to Impala dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Impala to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # Impala integers go to BigQuery INT64.
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
def test_schema_validation_view_core_types_vw():
    """Impala to Impala view dvt_core_types_vw schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types_vw",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_bool():
    """Impala to Impala dvt_bool schema validation"""
    schema_validation_test(tables="pso_data_validator.dvt_bool", tc="mock-conn")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    # """
    # Disabled this test in favour of test_column_validation_core_types_to_bigquery().
    # The Hive integration tests are too slow and timing out but I believe
    # test_column_validation_core_types_to_bigquery() will cover off most of what this test does.
    # """
    # pytest.skip(
    #    "Skipping test_column_validation_core_types in favour of test_column_validation_core_types_to_bigquery (due to elapsed time)."
    # )
    column_validation_test(
        tc="mock-conn",
        count_cols="*",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Impala to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_float32")]
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_view_core_types_vw():
    """Impala to Impala view dvt_core_types_vw column validation"""
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
    """Impala to Impala dvt_core_types row validation"""
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
    """Impala to BigQuery dvt_core_types row validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    # col_float64 is excluded below because there is no way to control the format
    # when casting to string.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_float64")
        ]
    )
    row_validation_test(tc="bq-conn", hash=cols)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types():
    """Impala to Impala dvt_core_types row validation with --comp-fields"""
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        comp_fields="*",
    )
