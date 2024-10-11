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

from data_validation import (
    cli_tools,
    data_validation,
    consts,
    find_tables,
    __main__ as main,
)
from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from tests.system.data_sources.common_functions import (
    binary_key_assertions,
    find_tables_assertions,
    id_type_test_assertions,
    null_not_null_assertions,
    row_validation_many_columns_test,
    run_test_from_cli_args,
    generate_partitions_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN

CONN = {
    "source_type": "FileSystem", 
    "table_name": "entries", 
    "file_path": "gs://pso-kokoro-resources/file_connection/parquet/entries.parquet", 
    "file_type": "parquet",
}

def test_filesystem_count():
    """Test count validation on Filesystem"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: "guestName",
                consts.CONFIG_TARGET_COLUMN: "guestName",
                consts.CONFIG_FIELD_ALIAS: "count_guestname",
            },
            {
                consts.CONFIG_TYPE: "sum",
                consts.CONFIG_SOURCE_COLUMN: "entryID",
                consts.CONFIG_TARGET_COLUMN: "entryID",
                consts.CONFIG_FIELD_ALIAS: "sum_entryid",
            },
        ],
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]

def mock_get_connection_config(*args):
    if args[1] == "bq-conn":
        return BQ_CONN
    else:
        return CONN

@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)

def test_schema_validation():
    """Test schema validation on Postgres instance"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        consts.CONFIG_TYPE: "Schema",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS


