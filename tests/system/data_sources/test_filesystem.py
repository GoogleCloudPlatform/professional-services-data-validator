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

from unittest import mock


from data_validation import (
    data_validation,
    consts,
)

from tests.system.data_sources.common_functions import (
    schema_validation_test,
)


def mock_get_connection_config(*args):
    if args[1] in ("parquet-conn", "mock-conn"):
        return PARQUET_CONN
    elif args[1] == "csv-conn":
        return CSV_CONN
    elif args[1] == "json-conn":
        return JSON_CONN
    elif args[1] == "orc-conn":
        return ORC_CONN
    else:
        return PARQUET_CONN


CSV_CONN = {
    "source_type": "FileSystem",
    "table_name": "entries",
    "file_path": "gs://pso-kokoro-resources/file_connection/csv/entries.csv",
    "file_type": "csv",
}

JSON_CONN = {
    "source_type": "FileSystem",
    "table_name": "entries",
    "file_path": "gs://pso-kokoro-resources/file_connection/json/entries.json",
    "file_type": "json",
}

ORC_CONN = {
    "source_type": "FileSystem",
    "table_name": "entries",
    "file_path": "gs://pso-kokoro-resources/file_connection/orc/entries.orc",
    "file_type": "orc",
}

PARQUET_CONN = {
    "source_type": "FileSystem",
    "table_name": "entries",
    "file_path": "gs://pso-kokoro-resources/file_connection/parquet/entries.parquet",
    "file_type": "parquet",
}


def format_config_count(s_conn, t_conn):
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: s_conn,
        consts.CONFIG_TARGET_CONN: t_conn,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validation Type
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
    return config_count_valid


def test_filesystem_count_parquet():
    config_count_valid = format_config_count(PARQUET_CONN, PARQUET_CONN)
    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]


def test_filesystem_count_json():
    config_count_valid = format_config_count(JSON_CONN, JSON_CONN)
    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]


def test_filesystem_count_orc():
    config_count_valid = format_config_count(ORC_CONN, ORC_CONN)
    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]


def test_filesystem_count_csv():
    config_count_valid = format_config_count(CSV_CONN, CSV_CONN)
    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_parquet():
    """Test schema validation Parquet file.
    This used to use the entries table.
    """
    schema_validation_test(tables="entries", tc="PARQUET_CONN")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_csv():
    """Test schema validation CSV file.
    This used to use the entries table.
    """
    schema_validation_test(tables="entries", tc="CSV_CONN")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_orc():
    """Test schema validation ORC file.
    This used to use the entries table.
    """
    schema_validation_test(tables="entries", tc="ORC_CONN")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_json():
    """Test schema validation JSON file.
    This used to use the entries table.
    """
    schema_validation_test(tables="entries", tc="JSON_CONN")
