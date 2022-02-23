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

from data_validation import state_manager

TEST_CONN_NAME = "example"
TEST_CONN = {
    "source_type": "BigQuery",
    "project_id": "my-project",
}
TEST_VALIDATION_NAME = "citibike.yaml"
TEST_VALIDATION_CONFIG = {
    "source": "example",
    "target": "example",
    "result_handler": {},
    "validations": [
        {
            "type": "Column",
            "table_name": "citibike_trips",
            "schema_name": "bigquery-public-data.new_york_citibike",
            "target_schema_name": "bigquery-public-data.new_york_citibike",
            "target_table_name": "citibike_trips",
            "labels": [],
            "threshold": 0.0,
            "format": "table",
            "filters": [],
            "aggregates": [
                {
                    "source_column": None,
                    "target_column": None,
                    "field_alias": "count",
                    "type": "count",
                }
            ],
        }
    ],
}


def test_create_and_get_connection_config(capsys, fs):
    manager = state_manager.StateManager()
    manager.create_connection(TEST_CONN_NAME, TEST_CONN)

    config = manager.get_connection_config(TEST_CONN_NAME)
    assert config == TEST_CONN


def test_create_and_list_connection(capsys, fs):
    manager = state_manager.StateManager()
    manager.create_connection(TEST_CONN_NAME, TEST_CONN)

    connections = manager.list_connections()
    assert connections == [TEST_CONN_NAME]


def test_create_unknown_filepath(capsys, fs):
    # Unknown file paths will be created by the state manager
    files_directory = "create/this/path/"
    manager = state_manager.StateManager(files_directory)
    manager.create_connection(TEST_CONN_NAME, TEST_CONN)

    connections = manager.list_connections()
    assert connections == [TEST_CONN_NAME]

    file_path = manager._get_connection_path(TEST_CONN_NAME)
    expected_file_path = files_directory + f"{TEST_CONN_NAME}.connection.json"
    assert file_path == expected_file_path


def test_create_and_get_validation_config(capsys, fs):
    manager = state_manager.StateManager()
    manager.create_validation_yaml(TEST_VALIDATION_NAME, TEST_VALIDATION_CONFIG)

    config = manager.get_validation_config(TEST_VALIDATION_NAME)
    assert config == TEST_VALIDATION_CONFIG


def test_create_and_list_validation(capsys, fs):
    manager = state_manager.StateManager()
    manager.create_validation_yaml(TEST_VALIDATION_NAME, TEST_VALIDATION_CONFIG)

    validations = manager.list_validations()
    assert validations == [TEST_VALIDATION_NAME.split(".")[0]]
