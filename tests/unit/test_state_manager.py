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

from data_validation import consts, state_manager
import yaml

TEST_CONN_NAME = "example"
TEST_CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_BIGQUERY,
    "project_id": "my-project",
}


def test_create_get_list_delete_connection_config(capsys, fs):
    manager = state_manager.StateManager()

    # 1. create and get
    manager.create_connection(TEST_CONN_NAME, TEST_CONN)
    config = manager.get_connection_config(TEST_CONN_NAME)
    assert config == TEST_CONN

    # 2. list
    connections = manager.list_connections()
    assert connections == [TEST_CONN_NAME]

    # 3. delete
    manager.delete_connection(TEST_CONN_NAME)

    connections = manager.list_connections()
    assert connections == []  # make sure connection is deleted


def test_describe_connection(fs):
    """Test that describe_connection returns correct info in both JSON and YAML."""
    manager = state_manager.StateManager()

    # 1. Create a connection
    manager.create_connection(TEST_CONN_NAME, TEST_CONN)

    # 2. Describe in JSON format
    json_desc = manager.describe_connection(TEST_CONN_NAME, "json")
    # Should return the raw dict
    assert (
        json_desc == TEST_CONN
    ), "describe_connection in JSON format should return the connection dict"

    # 3. Describe in YAML format
    yaml_desc = manager.describe_connection(TEST_CONN_NAME, "yaml")
    parsed_yaml = yaml.safe_load(yaml_desc)
    assert (
        parsed_yaml == TEST_CONN
    ), "describe_connection in YAML format should match the original connection data"


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
