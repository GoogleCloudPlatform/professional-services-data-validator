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

import pytest

from data_validation import state_manager


GCS_STATE_PATH = "gs://pso-kokoro-resources/state/"
TEST_CONN_NAME = "example"
TEST_CONN = {
    "source_type": "BigQuery",
    "project_id": "my-project",
}


def test_get_gcs_file_path():
    manager = state_manager.StateManager(GCS_STATE_PATH)
    result_path = manager._get_gcs_file_path(GCS_STATE_PATH + "file/path/name.json")

    assert result_path == "state/file/path/name.json"


def test_gcs_create_and_get_connection_config():
    manager = state_manager.StateManager(GCS_STATE_PATH)
    manager.create_connection(TEST_CONN_NAME, TEST_CONN)

    config = manager.get_connection_config(TEST_CONN_NAME)
    assert config == TEST_CONN


def test_list_connections():
    manager = state_manager.StateManager(GCS_STATE_PATH)
    expected = set(["example", "my_bq_conn"])

    connections = manager.list_connections()

    assert set(connections) == expected


def test_create_invalid_gcs_path_raises():
    # Unknown file paths will be created by the state manager
    files_directory = "gs://!!bucket!!/this/path/"

    with pytest.raises(ValueError, match=r"GCS Path Failure .*"):
        state_manager.StateManager(files_directory)
