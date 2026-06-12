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

import os
import sys
import pytest
from unittest import mock

# Add samples/cloud_run to sys.path so we can import main
cloud_run_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../samples/cloud_run")
)
sys.path.insert(0, cloud_run_dir)

import main


@pytest.fixture
def client():
    main.app.config["TESTING"] = True
    with main.app.test_client() as client:
        yield client


def test_ping(client):
    rv = client.get("/ping")
    assert rv.status_code == 200
    assert rv.data == b"OK"


def test_version(client):
    rv = client.get("/version")
    assert rv.status_code == 200
    assert "version" in rv.get_json()


@mock.patch("main.build_config_managers_from_args")
@mock.patch("main.store_yaml_config_file")
def test_generate_custom_query_row_config(mock_store, mock_build, client):
    mock_build.return_value = []

    payload = {
        "source-conn": "my_source",
        "target-conn": "my_target",
        "source-query": "SELECT * FROM my_table",
        "target-query": "SELECT * FROM target_table",
        "config-file": "test_config.yaml",
        "result-handler": "my_conn.my_dataset.my_table",
    }

    response = client.post("/generate_custom_query_row_config", json=payload)
    assert response.status_code == 200
    assert b"Success! Config output written to test_config.yaml" in response.data

    mock_build.assert_called_once()
    args = mock_build.call_args[0][0]
    assert args.source_conn == "my_source"
    assert args.target_conn == "my_target"
    assert args.source_query == "SELECT * FROM my_table"
    assert args.target_query == "SELECT * FROM target_table"
    assert args.config_file == "test_config.yaml"
    assert args.validate_cmd == "custom-query"
    assert args.custom_query_type == "row"


@mock.patch("main.build_config_managers_from_args")
@mock.patch("main.store_yaml_config_file")
def test_generate_custom_query_row_config_missing_file(mock_store, mock_build, client):
    payload = {
        "source-conn": "my_source",
        "target-conn": "my_target",
        "source-query": "SELECT * FROM my_table",
        "target-query": "SELECT * FROM target_table",
    }

    response = client.post("/generate_custom_query_row_config", json=payload)
    assert response.status_code == 400
    assert b"Bad Request: config_file is a mandatory parameter" in response.data


@mock.patch("main.build_config_managers_from_args")
@mock.patch("main.store_yaml_config_file")
def test_generate_custom_query_row_config_missing_source_query(
    mock_store, mock_build, client
):
    payload = {
        "source-conn": "my_source",
        "target-conn": "my_target",
        "target-query": "SELECT * FROM target_table",
        "config-file": "test_config.yaml",
    }

    response = client.post("/generate_custom_query_row_config", json=payload)
    assert response.status_code == 400
    assert b"Bad Request: source_query is a mandatory parameter" in response.data


@mock.patch("main.build_config_managers_from_args")
@mock.patch("main.store_yaml_config_file")
def test_generate_custom_query_row_config_missing_target_query(
    mock_store, mock_build, client
):
    payload = {
        "source-conn": "my_source",
        "target-conn": "my_target",
        "source-query": "SELECT * FROM my_table",
        "config-file": "test_config.yaml",
    }

    response = client.post("/generate_custom_query_row_config", json=payload)
    assert response.status_code == 400
    assert b"Bad Request: target_query is a mandatory parameter" in response.data


@mock.patch("data_validation.state_manager.StateManager")
@mock.patch("data_validation.clients.get_data_client_ctx")
def test_oracle_check_dbms_crypto_success(
    mock_get_client_ctx, mock_state_manager, client
):
    # Mock StateManager
    mock_mgr = mock.MagicMock()
    mock_state_manager.return_value = mock_mgr
    mock_mgr.get_connection_config.return_value = {"source_type": "Oracle"}

    # Mock client and cursor
    mock_client = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_client.raw_sql.return_value = mock_cursor

    # Mock context manager
    mock_get_client_ctx.return_value.__enter__.return_value = mock_client

    payload = {"connection_name": "my_oracle_conn"}
    response = client.post("/oracle_check_dbms_crypto", json=payload)

    assert response.status_code == 200
    assert response.data == b"OK"
    mock_client.raw_sql.assert_called_once_with(
        "SELECT DBMS_CRYPTO.HASH(TO_CLOB('DVT'),4) FROM dual"
    )
    mock_cursor.fetchone.assert_called_once()
    mock_cursor.close.assert_called_once()


@mock.patch("data_validation.state_manager.StateManager")
@mock.patch("data_validation.clients.get_data_client_ctx")
def test_oracle_check_dbms_crypto_failure(
    mock_get_client_ctx, mock_state_manager, client
):
    # Mock StateManager
    mock_mgr = mock.MagicMock()
    mock_state_manager.return_value = mock_mgr
    mock_mgr.get_connection_config.return_value = {"source_type": "Oracle"}

    # Mock client to raise exception on raw_sql
    mock_client = mock.MagicMock()
    mock_client.raw_sql.side_effect = Exception(
        'ORA-00904: "SYS"."DBMS_CRYPTO": invalid identifier'
    )

    # Mock context manager
    mock_get_client_ctx.return_value.__enter__.return_value = mock_client

    payload = {"connection_name": "my_oracle_conn"}
    response = client.post("/oracle_check_dbms_crypto", json=payload)

    assert response.status_code == 500
    assert b'ORA-00904: "SYS"."DBMS_CRYPTO": invalid identifier' in response.data


def test_oracle_check_dbms_crypto_missing_connection_name(client):
    payload = {}
    response = client.post("/oracle_check_dbms_crypto", json=payload)
    assert response.status_code == 400
    assert b"Bad Request: connection_name is a required parameter" in response.data
