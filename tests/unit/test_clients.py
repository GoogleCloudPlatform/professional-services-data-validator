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

from unittest import mock
import pytest

from google.auth import credentials
import pandas
import ibis.backends.pandas
from ibis.backends.pandas.client import PandasClient

from data_validation import clients, exceptions


TABLE_NAME = "my_table"
DATA = [{"a": 1, "b": 2}]
TABLES_RESULT = [(None, TABLE_NAME)]

SOURCE_TABLE_FILE_PATH = "source_table_data.json"
JSON_DATA = """[{"col_a":0,"col_b":"a"},{"col_a":1,"col_b":"b"}]"""
SOURCE_CONN_CONFIG = {
    "source_type": "FileSystem",
    "table_name": "my_table",
    "file_path": SOURCE_TABLE_FILE_PATH,
    "file_type": "json",
}

ORACLE_CONN_CONFIG = {
    "source_type": "Oracle",
    "host": "127.0.0.1",
    "port": 1521,
}


def _create_table_file(table_path, data):
    """Write JSON data to given file."""
    with open(table_path, "w") as f:
        f.write(data)


def _get_pandas_client():
    df = pandas.DataFrame(DATA)
    pandas_client = ibis.backends.pandas.connect({TABLE_NAME: df})

    return pandas_client


def test_get_all_tables():
    """Test get all tables util."""
    client = _get_pandas_client()
    all_tables = clients.get_all_tables(client)

    assert all_tables == TABLES_RESULT


def test_get_bigquery_client_sets_user_agent():
    mock_credentials = mock.create_autospec(credentials.Credentials)
    ibis_client = clients.get_bigquery_client(
        "test-project", dataset_id="some_dataset", credentials=mock_credentials
    )
    user_agent = ibis_client.client._connection._client_info.to_user_agent()
    assert "google-pso-tool/data-validator" in user_agent


def test_import_oracle_client():
    with pytest.raises(ModuleNotFoundError, match=r"No module named 'cx_Oracle'"):
        from third_party.ibis.ibis_oracle.client import OracleClient  # NOQA


def test_get_oracle_data_client():
    with pytest.raises(
        exceptions.DataClientConnectionFailure, match=r".*pip install cx_Oracle"
    ):
        clients.get_data_client(ORACLE_CONN_CONFIG)


def test_get_pandas_data_client(fs):
    conn_config = SOURCE_CONN_CONFIG
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    ibis_client = clients.get_data_client(conn_config)

    assert isinstance(ibis_client, PandasClient)
