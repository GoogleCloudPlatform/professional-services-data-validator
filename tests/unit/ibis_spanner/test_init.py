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

from unittest import mock

import pytest
from google.cloud.spanner_admin_database_v1.types import DatabaseDialect

from data_validation import clients, consts, exceptions

SPANNER_CONN_CONFIG = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SPANNER,
    "project_id": "my-project",
    "instance_id": "my-instance",
    "database_id": "my-database",
}


def _mock_spanner_client(mock_client_class, dialect):
    mock_database = mock.MagicMock()
    mock_database.database_id = SPANNER_CONN_CONFIG["database_id"]
    mock_database.database_dialect = dialect
    mock_client_class.return_value.instance.return_value.database.return_value = (
        mock_database
    )


@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_get_data_client_googlesql_dialect(mock_client_class):
    _mock_spanner_client(mock_client_class, DatabaseDialect.GOOGLE_STANDARD_SQL)
    ibis_client = clients.get_data_client(SPANNER_CONN_CONFIG)
    assert ibis_client.name == "spanner"


@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_get_data_client_postgresql_dialect(mock_client_class):
    _mock_spanner_client(mock_client_class, DatabaseDialect.POSTGRESQL)
    with pytest.raises(
        exceptions.DataClientConnectionFailure,
        match=r"Spanner database my-database uses the PostgreSQL dialect which is not supported",
    ):
        clients.get_data_client(SPANNER_CONN_CONFIG)
