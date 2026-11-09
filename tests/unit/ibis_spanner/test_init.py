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

"""Unit tests for the Spanner Ibis backend in third_party/ibis/ibis_cloud_spanner."""

from unittest import mock

import pytest

from data_validation import clients, consts, exceptions
from third_party.ibis.ibis_cloud_spanner import SPANNER_DIALECT_QUERY

SPANNER_CONN_CONFIG = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SPANNER,
    "project_id": "my-project",
    "instance_id": "my-instance",
    "database_id": "my-database",
}


def _mock_spanner_client(mock_client_class, dialect_rows):
    """Configure a mocked spanner.Client so the dialect query returns dialect_rows.

    Args:
        mock_client_class: The patched google.cloud.spanner.Client class.
        dialect_rows: Rows to be returned by the dialect query, e.g. [["POSTGRESQL"]].

    Returns:
        A tuple of the mocked Database and the mocked Snapshot.
    """
    mock_database = mock.MagicMock()
    mock_database.database_id = SPANNER_CONN_CONFIG["database_id"]
    mock_snapshot = mock_database.snapshot.return_value.__enter__.return_value
    mock_snapshot.execute_sql.return_value = iter(dialect_rows)
    mock_client_class.return_value.instance.return_value.database.return_value = (
        mock_database
    )
    return mock_database, mock_snapshot


def _assert_no_admin_api_calls(mock_client_class, mock_database):
    """Assert the dialect check did not use the Admin API.

    Admin API calls consume a separate quota and require extra permissions.
    """
    mock_database.reload.assert_not_called()
    mock_client_class.return_value.database_admin_api.get_database.assert_not_called()
    mock_client_class.return_value.database_admin_api.get_database_ddl.assert_not_called()


@pytest.mark.parametrize(
    "dialect_rows",
    [
        [["GOOGLE_STANDARD_SQL"]],
        # Defensive: no row returned, assume GoogleSQL.
        [],
    ],
)
@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_get_data_client_googlesql_dialect(mock_client_class, dialect_rows):
    """A GoogleSQL dialect database connects successfully using a data query only."""
    mock_database, mock_snapshot = _mock_spanner_client(mock_client_class, dialect_rows)
    ibis_client = clients.get_data_client(SPANNER_CONN_CONFIG)
    assert ibis_client.name == "spanner"
    mock_snapshot.execute_sql.assert_called_once_with(SPANNER_DIALECT_QUERY)
    _assert_no_admin_api_calls(mock_client_class, mock_database)


@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_get_data_client_postgresql_dialect(mock_client_class):
    """A PostgreSQL dialect database raises a clear DataClientConnectionFailure."""
    mock_database, mock_snapshot = _mock_spanner_client(
        mock_client_class, [["POSTGRESQL"]]
    )
    with pytest.raises(
        exceptions.DataClientConnectionFailure,
        match=r"Spanner database my-database uses the PostgreSQL dialect which is not supported",
    ):
        clients.get_data_client(SPANNER_CONN_CONFIG)
    mock_snapshot.execute_sql.assert_called_once_with(SPANNER_DIALECT_QUERY)
    _assert_no_admin_api_calls(mock_client_class, mock_database)
