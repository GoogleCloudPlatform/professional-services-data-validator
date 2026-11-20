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

from google.cloud.spanner_v1 import param_types
import pytest

from data_validation import clients, consts, exceptions
from third_party.ibis.ibis_cloud_spanner import (
    SPANNER_DIALECT_QUERY,
    SPANNER_PRIMARY_KEY_COLUMNS_QUERY,
)

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
        The mocked Snapshot.
    """
    mock_database = mock.MagicMock()
    mock_database.database_id = SPANNER_CONN_CONFIG["database_id"]
    mock_snapshot = mock_database.snapshot.return_value.__enter__.return_value
    mock_snapshot.execute_sql.return_value = iter(dialect_rows)
    mock_client_class.return_value.instance.return_value.database.return_value = (
        mock_database
    )
    return mock_snapshot


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
    mock_snapshot = _mock_spanner_client(mock_client_class, dialect_rows)
    ibis_client = clients.get_data_client(SPANNER_CONN_CONFIG)
    assert ibis_client.name == "spanner"
    mock_snapshot.execute_sql.assert_called_once_with(SPANNER_DIALECT_QUERY)


@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_get_data_client_postgresql_dialect(mock_client_class):
    """A PostgreSQL dialect database raises a clear DataClientConnectionFailure."""
    mock_snapshot = _mock_spanner_client(mock_client_class, [["POSTGRESQL"]])
    with pytest.raises(
        exceptions.DataClientConnectionFailure,
        match=r"Spanner database my-database uses the PostgreSQL dialect which is not supported",
    ):
        clients.get_data_client(SPANNER_CONN_CONFIG)
    mock_snapshot.execute_sql.assert_called_once_with(SPANNER_DIALECT_QUERY)


@pytest.mark.parametrize(
    "pk_rows,expected",
    [
        ([["id"]], ["id"]),
        # Composite key, the query orders by ordinal_position.
        ([["key1"], ["key2"], ["key3"]], ["key1", "key2", "key3"]),
        # Views and tables declared with PRIMARY KEY () have no key columns.
        ([], []),
    ],
)
@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_list_primary_key_columns(mock_client_class, pk_rows, expected):
    """Primary key columns come from a bound information_schema data query."""
    mock_snapshot = _mock_spanner_client(mock_client_class, [["GOOGLE_STANDARD_SQL"]])
    ibis_client = clients.get_data_client(SPANNER_CONN_CONFIG)
    mock_snapshot.execute_sql.reset_mock()
    mock_snapshot.execute_sql.return_value = iter(pk_rows)

    assert ibis_client.list_primary_key_columns("my-database", "my_table") == expected
    mock_snapshot.execute_sql.assert_called_once_with(
        SPANNER_PRIMARY_KEY_COLUMNS_QUERY,
        params={"table_name": "my_table"},
        param_types={"table_name": param_types.STRING},
    )


@pytest.mark.parametrize(
    "database",
    [
        # The schema part of a table name, e.g. a BigQuery dataset when Spanner is the target.
        "other-schema",
        None,
    ],
)
@mock.patch("third_party.ibis.ibis_cloud_spanner.spanner.Client")
def test_list_primary_key_columns_uses_connected_database(mock_client_class, database):
    """The primary key query always runs against the connected Spanner database."""
    mock_snapshot = _mock_spanner_client(mock_client_class, [["GOOGLE_STANDARD_SQL"]])
    ibis_client = clients.get_data_client(SPANNER_CONN_CONFIG)
    mock_instance = mock_client_class.return_value.instance.return_value
    mock_instance.database.assert_called_once_with(SPANNER_CONN_CONFIG["database_id"])
    mock_instance.database.reset_mock()
    mock_snapshot.execute_sql.reset_mock()
    mock_snapshot.execute_sql.return_value = iter([["id"]])

    assert ibis_client.list_primary_key_columns(database, "my_table") == ["id"]
    # No other database handle is opened, the connection's database is used.
    mock_instance.database.assert_not_called()
    assert ibis_client.database_name.database_id == SPANNER_CONN_CONFIG["database_id"]
    mock_snapshot.execute_sql.assert_called_once()
