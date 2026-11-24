# Copyright 2025 Google LLC
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


class PgColumn:
    def __init__(
        self, name, type_code, display_size, internal_size, precision, scale, null_ok
    ):
        self.name = name
        self.type_code = type_code
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.null_ok = null_ok


DVT_CORE_TYPES_CURS = (
    PgColumn("id", 23, None, 4, None, None, None),
    PgColumn("col_int8", 21, None, 2, None, None, None),
    PgColumn("col_int16", 21, None, 2, None, None, None),
    PgColumn("col_int32", 23, None, 4, None, None, None),
    PgColumn("col_int64", 20, None, 8, None, None, None),
    PgColumn("col_dec_20", 1700, None, 20, 20, 0, None),
    PgColumn("col_dec_38", 1700, None, 38, 38, 0, None),
    PgColumn("col_dec_10_2", 1700, None, 10, 10, 2, None),
    PgColumn("col_float32", 700, None, 4, None, None, None),
    PgColumn("col_float64", 701, None, 8, None, None, None),
    PgColumn("col_varchar_30", 1043, None, 30, None, None, None),
    PgColumn("col_char_2", 1042, None, 2, None, None, None),
    PgColumn("col_string", 25, None, -1, None, None, None),
    PgColumn("col_date", 1082, None, 4, None, None, None),
    PgColumn("col_datetime", 1114, None, 8, None, None, None),
    PgColumn("col_tstz", 1184, None, 8, None, None, None),
)


@pytest.fixture
def module_under_test():
    from third_party.ibis.ibis_postgres import client

    return client


def test_import(module_under_test):
    assert module_under_test is not None


def test_raw_column_metadata_no_args(module_under_test):
    backend = module_under_test.PostgresBackend()
    with pytest.raises(AssertionError):
        _ = list(backend.raw_column_metadata(database=None, table=None, query=None))


@mock.patch("third_party.ibis.ibis_postgres.client.PostgresBackend.begin")
def test_raw_column_metadata_core_types(mock_begin, module_under_test):
    mock_begin().__enter__().exec_driver_sql().cursor.description = DVT_CORE_TYPES_CURS
    backend = module_under_test.PostgresBackend()
    raw_types = list(
        backend.raw_column_metadata(
            database="pso_data_validator", table="dvt_core_types"
        )
    )
    # Ensure we have 7 attributes.
    assert all(len(_) == 7 for _ in raw_types)


@mock.patch("third_party.ibis.ibis_postgres.client.PostgresBackend.begin")
def test_list_primary_key_columns_mixed_case(mock_begin, module_under_test):
    """Test that schema and table names are passed separately so they can be quoted in the query."""
    mock_execute = mock_begin().__enter__().execute
    mock_execute().cursor.fetchall.return_value = [("Id",)]
    backend = module_under_test.PostgresBackend()
    assert backend.list_primary_key_columns("MySchema", "MyTable") == ["Id"]
    query = mock_execute.call_args.args[0]
    assert "format('%I.%I', :schema_name, :table_name)" in query.text
    assert query.compile().params == {
        "schema_name": "MySchema",
        "table_name": "MyTable",
    }


@mock.patch("third_party.ibis.ibis_postgres.client.PostgresBackend.begin")
def test_raw_column_metadata_qry(mock_begin, module_under_test):
    mock_begin().__enter__().exec_driver_sql().cursor.description = DVT_CORE_TYPES_CURS
    backend = module_under_test.PostgresBackend()
    raw_types = list(
        backend.raw_column_metadata(
            query="SELECT * FROM pso_data_validator.dvt_core_types"
        )
    )
    # Ensure we have 7 attributes.
    assert all(len(_) == 7 for _ in raw_types)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM pso_data_validator.dvt_core_types",
        "WITH cte AS (SELECT * FROM pso_data_validator.dvt_core_types) SELECT * FROM cte",
        "/* comment */ SELECT * FROM pso_data_validator.dvt_core_types",
        "VALUES (1)",
    ],
)
@mock.patch("third_party.ibis.ibis_postgres.client.PostgresBackend.begin")
def test_get_schema_using_query(mock_begin, module_under_test, query):
    """Test that the query is used as a subquery whatever it starts with."""
    mock_execute = mock_begin().__enter__().exec_driver_sql
    mock_execute().cursor.description = []
    mock_execute().__iter__.return_value = iter([("id", "integer")])
    backend = module_under_test.PostgresBackend()
    schema = backend._get_schema_using_query(query)
    assert schema.names == ("id",)
    executed_sql = [_.args[0] for _ in mock_execute.call_args_list if _.args]
    assert executed_sql[0] == f"SELECT * FROM ({query}) t0 LIMIT 0"


@mock.patch("third_party.ibis.ibis_postgres.client.PostgresBackend.begin")
def test_metadata_table_name(mock_begin, module_under_test):
    """Test that a table name is not wrapped in brackets."""
    mock_execute = mock_begin().__enter__().exec_driver_sql
    mock_execute().cursor.description = []
    backend = module_under_test.PostgresBackend()
    list(backend._metadata('"MySchema"."MyTable"'))
    executed_sql = [_.args[0] for _ in mock_execute.call_args_list if _.args]
    assert executed_sql[0] == 'SELECT * FROM "MySchema"."MyTable" t0 LIMIT 0'
