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
