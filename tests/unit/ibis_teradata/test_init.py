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

import datetime
import decimal
from unittest import mock

import pytest


DVT_CORE_TYPES_CURS = [
    ["id", int, None, 8, None, None, False],
    ["col_int8", int, None, 1, None, None, True],
    ["col_int16", int, None, 2, None, None, True],
    ["col_int32", int, None, 4, None, None, True],
    ["col_int64", int, None, 8, None, None, True],
    ["col_dec_20", decimal.Decimal, None, 18, 20, 0, True],
    ["col_dec_38", decimal.Decimal, None, 18, 38, 0, True],
    ["col_dec_10_2", decimal.Decimal, None, 18, 10, 2, True],
    ["col_float32", float, None, 8, None, None, True],
    ["col_float64", float, None, 8, None, None, True],
    ["col_varchar_30", str, None, 60, None, None, True],
    ["col_char_2", str, None, 4, None, None, True],
    ["col_string", str, None, 64000, None, None, True],
    ["col_date", datetime.date, None, 4, None, None, True],
    ["col_datetime", datetime.datetime, None, 23, 0, 3, True],
    ["col_tstz", datetime.datetime, None, 29, 0, 3, True],
]

DVT_CORE_TYPES_TD_TYPES = [
    "BIGINT",
    "BYTEINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "NUMBER",
    "NUMBER",
    "NUMBER",
    "FLOAT",
    "FLOAT",
    "VARCHAR",
    "CHAR",
    "LONG VARCHAR",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
]


def get_module_under_test():
    try:
        from third_party.ibis import ibis_teradata
    except ModuleNotFoundError:
        # We don't necessarily have the Teradata client installed.
        # Tests will be skipped when the client is missing.
        ibis_teradata = None

    return ibis_teradata


@pytest.fixture
def module_under_test():
    return get_module_under_test()


@pytest.mark.skipif(not get_module_under_test(), reason="No Teradata driver")
def test_import(module_under_test):
    assert module_under_test is not None


@pytest.mark.skipif(not get_module_under_test(), reason="No Teradata driver")
def test_raw_column_metadata_no_args(module_under_test):
    backend = module_under_test.Backend()
    with pytest.raises(AssertionError):
        _ = list(backend.raw_column_metadata(database=None, table=None, query=None))


@pytest.mark.skipif(not get_module_under_test(), reason="No Teradata driver")
@mock.patch("third_party.ibis.ibis_teradata.Backend.raw_sql")
def test_raw_column_metadata_core_types(mock_raw_sql, module_under_test):
    mock_raw_sql().description = DVT_CORE_TYPES_CURS
    mock_raw_sql().columntypename = DVT_CORE_TYPES_TD_TYPES
    backend = module_under_test.Backend()
    backend.con = mock.Mock()
    raw_types = list(
        backend.raw_column_metadata(database="udf", table="dvt_core_types")
    )
    # Assert that we have str data types and not Python types.
    assert all(not isinstance(_[1], type) for _ in raw_types)
    assert all(isinstance(_[1], str) for _ in raw_types)
    # Ensure we have 7 attributes.
    assert all(len(_) == 7 for _ in raw_types)


@pytest.mark.skipif(not get_module_under_test(), reason="No Teradata driver")
@mock.patch("third_party.ibis.ibis_teradata.Backend.raw_sql")
def test_raw_column_metadata_qry(mock_raw_sql, module_under_test):
    mock_raw_sql().description = DVT_CORE_TYPES_CURS
    mock_raw_sql().columntypename = DVT_CORE_TYPES_TD_TYPES
    backend = module_under_test.Backend()
    backend.con = mock.Mock()
    raw_types = list(
        backend.raw_column_metadata(
            query="SELECT * FROM udf.dvt_core_types",
        )
    )
    # Assert that we have str data types and not Python types.
    assert all(not isinstance(_[1], type) for _ in raw_types)
    assert all(isinstance(_[1], str) for _ in raw_types)
    # Ensure we have 7 attributes.
    assert all(len(_) == 7 for _ in raw_types)
