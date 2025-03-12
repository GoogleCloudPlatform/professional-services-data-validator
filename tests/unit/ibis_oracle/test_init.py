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


class cxoDbTypeMock:
    name: str = "DB_TYPE_MOCKTYPE"


DVT_CORE_TYPES_CURS = [
    ("ID", cxoDbTypeMock(), 9, None, 8, 0, 0),
    ("COL_INT8", cxoDbTypeMock(), 3, None, 2, 0, 1),
    ("COL_INT16", cxoDbTypeMock(), 5, None, 4, 0, 1),
    ("COL_INT32", cxoDbTypeMock(), 10, None, 9, 0, 1),
    ("COL_INT64", cxoDbTypeMock(), 19, None, 18, 0, 1),
    ("COL_DEC_20", cxoDbTypeMock(), 21, None, 20, 0, 1),
    ("COL_DEC_38", cxoDbTypeMock(), 39, None, 38, 0, 1),
    ("COL_DEC_10_2", cxoDbTypeMock(), 14, None, 10, 2, 1),
    ("COL_FLOAT32", cxoDbTypeMock(), 127, None, None, None, 1),
    ("COL_FLOAT64", cxoDbTypeMock(), 127, None, None, None, 1),
    ("COL_VARCHAR_30", cxoDbTypeMock(), 30, 30, None, None, 1),
    ("COL_CHAR_2", cxoDbTypeMock(), 2, 2, None, None, 1),
    ("COL_STRING", cxoDbTypeMock(), 4000, 4000, None, None, 1),
    ("COL_DATE", cxoDbTypeMock(), 23, None, None, None, 1),
    ("COL_DATETIME", cxoDbTypeMock(), 23, None, 0, 3, 1),
    ("COL_TSTZ", cxoDbTypeMock(), None, None, 0, 3, 1),
]


def get_module_under_test():
    try:
        from third_party.ibis import ibis_oracle
    except ModuleNotFoundError:
        # We don't install Oracle client for Github unit tests.
        # These tests will only complete when executed locally.
        ibis_oracle = None

    return ibis_oracle


@pytest.fixture
def module_under_test():
    return get_module_under_test()


@pytest.mark.skipif(not get_module_under_test(), reason="No Oracle driver")
def test_import(module_under_test):
    """Check that importing ibis_oracle does not throw exceptions (aside from ModuleNotFoundError)"""
    assert module_under_test is not None


@pytest.mark.skipif(not get_module_under_test(), reason="No Oracle driver")
def test_raw_column_metadata_no_args(module_under_test):
    backend = module_under_test.Backend()
    with pytest.raises(AssertionError):
        _ = list(backend.raw_column_metadata(database=None, table=None, query=None))


@pytest.mark.skipif(not get_module_under_test(), reason="No Oracle driver")
@mock.patch("third_party.ibis.ibis_oracle.Backend.begin")
def test_raw_column_metadata_core_types(mock_begin, module_under_test):
    mock_begin().__enter__().exec_driver_sql().cursor.description = DVT_CORE_TYPES_CURS
    backend = module_under_test.Backend()
    raw_types = list(
        backend.raw_column_metadata(
            database="pso_data_validator", table="dvt_core_types"
        )
    )
    # Assert that the cx_Oracle prefix was removed from data types.
    assert all(_[1] == "MOCKTYPE" for _ in raw_types)
    # Ensure we have 7 attributes.
    assert all(len(_) == 7 for _ in raw_types)


@pytest.mark.skipif(not get_module_under_test(), reason="No Oracle driver")
@mock.patch("third_party.ibis.ibis_oracle.Backend.begin")
def test_raw_column_metadata_qry(mock_begin, module_under_test):
    mock_begin().__enter__().exec_driver_sql().cursor.description = DVT_CORE_TYPES_CURS
    backend = module_under_test.Backend()
    raw_types = list(
        backend.raw_column_metadata(
            query="SELECT * FROM pso_data_validator.dvt_core_types"
        )
    )
    # Assert that the cx_Oracle prefix was removed from data types.
    assert all(_[1] == "MOCKTYPE" for _ in raw_types)
    # Ensure we have 7 attributes.
    assert all(len(_) == 7 for _ in raw_types)
