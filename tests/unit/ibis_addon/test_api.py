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

import ibis.expr.datatypes as dt
import pytest

from third_party.ibis.ibis_addon import api


def test_db2_type_string_length_string():
    dtype = dt.String()
    raw_data_type = ["VARCHAR", 50]
    assert api.db2_type_string_length(dtype, raw_data_type) == 50


def test_db2_type_string_length_decimal():
    dtype = dt.Decimal(10, 2)
    raw_data_type = ["DECIMAL", None, 10, 2]
    assert api.db2_type_string_length(dtype, raw_data_type) == 10


def test_db2_type_string_length_time():
    dtype = dt.Time()
    assert api.db2_type_string_length(dtype, []) == 8


def test_db2_type_string_length_date():
    dtype = dt.Date()
    assert api.db2_type_string_length(dtype, []) == 10


def test_db2_type_string_length_timestamp():
    dtype = dt.Timestamp()
    assert api.db2_type_string_length(dtype, []) == 19


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (dt.Int64(), 20),
        (dt.Int32(), 11),
        (dt.Int16(), 6),
        (dt.Int8(), 4),
    ],
)
def test_db2_type_string_length_integer(dtype, expected):
    assert api.db2_type_string_length(dtype, []) == expected


def test_db2_type_string_length_other():
    dtype = dt.Float64()
    assert api.db2_type_string_length(dtype, []) is None


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (dt.Int64(), 20),
        (dt.Int32(), 11),
        (dt.Int16(), 6),
        (dt.Int8(), 4),
    ],
)
def test_ibis_integer_string_length(dtype, expected):
    assert api.ibis_integer_string_length(dtype) == expected


def test_ibis_integer_string_length_other():
    dtype = dt.Float64()
    assert api.ibis_integer_string_length(dtype) is None


def test_cache_generator_results():
    class DummyBackend:
        def __init__(self):
            self.execution_count = 0

        @api.cache_generator_results
        def mock_generator(self, db, table, query=None):
            self.execution_count += 1
            yield ("col1", "VARCHAR")
            yield ("col2", "INT")

    backend = DummyBackend()

    # First call: should execute
    res1 = list(backend.mock_generator("db1", "tbl1"))
    assert backend.execution_count == 1
    assert res1 == [("col1", "VARCHAR"), ("col2", "INT")]

    # Second call with same args: should serve from cache
    res2 = list(backend.mock_generator("db1", "tbl1"))
    assert backend.execution_count == 1  # Execution count shouldn't increase
    assert res1 == res2

    # Third call with different args: should execute
    res3 = list(backend.mock_generator("db2", "tbl2"))
    assert backend.execution_count == 2
    assert res3 == [("col1", "VARCHAR"), ("col2", "INT")]


def test_cache_generator_results_long_query():
    class DummyBackend:
        def __init__(self):
            self.execution_count = 0

        @api.cache_generator_results
        def mock_generator(self, db, table, query=None):
            self.execution_count += 1
            yield ("col1", "VARCHAR")

    backend = DummyBackend()

    long_query = "A" * 1024

    # First call: should execute
    res1 = list(backend.mock_generator("db1", "tbl1", query=long_query))
    assert backend.execution_count == 1
    assert res1 == [("col1", "VARCHAR")]

    # Second call with same long string: should serve from cache
    res2 = list(backend.mock_generator("db1", "tbl1", query=long_query))
    assert backend.execution_count == 1
    assert res1 == res2

    # Third call with one character added: should execute (cache miss)
    long_query_modified = long_query + "B"
    res3 = list(backend.mock_generator("db1", "tbl1", query=long_query_modified))
    assert backend.execution_count == 2
    assert res3 == [("col1", "VARCHAR")]


def test_cache_generator_results_mixed_args():
    class DummyBackend:
        def __init__(self):
            self.execution_count = 0

        @api.cache_generator_results
        def mock_generator(self, database=None, table=None, query=None):
            self.execution_count += 1
            yield ("col1", "VARCHAR")

    backend = DummyBackend()

    # First call with positional args
    res1 = list(backend.mock_generator("pso_db", "dvt_table"))
    assert backend.execution_count == 1
    assert res1 == [("col1", "VARCHAR")]

    # Second call with keyword args: should hit cache
    res2 = list(backend.mock_generator(database="pso_db", table="dvt_table"))
    assert backend.execution_count == 1
    assert res1 == res2

    # Third call with kwargs explicitly defining query=None: should hit cache
    res3 = list(backend.mock_generator(database="pso_db", table="dvt_table", query=None))
    assert backend.execution_count == 1
    assert res1 == res3
