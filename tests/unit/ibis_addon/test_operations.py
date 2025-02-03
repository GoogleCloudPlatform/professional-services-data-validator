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
import ibis
import pandas
import pytest

from third_party.ibis.ibis_addon import operations


TABLE_DF = pandas.DataFrame([{"column": "value"}])
CLIENT = ibis.pandas.connect({"table": TABLE_DF})
WHERE_FILTER = "id > 100"

SECONDS_IN_A_DAY = 60 * 60 * 24


@pytest.fixture
def module_under_test():
    from third_party.ibis.ibis_addon import operations

    return operations


def test_import(module_under_test):
    assert module_under_test is not None


def test_format_raw_sql_expr(module_under_test):
    ibis_table = CLIENT.table("table")

    filters = [operations.compile_raw_sql(ibis_table, WHERE_FILTER)]
    query = ibis_table.filter(filters)

    # Recurse to the boolean filter column expression
    raw_sql_column_expr = query.op().to_expr().op().predicates[0]
    raw_sql = operations.format_raw_sql(ibis_table.column, raw_sql_column_expr)

    assert raw_sql == WHERE_FILTER


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "1970-01-01",
            0,
        ),
        (
            "1970-01-01 00:00:01",
            1,
        ),
        (
            "1970-01-02",
            SECONDS_IN_A_DAY,
        ),
        (
            "1970-02-01 00:00:01",
            (SECONDS_IN_A_DAY * 31) + 1,
        ),
        (
            "1969-12-31",
            -SECONDS_IN_A_DAY,
        ),
        (
            "1969-12-31 23:59:00",
            -60,
        ),
    ],
)
def test_string_to_epoch(module_under_test, test_input: str, expected: int):
    result = module_under_test.string_to_epoch(test_input)
    assert result == expected
