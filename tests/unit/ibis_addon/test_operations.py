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

import pandas
import pytest
import ibis.pandas

from third_party.ibis.ibis_addon import operations


TABLE_DF = pandas.DataFrame([{"column": "value"}])
CLIENT = ibis.pandas.connect({"table": TABLE_DF})
WHERE_FILTER = "id > 100"


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
