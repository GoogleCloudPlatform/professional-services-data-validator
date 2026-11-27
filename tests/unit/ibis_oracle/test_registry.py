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

import functools
from unittest import mock

import ibis
import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import oracle


def get_module_under_test():
    try:
        from third_party.ibis.ibis_oracle import registry
    except ModuleNotFoundError:
        # We don't install Oracle client for Github unit tests.
        # These tests will only complete when executed locally.
        registry = None

    return registry


def _strftime_sql(registry, format_str: str) -> str:
    tokens, _ = registry._scanner.scan(format_str)
    reduced = registry._reduce_tokens(tokens, sa.column("ts"))
    expr = functools.reduce(sa.sql.ColumnElement.concat, reduced)
    return str(
        expr.compile(dialect=oracle.dialect(), compile_kwargs={"literal_binds": True})
    )


@pytest.mark.skipif(not get_module_under_test(), reason="No Oracle driver")
def test_strftime_day_of_month_space_padded():
    """%e must only replace the leading zero, not the zero in 10, 20 and 30 (issue 1808)."""
    sql = _strftime_sql(get_module_under_test(), "%Y-%m-%e")
    assert "lpad(ltrim(to_char(ts, 'DD'), '0'), 2, ' ')" in sql
    assert "replace(" not in sql


@pytest.mark.skipif(not get_module_under_test(), reason="No Oracle driver")
@pytest.mark.parametrize(
    ("from_type", "to_type", "expected_sql"),
    [
        # Oracle FLOAT is a NUMBER subtype, so float64 must be BINARY_DOUBLE to stay IEEE.
        ("float32", "float64", "CAST(c AS BINARY_DOUBLE)"),
        ("decimal(10, 2)", "float64", "CAST(c AS BINARY_DOUBLE)"),
        ("decimal(10, 2)", "float32", "CAST(c AS BINARY_FLOAT)"),
    ],
)
def test_cast_to_binary_float_types(from_type, to_type, expected_sql):
    registry = get_module_under_test()
    arg = ibis.table([("c", from_type)], name="t").c
    op = arg.cast(to_type).op()
    translator = mock.Mock(translate=mock.Mock(return_value=sa.column("c")))
    sql = str(registry._cast(translator, op).compile(dialect=oracle.dialect()))
    assert sql == expected_sql
