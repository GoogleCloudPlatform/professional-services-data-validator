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

import datetime
import decimal
import ibis
import pandas
import pytest

from third_party.ibis.ibis_addon import operations

TABLE_DF = pandas.DataFrame([{"column": "value"}])
CLIENT = ibis.pandas.connect({"table": TABLE_DF})
WHERE_FILTER = "id > 100"

SECONDS_IN_A_DAY = 60 * 60 * 24

INT64_MIN = int("-9223372036854775808")


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
        (
            "1969-12-31 23:59:00Z",
            -60,
        ),
        (
            "1970-01-01T00:00:00Z",
            0,
        ),
        (
            "1970-01-01T01:00:00+01:00",
            0,
        ),
        (
            "1969-12-31T23:00:00-01:00",
            0,
        ),
    ],
)
def test_string_to_epoch(module_under_test, test_input: str, expected: int):
    result = module_under_test.string_to_epoch(test_input)
    assert result == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        # Simple input.
        (
            pandas.Series(
                [
                    pandas.to_datetime("1970-01-01 00:00:10"),
                ]
            ),
            pandas.Series(
                [
                    10,
                ]
            ),
        ),
        (
            pandas.Series(
                [
                    pandas.to_datetime("1969-12-31 23:59:50"),
                ]
            ),
            pandas.Series(
                [
                    -10,
                ]
            ),
        ),
        # With NaT.
        (
            pandas.Series(
                [
                    pandas.to_datetime("1970-01-01 00:00:10"),
                    pandas.to_datetime(None),
                ]
            ),
            pandas.Series(
                [
                    10,
                    INT64_MIN // 1_000_000_000,
                ]
            ),
        ),
        # With datetime which is what happens when datetime64[ns] overflows.
        (
            pandas.Series(
                [
                    pandas.to_datetime("1970-01-01 00:00:10"),
                    datetime.date(1000, 1, 1),
                ]
            ),
            pandas.Series(
                [
                    10,
                    -30610224000,
                ]
            ),
        ),
        # With datetime and NaT.
        (
            pandas.Series(
                [
                    datetime.date(1000, 1, 1),
                    pandas.to_datetime(None),
                ]
            ),
            pandas.Series(
                [
                    -30610224000,
                    INT64_MIN // 1_000_000_000,
                ]
            ),
        ),
    ],
)
def test_execute_epoch_seconds_new(
    module_under_test, test_input: pandas.Series, expected: pandas.Series
):
    result = module_under_test.execute_epoch_seconds_new(None, test_input)
    assert list(result) == list(expected)


@pytest.mark.parametrize(
    ("values", "target_type", "expected"),
    [
        # Oracle returns NUMBER(5) group key as float, e.g. 1.0 -> safe to use decimal(5, 0).
        ([1.0, 2.0, None], "decimal128(5, 0)", True),
        ([None, None], "decimal128(5, 0)", True),
        # Fractional values (e.g. avg) must stay as float, Arrow would round 1.5 to 2.
        ([1.0, 1.5], "decimal128(5, 0)", False),
        # Overflows the target precision.
        ([123456.0], "decimal128(5, 0)", False),
        # Whole number but beyond exact float64 range, precision already lost.
        ([1.2345678901234568e28], "decimal128(38, 0)", False),
        # Only integer decimals (scale 0) are considered.
        ([1.0], "decimal128(38, 9)", False),
    ],
)
def test_floats_exactly_fit_integer_decimal(
    module_under_test, values, target_type, expected
):
    import pyarrow

    precision, scale = (
        int(_) for _ in target_type[len("decimal128(") : -1].split(",")  # noqa: E203
    )
    col = pyarrow.chunked_array([pyarrow.array(values, type=pyarrow.float64())])
    assert (
        module_under_test._floats_exactly_fit_integer_decimal(
            col, pyarrow.decimal128(precision, scale)
        )
        is expected
    )


class _FakeSQLBackend:
    """Minimal SQL backend stub for exercising _sql_to_pyarrow_batches + _base_to_pyarrow."""

    def __init__(self, rows):
        self._rows = rows

    def _import_pyarrow(self):
        import pyarrow

        return pyarrow

    def _run_pre_execute_hooks(self, expr):
        pass

    def _cursor_batches(self, expr, params=None, limit=None, chunk_size=1_000_000):
        yield self._rows

    to_pyarrow_batches = operations._sql_to_pyarrow_batches
    to_pyarrow = operations._base_to_pyarrow


@pytest.mark.parametrize(
    ("ibis_col_type", "agg_fn", "cursor_value", "expected_pa_type", "expected_py_val"),
    [
        # PostgreSQL/MySQL SUM(BIGINT) > 2^63 - 1 returned as Decimal: must not wrap int64.
        (
            "int64",
            lambda c: c.sum(),
            decimal.Decimal(2**63 + 100),
            "decimal128(19, 0)",
            decimal.Decimal(2**63 + 100),
        ),
        # Python int > 2^63 - 1 from cursor: _make_array converts to Decimal, preserved in Arrow.
        (
            "int64",
            lambda c: c.sum(),
            2**63 + 100,
            "decimal128(19, 0)",
            decimal.Decimal(2**63 + 100),
        ),
        # PostgreSQL/MySQL SUM(BIGINT) fitting in int64: cast cleanly to int64.
        (
            "int64",
            lambda c: c.sum(),
            decimal.Decimal(12345),
            "int64",
            12345,
        ),
        # Oracle SUM(NUMBER(38)) exceeding decimal128(38, 2) integer digits: keep decimal128(38, 0).
        (
            "decimal(38, 2)",
            lambda c: c.sum(),
            decimal.Decimal("61728394506172839450617283945061728390"),
            "decimal128(38, 0)",
            decimal.Decimal("61728394506172839450617283945061728390"),
        ),
        # Wider integer from cursor overflowing int32 target: keep int64 rather than wrapping.
        (
            "int32",
            lambda c: c.min(),
            2**31 + 5,
            "int64",
            2**31 + 5,
        ),
    ],
)
def test_base_to_pyarrow_preserves_overflowing_numerics(
    ibis_col_type, agg_fn, cursor_value, expected_pa_type, expected_py_val
):
    t = ibis.table([("col", ibis_col_type)], name="t")
    expr = t.aggregate(agg_val=agg_fn(t.col))
    backend = _FakeSQLBackend([(cursor_value,)])

    result = backend.to_pyarrow(expr)

    assert str(result["agg_val"].type) == expected_pa_type
    assert result["agg_val"][0].as_py() == expected_py_val
