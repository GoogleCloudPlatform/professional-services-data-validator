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

from contextlib import contextmanager
import decimal

import ibis
import pyarrow
import pytest

from third_party.ibis.ibis_addon import operations  # noqa: F401
from third_party.ibis.ibis_pyarrow import execution


@pytest.fixture
def module_under_test():
    return execution


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

    def __init__(self, batches):
        # Accept either a single list of row tuples or a list of batches.
        if batches and isinstance(batches[0], tuple):
            self._batches = [batches]
        else:
            self._batches = batches

    def _import_pyarrow(self):
        return pyarrow

    def _run_pre_execute_hooks(self, expr):
        pass

    def _cursor_batches(self, expr, params=None, limit=None, chunk_size=1_000_000):
        yield from self._batches

    to_pyarrow_batches = execution._sql_to_pyarrow_batches
    to_pyarrow = execution._base_to_pyarrow


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
        # Float cursor exceeding int64 (e.g. Teradata SUM(BIGINT)) or fractional: keep float64.
        (
            "int64",
            lambda c: c.sum(),
            1e20,
            "double",
            1e20,
        ),
        (
            "int64",
            lambda c: c.min(),
            1.5,
            "double",
            1.5,
        ),
        # Whole-number float fitting in int64: cast cleanly to int64.
        (
            "int64",
            lambda c: c.min(),
            42.0,
            "int64",
            42,
        ),
        # SUM(NUMBER(38, 2)) exceeding 36 integer digits: keep decimal128(38, 0).
        (
            "decimal(38, 2)",
            lambda c: c.sum(),
            decimal.Decimal("61728394506172839450617283945061728390"),
            "decimal128(38, 0)",
            decimal.Decimal("61728394506172839450617283945061728390"),
        ),
        # 39-digit SUM(NUMBER(38, 0)) returned as decimal256(39, 0): keep decimal256(39, 0).
        (
            "decimal(38, 0)",
            lambda c: c.sum(),
            decimal.Decimal("123456789012345678901234567890123456789"),
            "decimal256(39, 0)",
            decimal.Decimal("123456789012345678901234567890123456789"),
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


def test_sql_to_pyarrow_batches_unifies_mismatched_batch_schemas():
    t = ibis.table(
        [
            ("nullable_int", "int64"),
            ("dec_var", "decimal(38, 3)"),
            ("big_dec", "decimal(38, 2)"),
            ("int_or_big", "int64"),
        ],
        name="t",
    )
    batches = [
        # Batch 0: null, decimal128(4, 2), 38-digit integer decimal, int64
        [(None, decimal.Decimal("12.34"), decimal.Decimal("1" + "0" * 37), 42)],
        # Batch 1: int64, decimal128(8, 3), scale-2 decimal (needs 40 digits -> decimal256), >int64
        [
            (
                99,
                decimal.Decimal("12345.678"),
                decimal.Decimal("0.25"),
                2**63 + 7,
            )
        ],
    ]
    backend = _FakeSQLBackend(batches)

    result = backend.to_pyarrow(t)

    assert result["nullable_int"].to_pylist() == [None, 99]
    assert result["dec_var"].to_pylist() == [
        decimal.Decimal("12.340"),
        decimal.Decimal("12345.678"),
    ]
    assert str(result["big_dec"].type) == "decimal256(40, 2)"
    assert result["big_dec"].to_pylist() == [
        decimal.Decimal("1" + "0" * 37),
        decimal.Decimal("0.25"),
    ]
    assert str(result["int_or_big"].type) == "decimal128(19, 0)"
    assert result["int_or_big"].to_pylist() == [
        decimal.Decimal(42),
        decimal.Decimal(2**63 + 7),
    ]


def test_snowflake_to_pyarrow_preserves_precision():
    """SnowflakeBackend.to_pyarrow should preserve overflowing sums and higher-scale averages."""

    class _FakeCursor:
        def __init__(self, arrow_table):
            self._arrow_table = arrow_table

        def fetch_arrow_all(self):
            return self._arrow_table

    class _FakeResult:
        def __init__(self, arrow_table):
            self.cursor = _FakeCursor(arrow_table)

    class _FakeConn:
        def __init__(self, arrow_table):
            self._arrow_table = arrow_table

        def execute(self, sql):
            return _FakeResult(self._arrow_table)

    class _FakeCompiler:
        def to_ast_ensure_limit(self, expr, limit, params=None):
            class _AST:
                def compile(self_inner):
                    return "SELECT 1"

            return _AST()

    class _FakeSnowflakeBackend:
        compiler = _FakeCompiler()

        def __init__(self, arrow_table):
            self._arrow_table = arrow_table

        def _run_pre_execute_hooks(self, expr):
            pass

        @contextmanager
        def begin(self):
            yield _FakeConn(self._arrow_table)

        to_pyarrow = execution._snowflake_to_pyarrow

    t = ibis.table(
        [("n38", "decimal(38, 0)"), ("n10_2", "decimal(10, 2)")],
        name="t",
    )
    expr = t.aggregate(
        sum_n38=t.n38.sum(),
        avg_n10_2=t.n10_2.mean(),
    )
    sf_arrow = pyarrow.table(
        {
            "SUM_N38": pyarrow.array(
                [decimal.Decimal("12345678901234567890123456789012345678")],
                type=pyarrow.decimal128(38, 0),
            ),
            "AVG_N10_2": pyarrow.array(
                [decimal.Decimal("12.34567890")],
                type=pyarrow.decimal128(16, 8),
            ),
        }
    )

    result = _FakeSnowflakeBackend(sf_arrow).to_pyarrow(expr)

    assert result.column_names == ["sum_n38", "avg_n10_2"]
    assert str(result["sum_n38"].type) == "decimal128(38, 0)"
    assert result["sum_n38"][0].as_py() == decimal.Decimal(
        "12345678901234567890123456789012345678"
    )
    assert str(result["avg_n10_2"].type) == "decimal128(16, 8)"
    assert result["avg_n10_2"][0].as_py() == decimal.Decimal("12.34567890")
    assert _FakeSnowflakeBackend(None).to_pyarrow(expr).num_rows == 0
