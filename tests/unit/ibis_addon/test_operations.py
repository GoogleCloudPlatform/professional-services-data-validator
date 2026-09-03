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
import ibis.expr.datatypes as dt
import pandas
import pytest
import sqlalchemy as sa
from ibis.backends.bigquery.compiler import BigQueryCompiler
from ibis.backends.mssql.compiler import MsSqlCompiler

from third_party.ibis.ibis_addon import operations

try:
    from third_party.ibis.ibis_db2.compiler import Db2Compiler
except Exception as exc:
    Db2Compiler = None
    DB2_SKIP_REASON = f"Db2 compiler unavailable: {exc}"
else:
    DB2_SKIP_REASON = ""

try:
    from third_party.ibis.ibis_sybase.compiler import SybaseCompiler
except Exception as exc:
    SybaseCompiler = None
    SYBASE_SKIP_REASON = f"Sybase compiler unavailable: {exc}"
else:
    SYBASE_SKIP_REASON = ""

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


def test_bigquery_decimal_literal_compiles_without_scientific_notation():
    schema = ibis.schema(
        {
            "id": dt.Decimal(76, 38),
            "small_dec": dt.Decimal(38, 9),
        }
    )
    t = ibis.table(schema, name="dvt_large_decimals")
    large_vals = [
        decimal.Decimal("323456789012345678901234567890"),
        decimal.Decimal("223456789012345678901234567890"),
    ]
    small_vals = [
        decimal.Decimal("123.456"),
        decimal.Decimal("789.012"),
    ]

    sql_large = str(BigQueryCompiler().to_sql(t.filter(t.id.isin(large_vals))))
    sql_small = str(BigQueryCompiler().to_sql(t.filter(t.small_dec.isin(small_vals))))

    assert "323456789012345678901234567890" in sql_large
    assert "BIGNUMERIC" in sql_large
    assert "E+" not in sql_large

    assert "123.456" in sql_small
    assert "NUMERIC" in sql_small


@pytest.mark.skipif(Db2Compiler is None, reason=DB2_SKIP_REASON)
def test_db2_decimal_literal_compiles_without_scientific_notation():
    schema = ibis.schema({"id": dt.Decimal(31, 0)})
    t = ibis.table(schema, name="dvt_large_decimals")
    vals = [decimal.Decimal("223456789012345678901234567890")]
    expr = t.filter(t.id.isin(vals))

    ast = Db2Compiler.to_ast(expr.op())
    query = ast.queries[0]
    metadata = sa.MetaData()
    sa_t = sa.Table("dvt_large_decimals", metadata, sa.Column("id", sa.DECIMAL(31, 0)))
    query.context.set_ref(t.op(), sa_t)

    w = query._translate(query.where[0])
    compiled = str(w.compile(compile_kwargs={"literal_binds": True}))
    assert "223456789012345678901234567890" in compiled
    assert "E+" not in compiled


def test_mssql_decimal_literal_compiles_without_scientific_notation():
    schema = ibis.schema({"id": dt.Decimal(38, 0)})
    t = ibis.table(schema, name="dvt_large_decimals")
    vals = [decimal.Decimal("223456789012345678901234567890")]
    expr = t.filter(t.id.isin(vals))

    ast = MsSqlCompiler.to_ast(expr.op())
    query = ast.queries[0]
    metadata = sa.MetaData()
    sa_t = sa.Table("dvt_large_decimals", metadata, sa.Column("id", sa.DECIMAL(38, 0)))
    query.context.set_ref(t.op(), sa_t)

    w = query._translate(query.where[0])
    compiled = str(
        w.compile(
            dialect=sa.dialects.mssql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "223456789012345678901234567890" in compiled
    assert "E+" not in compiled


@pytest.mark.skipif(SybaseCompiler is None, reason=SYBASE_SKIP_REASON)
def test_sybase_decimal_literal_compiles_without_scientific_notation():
    schema = ibis.schema({"id": dt.Decimal(38, 0)})
    t = ibis.table(schema, name="dvt_large_decimals")
    vals = [decimal.Decimal("223456789012345678901234567890")]
    expr = t.filter(t.id.isin(vals))

    ast = SybaseCompiler.to_ast(expr.op())
    query = ast.queries[0]
    metadata = sa.MetaData()
    sa_t = sa.Table("dvt_large_decimals", metadata, sa.Column("id", sa.DECIMAL(38, 0)))
    query.context.set_ref(t.op(), sa_t)

    w = query._translate(query.where[0])
    compiled = str(w.compile(compile_kwargs={"literal_binds": True}))
    assert "223456789012345678901234567890" in compiled
    assert "E+" not in compiled


def test_null_decimal_literal():
    expr = ibis.literal(None, type=dt.Decimal(10, 2))
    # Test that compiling NULL decimal literal does not raise TypeError
    mssql_res = MsSqlCompiler.to_ast(expr.op()).queries[0]._translate(expr.op())
    assert str(mssql_res) == "NULL"

    if Db2Compiler is not None:
        db2_res = Db2Compiler.to_ast(expr.op()).queries[0]._translate(expr.op())
        assert str(db2_res) == "NULL"

    if SybaseCompiler is not None:
        sybase_res = SybaseCompiler.to_ast(expr.op()).queries[0]._translate(expr.op())
        assert str(sybase_res) == "NULL"
