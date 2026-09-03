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

import decimal
import textwrap

import pytest
import ibis
import ibis.expr.datatypes as dt

# Import required in order to register operations.
import third_party.ibis.ibis_addon.operations  # noqa: F401


@pytest.fixture
def bigquery_client():
    return ibis.bigquery.connect()


def test_bit_xor_bigquery(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data", schema="new_york_citibike"
    )
    expr = tbl["tripduration"].bit_xor().name("checksum")
    sql = expr.compile()
    assert sql == textwrap.dedent("""
    SELECT
      BIT_XOR(t0.`tripduration`) AS `checksum`
    FROM `bigquery-public-data`.new_york_citibike.citibike_trips AS t0
    """).strip()


def test_hash_bigquery_string(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data", schema="new_york_citibike"
    )
    expr = tbl[tbl["start_station_name"].hash().name("station_hash")]
    sql = expr.compile()
    assert sql == textwrap.dedent("""
    SELECT
      farm_fingerprint(t0.`start_station_name`) AS `station_hash`
    FROM `bigquery-public-data`.new_york_citibike.citibike_trips AS t0
    """).strip()


def test_hash_bigquery_binary(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data", schema="new_york_citibike"
    )
    expr = tbl[tbl["start_station_name"].cast(dt.binary).hash().name("station_hash")]
    sql = expr.compile()
    # TODO: Update the expected SQL to be a valid query once
    #       https://github.com/ibis-project/ibis/issues/2354 is fixed.
    assert sql == textwrap.dedent("""
    SELECT
      farm_fingerprint(FROM_HEX(t0.`start_station_name`)) AS `station_hash`
    FROM `bigquery-public-data`.new_york_citibike.citibike_trips AS t0
    """).strip()


def test_hashbytes_bigquery_string(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data", schema="new_york_citibike"
    )
    expr = tbl[tbl["start_station_name"].hashbytes(how="sha256").name("station_hash")]
    sql = expr.compile()
    assert sql == textwrap.dedent("""
    SELECT
      TO_HEX(SHA256(t0.`start_station_name`)) AS `station_hash`
    FROM `bigquery-public-data`.new_york_citibike.citibike_trips AS t0
    """).strip()


def test_hashbytes_bigquery_binary(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data", schema="new_york_citibike"
    )
    expr = tbl[
        tbl["start_station_name"]
        .cast(dt.binary)
        .hashbytes(how="sha256")
        .name("station_hash")
    ]
    sql = expr.compile()
    # TODO: Update the expected SQL to be a valid query once
    #       https://github.com/ibis-project/ibis/issues/2354 is fixed.
    assert sql == textwrap.dedent("""
    SELECT
      TO_HEX(SHA256(FROM_HEX(t0.`start_station_name`))) AS `station_hash`
    FROM `bigquery-public-data`.new_york_citibike.citibike_trips AS t0
    """).strip()


def test_decimal_literal_bigquery(bigquery_client):
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

    expr_large = t.filter(t.id.isin(large_vals))
    expr_small = t.filter(t.small_dec.isin(small_vals))

    sql_large = bigquery_client.compile(expr_large)
    sql_small = bigquery_client.compile(expr_small)

    assert "323456789012345678901234567890" in sql_large
    assert "BIGNUMERIC" in sql_large
    assert "E+" not in sql_large

    assert "123.456" in sql_small
    assert "NUMERIC" in sql_small


def test_decimal_literal_db2():
    import sqlalchemy as sa
    from third_party.ibis.ibis_db2.compiler import Db2Compiler

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


def test_decimal_literal_mssql():
    import sqlalchemy as sa
    from ibis.backends.mssql.compiler import MsSqlCompiler

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


def test_null_decimal_literal():
    from ibis.backends.mssql.compiler import MsSqlCompiler
    from third_party.ibis.ibis_db2.compiler import Db2Compiler

    expr = ibis.literal(None, type=dt.Decimal(10, 2))
    # Test that compiling NULL decimal literal does not raise TypeError
    mssql_res = MsSqlCompiler.to_ast(expr.op()).queries[0]._translate(expr.op())
    db2_res = Db2Compiler.to_ast(expr.op()).queries[0]._translate(expr.op())

    assert str(mssql_res) == "NULL"
    assert str(db2_res) == "NULL"
