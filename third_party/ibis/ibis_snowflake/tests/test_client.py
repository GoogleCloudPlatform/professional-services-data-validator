# Copyright(C) 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0(the "License"); you may not
# use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the
# License for the specific language governing permissions and limitations under
# the License.

import os

import ibis
import ibis.expr.types as ir
import pandas as pd
import pytest
from ibis.tests.util import assert_equal

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('snowflake')

pytestmark = pytest.mark.snowflake

IBIS_SNOWFLAKE_ACCOUNT = os.environ.get('IBIS_TEST_SNOWFLAKE_ACCOUNT', 'MYACCOUNT')
IBIS_SNOWFLAKE_USER = os.environ.get('IBIS_TEST_SNOWFLAKE_USER', 'USER')
IBIS_SNOWFLAKE_PASS = os.environ.get('IBIS_TEST_SNOWFLAKE_PASSWORD', 'PASSWORD')
IBIS_SNOWFLAKE_DATABASE = os.environ.get('IBIS_TEST_SNOWFLAKE_DATABASE', 'TEST')
IBIS_SNOWFLAKE_SCHEMA = os.environ.get('IBIS_TEST_SNOWFLAKE_SCHEMA', 'PUBLIC')


def test_table(alltypes):
    assert isinstance(alltypes, ir.TableExpr)


def test_array_execute(alltypes):
    d = alltypes.limit(10).cc_name
    s = d.execute()
    assert isinstance(s, pd.Series)
    assert len(s) == 10


def test_literal_execute(con):
    expr = ibis.literal('1234')
    result = con.execute(expr)
    assert result == '1234'


def test_simple_aggregate_execute(alltypes):
    d = alltypes.cc_tax_percentage.sum()
    v = d.execute()
    assert isinstance(v, float)


def test_list_tables(con):
    assert len(con.list_tables()) > 0
    assert len(con.list_tables(like='it')) == 1


def test_compile_verify(alltypes):
    unsupported_expr = alltypes.cc_tax_percentage.approx_median()
    assert not unsupported_expr.verify()

    supported_expr = alltypes.cc_tax_percentage.sum()
    assert supported_expr.verify()


def test_database_layer(con, alltypes):
    db = con.database()
    t = db.item

    assert_equal(t, alltypes)

    assert db.list_tables() == con.list_tables()

    db_schema = con.schema("PUBLIC")

    assert db_schema.list_tables() == con.list_tables()


def test_compile_toplevel():
    t = ibis.table([('cc_gmt_offset', 'float64')], name='t0')

    expr = t.cc_gmt_offset.sum()
    result = ibis.snowflake.compile(expr)
    expected = "SELECT sum(t0.cc_gmt_offset) AS sum \nFROM t0 AS t0"  # noqa

    assert str(result) == expected


def test_list_databases(con):
    assert IBIS_SNOWFLAKE_DATABASE is not None
    assert 'DEMO_DB' in con.list_database()


def test_list_schemas(con):
    assert 'public' in con.list_schemas()
    assert 'information_schema' in con.list_schemas()


def test_metadata_is_per_table():
    con = ibis.snowflake.connect(
        user=IBIS_SNOWFLAKE_USER,
        password=IBIS_SNOWFLAKE_PASS,
        database=IBIS_SNOWFLAKE_DATABASE,
        schema=IBIS_SNOWFLAKE_SCHEMA,
        account=IBIS_SNOWFLAKE_ACCOUNT
    )
    assert len(con.meta.tables) == 0

    # assert that we reflect only when a table is requested
    t = con.table('ITEM')  # noqa
    assert 'ITEM' in con.meta.tables
    assert len(con.meta.tables) == 1


def test_schema_table():
    con = ibis.snowflake.connect(
        user=IBIS_SNOWFLAKE_USER,
        password=IBIS_SNOWFLAKE_PASS,
        database=IBIS_SNOWFLAKE_DATABASE,
        schema=IBIS_SNOWFLAKE_SCHEMA,
        account=IBIS_SNOWFLAKE_ACCOUNT
    )
    schema = con.schema('PUBLIC')

    assert isinstance(schema['ITEM'], ir.TableExpr)