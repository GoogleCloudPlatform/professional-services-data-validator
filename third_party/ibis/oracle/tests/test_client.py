# Copyright 2015 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import numpy as np
import pandas as pd
import pytest

import ibis
import ibis.expr.datatypes as dt
import ibis.sql.oracle.expr.datatypes as dts
import ibis.expr.types as ir
import ibis.sql.alchemy as alch  # noqa: E402
from ibis.tests.util import assert_equal
from ibis.sql.oracle.api import compile

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('cx_Oracle')

pytestmark = pytest.mark.oracle

ORACLE_TEST_DB = os.environ.get(
    'IBIS_TEST_ORACLE_DATABASE', 'oracle_database'
)
IBIS_ORACLE_USER = os.environ.get('IBIS_TEST_ORACLE_USER', 'user')
IBIS_ORACLE_PASS = os.environ.get('IBIS_TEST_ORACLE_PASSWORD', 'password')


def test_table(alltypes):
    assert isinstance(alltypes, ir.TableExpr)


def test_array_execute(alltypes):
    d = alltypes.limit(10).double_col
    s = d.execute()
    assert isinstance(s, pd.Series)
    assert len(s) == 5


def test_literal_execute(con):
    expr = ibis.literal('1234')
    result = con.execute(expr)
    assert result == '1234'


def test_simple_aggregate_execute(alltypes):
    d = alltypes.double_col.sum()
    v = d.execute()
    assert isinstance(v, float)


def test_list_tables(con):
    assert len(con.list_tables()) > 0
    assert len(con.list_tables(like='functional')) == 1


def test_compile_verify(alltypes):
    unsupported_expr = alltypes.double_col.approx_median()
    assert not unsupported_expr.verify()

    supported_expr = alltypes.double_col.sum()
    assert supported_expr.verify()


def test_database_layer(con, alltypes):
    db = con.database()
    t = db.functional_alltypes

    assert_equal(t, alltypes)

    assert db.list_tables() == con.list_tables()

    db_schema = con.schema("admin")

    assert db_schema.list_tables() == con.list_tables()


def test_compile_toplevel():
    t = ibis.table([('foo', 'double')], name='t0')

    expr = t.foo.sum()
    result = ibis.sql.oracle.api.compile(expr)
    expected = "SELECT sum(t0.foo) AS sum \nFROM t0 AS t0"  # noqa

    assert str(result) == expected


def test_list_databases(con):
    assert ORACLE_TEST_DB is not None
    assert 'EUK1POD' in con.list_databases()


def test_list_schemas(con):
    assert 'admin' in con.list_schemas()
    assert 'adbsnmp' in con.list_schemas()


def test_metadata_is_per_table():
    con = ibis.sql.oracle.api.connect(
        user=IBIS_ORACLE_USER,
        password=IBIS_ORACLE_PASS,
        database=ORACLE_TEST_DB,
    )
    assert len(con.meta.tables) == 0

    # assert that we reflect only when a table is requested
    t = con.table('functional_alltypes')  # noqa
    assert 'functional_alltypes' in con.meta.tables
    assert len(con.meta.tables) == 1


def test_schema_table():
    con = ibis.sql.oracle.api.connect(
        user=IBIS_ORACLE_USER,
        password=IBIS_ORACLE_PASS,
        database=ORACLE_TEST_DB,
    )

    # ensure that we can reflect the information schema (which is guaranteed
    # to exist)
    schema = con.schema('admin')

    assert isinstance(schema['students'], ir.TableExpr)




