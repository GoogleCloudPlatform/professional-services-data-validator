# Copyright 2020 Google Inc.
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

import numpy
import pandas as pd
import pytest

import ibis
import ibis.expr.types as ir
from ibis.tests.util import assert_equal

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('ibm_db_sa')

pytestmark = pytest.mark.DB2

DB2_TEST_DB = os.environ.get('IBIS_TEST_DB2_DATABASE', 'dbname')
IBIS_DB2_HOST = os.environ.get('IBIS_TEST_DB2_HOST', 'localhost')
IBIS_DB2_USER = os.environ.get('IBIS_TEST_DB2_USER', 'user')
IBIS_DB2_PASS = os.environ.get('IBIS_TEST_DB2_PASSWORD', 'password')


def test_table(alltypes):
    assert isinstance(alltypes, ir.TableExpr)


def test_array_execute(alltypes):
    d = alltypes.limit(15).smallint_col
    s = d.execute()
    assert isinstance(s, pd.Series)
    assert len(s) == 6


def test_simple_aggregate_execute(alltypes):
    d = alltypes.smallint_col.sum()
    v = d.execute()
    assert isinstance(v,numpy.int64)


# To get list of tables based on some pattern
# use the same in the 'like' section.
# Currently we have used STORES
def test_list_tables(con):
    assert len(con.list_tables()) > 0
    assert len(con.list_tables(like='functional_alltypes')) == 1


def test_compile_verify(alltypes):
    unsupported_expr = alltypes.smallint_col.approx_median()
    assert not unsupported_expr.verify()

    supported_expr = alltypes.smallint_col.sum()
    assert supported_expr.verify()


def test_database_layer(con, alltypes):
    db = con.database()
    t = db.FUNCTIONAL_ALLTYPES

    assert_equal(t, alltypes)

    assert db.list_tables() == con.list_tables()

    db_schema = con.schema("db2inst1")

    assert db_schema.list_tables() == con.list_tables()


def test_compile_toplevel():
    t = ibis.table([('foo', 'double')], name='t0')

    expr = t.foo.sum()
    result = third_party.ibis.ibis_DB2.api.compile(expr)
    expected = "SELECT sum(t0.foo) AS sum \nFROM t0 AS t0"  # noqa

    assert str(result) == expected

# Replace dbName with your database name 
def test_list_databases(con):
    assert DB2_TEST_DB is not None
    assert 'dbName' in con.list_databases()


def test_list_schemas(con):
    assert 'db2inst1' in con.list_schemas()


def test_metadata_is_per_table():
    con = third_party.ibis.ibis_DB2.api.connect(
        host=IBIS_DB2_HOST,
        user=IBIS_DB2_USER,
        password=IBIS_DB2_PASS,
        database=DB2_TEST_DB,
    )
    assert len(con.meta.tables) == 0

    # assert that we reflect only when a table is requested.
    # Enter table name in the con.table function
    t = con.table('students_pointer')  # noqa
    assert 'students_pointer' in con.meta.tables
    assert len(con.meta.tables) == 1


def test_schema_table():
    con = third_party.ibis.ibis_DB2.api.connect(
        host=IBIS_DB2_HOST,
        user=IBIS_DB2_USER,
        password=IBIS_DB2_PASS,
        database=DB2_TEST_DB,
    )

    # db2inst1 will always be there inany of the DB2 database.
    # To get schema of particular table please mention
    # table name in 'isinstance(schema['table_name']))'
    schema = con.schema('db2inst1')

    assert isinstance(schema['students_pointer'], ir.TableExpr)
