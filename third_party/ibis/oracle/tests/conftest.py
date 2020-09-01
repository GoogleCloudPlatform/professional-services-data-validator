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

import pytest

import ibis
import ibis.sql.oracle.api
OL_USER = os.environ.get(
    'IBIS_TEST_ORACLE_USER', os.environ.get('OLUSER', 'ADMIN')
)
OL_PASS = os.environ.get(
    'IBIS_TEST_ORACLE_PASSWORD', os.environ.get('OLPASSWORD', 'password')
)
IBIS_TEST_ORACLE_DB = os.environ.get(
    'IBIS_TEST_ORACLE_DATABASE', os.environ.get('OLDATABASE', 'ibis_testing')
)


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, ibis.util.guid())


@pytest.fixture(scope='session')
def con():
    return ibis.sql.oracle.api.connect(
        user=OL_USER,
        password=OL_PASS,
        database=IBIS_TEST_ORACLE_DB,
    )
    


@pytest.fixture(scope='module')
def db(con):
    return con.database()


@pytest.fixture(scope='module')
def alltypes(db):
    return db.functional_alltypes


@pytest.fixture(scope='module')
def geotable(con):
    return con.table('geo')


@pytest.fixture(scope='module')
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope='module')
def gdf(geotable):
    return geotable.execute()


@pytest.fixture(scope='module')
def at(alltypes):
    return alltypes.op().sqla_table


@pytest.fixture(scope='module')
def intervals(con):
    return con.table("intervals")


@pytest.fixture
def translate():
    from ibis.sql.oracle.compiler import OracleDialect

    dialect = OracleDialect()
    context = dialect.make_context()
    return lambda expr: dialect.translator(expr, context).get_result()


@pytest.fixture
def temp_table(con) -> str:
    """
    Return a temporary table name.

    Parameters
    ----------
    con : ibis.oracle.OracleDialect

    Yields
    ------
    name : string
        Random table name for a temporary usage.
    """
    name = _random_identifier('table')
    try:
        yield name
    finally:
        con.drop_table(name, force=True)
