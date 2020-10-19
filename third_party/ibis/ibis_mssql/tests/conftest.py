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

import pytest

import ibis

MSSQL_USER = os.environ.get(
    'IBIS_TEST_MSSQL_USER', os.environ.get('MSSQLUSER', 'user')
)
MSSQL_PASS = os.environ.get(
    'IBIS_TEST_MSSQL_PASSWORD', os.environ.get('MSSQLPASSWORD', 'password')
)
MSSQL_HOST = os.environ.get(
    'IBIS_TEST_MSSQL_HOST', os.environ.get('MSSQLHOST', 'host')
)
MSSQL_PORT = os.environ.get(
    'IBIS_TEST_MSSQL_PORT', os.environ.get('MSSQLPORT', 'port')
)
MSSQL_DB = os.environ.get(
    'IBIS_TEST_MSSQL_DATABASE', os.environ.get('MSSQLDATABASE', 'db_name')
)


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, ibis.util.guid())


@pytest.fixture(scope='session')
def con():
    return third_party.ibis.ibis_mssql.api.connect(
        host=MSSQL_HOST,
        user=MSSQL_USER,
        password=MSSQL_PASS,
        database=MSSQL_DB,
        port=MSSQL_PORT,
    )


@pytest.fixture(scope='module')
def db(con):
    return con.database()


@pytest.fixture(scope='module')
def alltypes(db):
    return db.functional_alltypes


@pytest.fixture(scope='module')
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope='module')
def at(alltypes):
    return alltypes.op().sqla_table


@pytest.fixture(scope='module')
def batting(db):
    return db.batting


@pytest.fixture(scope='module')
def batting_df(batting):
    return batting.execute()


@pytest.fixture(scope='module')
def awards_players(db):
    return db.awards_players


@pytest.fixture(scope='module')
def awards_players_df(awards_players):
    return awards_players.execute()


@pytest.fixture
def translate():
    from third_party.ibis.ibis_mssql.compiler import MSSQLDialect

    dialect = MSSQLDialect()
    context = dialect.make_context()
    return lambda expr: dialect.translator(expr, context).get_result()


@pytest.fixture
def temp_table(con) -> str:
    name = _random_identifier('table')
    try:
        yield name
    finally:
        con.drop_table(name, force=True)
