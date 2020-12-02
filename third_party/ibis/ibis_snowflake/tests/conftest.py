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

import pytest

import ibis

IBIS_SNOWFLAKE_USER = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_USER', os.environ.get('SF_USER', 'snowflake')
)
IBIS_SNOWFLAKE_PASS = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_PASSWORD', os.environ.get('SF_PASWORD', 'snowflake')
)
IBIS_SNOWFLAKE_ACCOUNT = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_ACCOUNT', os.environ.get('SF_ACCOUNT', 'myAccount')
)
IBIS_SNOWFLAKE_SCHEMA = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_SCHEMA', os.environ.get('SF_SCHEMA', 'PUBLIC')
)
IBIS_SNOWFLAKE_DATABASE = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_DATABASE', os.environ.get('SF_DB', 'TEST')
)


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, ibis.util.guid())


@pytest.fixture(scope='session')
def con():
    return ibis.snowflake.connect(
        user=IBIS_SNOWFLAKE_USER,
        password=IBIS_SNOWFLAKE_PASS,
        database=IBIS_SNOWFLAKE_DATABASE,
        account=IBIS_SNOWFLAKE_ACCOUNT,
        schema=IBIS_SNOWFLAKE_SCHEMA,
    )


@pytest.fixture(scope='module')
def db(con):
    return con.database()


@pytest.fixture(scope='module')
def alltypes(db):
    return db.functional_alltypes


@pytest.fixture(scope='module')
def demonstration(db):
    return db.demonstration1


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
    from ibis.snowflake.compiler import SnowflakeDialect

    dialect = SnowflakeDialect()
    context = dialect.make_context()
    return lambda expr: dialect.translator(expr, context).get_result()


@pytest.fixture
def temp_table(con) -> str:
    """
    Return a temporary table name.

    Parameters
    ----------
    con : ibis.snowflake.client.SnowflakeClient

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
