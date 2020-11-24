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
import third_party.ibis.ibis_oracle.api


OL_HOST= os.environ.get(
    'IBIS_TEST_ORACLE_HOST', os.environ.get('OLHOST','host')
)
OL_PORT= os.environ.get(
    'IBIS_TEST_ORACLE_PORT', os.environ.get('OLPORT','port')
)
OL_USER = os.environ.get(
    'IBIS_TEST_ORACLE_USER', os.environ.get('OLUSER', 'username')
)
OL_PASS = os.environ.get(
    'IBIS_TEST_ORACLE_PASSWORD', os.environ.get('OLPASSWORD', 'password')
)
IBIS_TEST_ORACLE_DB = os.environ.get(
    'IBIS_TEST_ORACLE_DATABASE', os.environ.get('OLDATABASE', 'database_name')
)
IBIS_TEST_ORACLE_protocol = os.environ.get(
    'IBIS_TEST_ORACLE_PROTOCOL', os.environ.get('OLPROTOCOL', 'protocol')
)


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, ibis.util.guid())


@pytest.fixture(scope='session')
def con():
    return third_party.ibis.ibis_oracle.api.connect(
        host=OL_HOST,
        port=OL_PORT,
        user=OL_USER,
        password=OL_PASS,
        database=IBIS_TEST_ORACLE_DB,
        protocol=IBIS_TEST_ORACLE_protocol,
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
def intervals(con):
    return con.table("intervals")


@pytest.fixture
def translate():
    from third_party.ibis.ibis_oracle.compiler import OracleDialect

    dialect = OracleDialect()
    context = dialect.make_context()
    return lambda expr: dialect.translator(expr, context).get_result()


@pytest.fixture
def temp_table(con) -> str:
    """
    Return a temporary table name.

    Parameters
    ----------
    con : third_party.ibis.ibis_oracle.compiler.OracleDialect

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
