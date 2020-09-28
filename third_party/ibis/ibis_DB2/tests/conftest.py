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
import third_party.ibis.ibis_DB2.api

DB2_USER = os.environ.get(
    'IBIS_TEST_DB2_USER', os.environ.get('DB2USER', 'user')
)
DB2_PASS = os.environ.get(
    'IBIS_TEST_DB2_PASSWORD', os.environ.get('DB2PASSWORD', 'password')
)

DB2_HOST = os.environ.get(
    'IBIS_TEST_DB2_HOST', os.environ.get('DB2HOST', 'localhost')
)
DB2_PORT = os.environ.get(
    'IBIS_TEST_DB2_PORT', os.environ.get('DB2PORT', 50000)
)

IBIS_TEST_DB2_DB = os.environ.get(
    'IBIS_TEST_DB2_DATABASE', os.environ.get('DB2DATABASE', 'dbname')
)


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, ibis.util.guid())


@pytest.fixture(scope='session')
def con():
    return third_party.ibis.ibis_DB2.api.connect(
        host=DB2_HOST,
        user=DB2_USER,
        password=DB2_PASS,
        database=IBIS_TEST_DB2_DB,
        port=DB2_PORT,
    )


@pytest.fixture(scope='module')
def db(con):
    return con.database()


@pytest.fixture(scope='module')
def alltypes(db):
    return db.FUNCTIONAL_ALLTYPES


@pytest.fixture(scope='module')
def pointer(db):
    return db.STUDENTS_POINTER


@pytest.fixture(scope='module')
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope='module')
def pdf(pointer):
    return pointer.execute()


@pytest.fixture(scope='module')
def at(alltypes):
    return alltypes.op().sqla_table



@pytest.fixture
def translate():
    from third_party.ibis.ibis_DB2.compiler import DB2Dialect

    dialect = DB2Dialect()
    context = dialect.make_context()
    return lambda expr: dialect.translator(expr, context).get_result()


@pytest.fixture
def temp_table(con) -> str:
    """
    Return a temporary table name.

    Parameters
    ----------
    con : third_party.ibis.ibis_DB2.compiler.DB2Dialect

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
