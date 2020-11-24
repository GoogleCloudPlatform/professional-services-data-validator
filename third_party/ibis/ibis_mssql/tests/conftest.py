import os

import pytest

import ibis

MSSQL_USER = os.environ.get(
    'IBIS_TEST_MSSQL_USER', os.environ.get('MSSQLUSER', 'user')
)
MSSQL_PASS = os.environ.get(
    'IBIS_TEST_MSSQL_PASSWORD', os.environ.get('MSSQLPASSWORD', 'pas')
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
