from ibis.sql.alchemy import to_sqlalchemy
from ibis.sql.oracle.client import OracleClient
from ibis.sql.oracle.compiler import dialect, rewrites  # noqa: F401


def compile(expr, params=None):
    return to_sqlalchemy(expr, dialect.make_context(params=params))


def connect(
    user=None, password=None, database=None, url=None, driver='cx_Oracle',
):

    return OracleClient(
        user=user,
        password=password,
        database=database,
        url=url,
        driver=driver,
    )

