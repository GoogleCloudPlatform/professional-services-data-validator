from third_party.ibis.ibis_oracle.client import OracleClient
from third_party.ibis.ibis_oracle.compiler import (  # noqa: F401
    dialect,
    rewrites,
)

from ibis.sql.alchemy import to_sqlalchemy


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
