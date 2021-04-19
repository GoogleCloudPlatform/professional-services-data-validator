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


from third_party.ibis.ibis_mssql.client import MSSQLClient
from third_party.ibis.ibis_mssql.compiler import (  # noqa: F401, E501
    compiles,
    dialect,
    rewrites,
)
from ibis.backends.base_sqlalchemy.alchemy import to_sqlalchemy

def compile(expr, params=None):
    """Compile an ibis expression to the MSSQL target.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The ibis expression to compile
    params : dict, optional
        ``dict`` mapping :class:`ibis.expr.types.ScalarParameter` objects to
        values

    Returns
    -------
    sqlalchemy_expression : sqlalchemy.sql.expression.ClauseElement

    Examples
    --------
    >>> import os
    >>> import getpass
    >>> host = os.environ.get('IBIS_TEST_MSSQL_HOST', 'localhost')
    >>> user = os.environ.get('IBIS_TEST_MSSQL_USER', getpass.getuser())
    >>> password = os.environ.get('IBIS_TEST_MSSQL_PASSWORD')
    >>> database = os.environ.get('IBIS_TEST_MSSQL_DATABASE', 'master')
    >>> con = connect(
    ...     database=database,
    ...     host=host,
    ...     user=user,
    ...     password=password,
    ... )
    >>> t = con.table('functional_alltypes')
    >>> expr = t.double_col + 1
    >>> sqla = compile(expr)
    >>> print(str(sqla))  # doctest: +NORMALIZE_WHITESPACE
    SELECT t0.double_col + ? AS tmp
    FROM functional_alltypes AS t0
    """
    return to_sqlalchemy(expr, dialect.make_context(params=params))


def connect(
    host='localhost',
    user=None,
    password=None,
    port=1433,
    database='master',
    driver='pyodbc',
    odbc_driver='ODBC Driver 17 for SQL Server',
    url=None,
):
    """Create an Ibis client connected to a MSSQL database.

    Parameters
    ----------
    host : string, default 'localhost'
    user : string, optional
    password : string, optional
    port : string or integer, default 1433
    database : string, default 'master'
    url : string, optional
        Complete SQLAlchemy connection string. If passed, the other connection
        arguments are ignored.
    driver : string, default 'pyodbc'
    odbc_driver : string, default 'ODBC Driver 17 for SQL Server'

    Returns
    -------
    MSSQLClient

    Examples
    --------
    >>> import os
    >>> import getpass
    >>> host = os.environ.get('IBIS_TEST_MSSQL_HOST', 'localhost')
    >>> user = os.environ.get('IBIS_TEST_MSSQL_USER', getpass.getuser())
    >>> password = os.environ.get('IBIS_TEST_MSSQL_PASSWORD')
    >>> database = os.environ.get('IBIS_TEST_MSSQL_DATABASE', 'master')
    >>> con = connect(
    ...     database=database,
    ...     host=host,
    ...     user=user,
    ...     password=password
    ... )
    >>> con.list_tables()  # doctest: +ELLIPSIS
    [...]
    >>> t = con.table('functional_alltypes')
    >>> t
    MSSQLTable[table]
      name: functional_alltypes
      schema:
        index : int64
        Unnamed: 0 : int64
        id : int32
        bool_col : boolean
        tinyint_col : int16
        smallint_col : int16
        int_col : int32
        bigint_col : int64
        float_col : float32
        double_col : float32
        date_string_col : string
        string_col : string
        timestamp_col : timestamp
        year : int32
        month : int32
    """
    return MSSQLClient(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        url=url,
        driver=driver,
        odbc_driver=odbc_driver,
    )
