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

from typing import Optional
import ibis.common.exceptions as com
from third_party.ibis.ibis_snowflake.client import SnowflakeClient  # TODO make non local
from ibis.config import options
from ibis.sql.alchemy import to_sqlalchemy
from third_party.ibis.ibis_snowflake.compiler import dialect


def compile(expr, params=None):
    """Compile an ibis expression to the Snowflake target.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The ibis expression to compile
    params : dict or None
        ``dict`` mapping :class:`ibis.expr.types.ScalarParameter` objects to
        values

    Returns
    -------
    sqlalchemy_expression : sqlalchemy.sql.expression.ClauseElement

    Examples
    --------
    >>> import os
    >>> import getpass
    >>> user = os.environ.get('IBIS_TEST_SNOWFLAKE_USER', getpass.getuser())
    >>> password = os.environ.get('IBIS_TEST_SNOWFLAKE_PASSWORD')
    >>> account = os.environ.get('IBIS_TEST_SNOWFLAKE_ACCOUNT')
    >>> database = os.environ.get('IBIS_TEST_SNOWFLAKE_DATABASE',
    ...                           'ibis_testing')
    >>> con = connect(
    ...     database=database,
    ...     account=account,
    ...     user=user,
    ...     password=password
    ... )
    >>> t = con.table('functional_alltypes')
    >>> expr = t.double_col + 1
    >>> sqla = compile(expr)
    >>> print(str(sqla))  # doctest: +NORMALIZE_WHITESPACE
    SELECT t0.double_col + %(param_1)s AS tmp
    FROM functional_alltypes AS t0
    """
    return to_sqlalchemy(expr, dialect.make_context(params=params))


def connect(
    user=None,
    password=None,
    database=None,
    account=None,
    schema=None,
    url=None
):
    """Create an Ibis client located at `user`:`password`@`host`:`port`
    connected to a Snowflake database named `database`.

    Parameters
    ----------
    user : string, default None
    password : string, default None
    account : string or integer, default 5432
    database : string, default None
    url : string, default None
        Complete SQLAlchemy connection string. If passed, the other connection
        arguments are ignored.

    Returns
    -------
    SnowflakeClient

    Examples
    --------
    >>> import os
    >>> import getpass
    >>> user = os.environ.get('IBIS_TEST_SNOWFLAKE_USER', getpass.getuser())
    >>> password = os.environ.get('IBIS_TEST_SNOWFLAKE_PASSWORD')
    >>> account = os.environ.get('IBIS_TEST_SNOWFLAKE_ACCOUNT')
    >>> database = os.environ.get('IBIS_TEST_SNOWFLAKE_DATABASE',
    ...                           'ibis_testing')
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
    SnowflakeTable[table]
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
        double_col : float64
        date_string_col : string
        string_col : string
        timestamp_col : timestamp
        year : int32
        month : int32
    """

    return SnowflakeClient(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        url=url
    )