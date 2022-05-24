"""BigQuery public API."""

from typing import Optional

import ibis.common.exceptions as com
from client import TeradataClient  # TODO make non local
from compiler import dialect  # TODO make non local
from ibis.config import options  # noqa: F401


__all__ = ("compile", "connect", "verify")


def compile(expr, params=None):
    """ Compile an expression for Teradata
    Returns
    -------
    compiled : str
    See Also
    --------
    ibis.expr.types.Expr.compile
    """
    from compiler import to_sql  # TODO make non local

    return to_sql(expr, dialect.make_context(params=params))


def verify(expr, params=None):
    """ Check if an expression can be compiled using Teradata """
    try:
        compile(expr, params=params)
        return True
    except com.TranslationError:
        return False


def connect(
    host: str, user_name: str, password: str, port: Optional[int] = 1025, logmech: Optional[str]='TD2'
) -> TeradataClient:
    """ Create a TeradataClient for use with Ibis.
    Parameters
    ----------
    host : str
        The Database host to connect to
    user_name : str
        A Database username to connect with
    password : str
        Password for supplied username
    port : Optional[int]
        The database port to connect to (default. 1025)
    logmech : Optional[str]
        Logmech flag to select with (default. TD2)
    Returns
    -------
    TeradataClient
    """

    return TeradataClient(host, user_name, password, port, logmech)
