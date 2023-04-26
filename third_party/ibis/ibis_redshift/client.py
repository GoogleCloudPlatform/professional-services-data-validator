import contextlib
import getpass
from typing import Optional
import parsy
import re
import ast
import toolz
import psycopg2  # NOQA fail early if the driver is missing
import sqlalchemy as sa

import ibis.backends.base_sqlalchemy.alchemy as alch
from ibis.backends.postgres import udf
import ibis.backends.postgres.client as postgres
from third_party.ibis.ibis_redshift.compiler import RedShiftSQLDialect
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from ibis import util


class RedShiftTable(postgres.PostgreSQLTable):
    pass


class RedShiftSchema(postgres.PostgreSQLSchema):
    pass


class RedShiftDatabase(postgres.PostgreSQLDatabase):
    schema_class = RedShiftSchema 


class RedShiftClient(alch.AlchemyClient):

    """The Ibis RedShift client class

    Attributes
    ----------
    con : sqlalchemy.engine.Engine
    """

    dialect = RedShiftSQLDialect
    database_class = RedShiftDatabase
    table_class = RedShiftTable

    def __init__(
        self,
        host: str = 'localhost',
        user: str = getpass.getuser(),
        password: Optional[str] = None,
        port: int = 5439,
        database: str = 'public',
        url: Optional[str] = None,
        driver: str = 'psycopg2',
    ):
        if url is None:
            if driver != 'psycopg2':
                raise NotImplementedError(
                    'psycopg2 is currently the only supported driver'
                )
            sa_url = sa.engine.url.URL(
                'postgresql+psycopg2',
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
            )
        else:
            sa_url = sa.engine.url.make_url(url)

        super().__init__(sa.create_engine(sa_url))
        self.database_name = sa_url.database

    @contextlib.contextmanager
    def begin(self):
        with super().begin() as bind:
            previous_timezone = bind.execute('SHOW TIMEZONE').scalar()
            bind.execute('SET TIMEZONE = UTC')
            try:
                yield bind
            finally:
                bind.execute("SET TIMEZONE = '{}'".format(previous_timezone))

    def database(self, name=None):
        """Connect to a database called `name`.

        Parameters
        ----------
        name : str, optional
            The name of the database to connect to. If ``None``, return
            the database named ``self.current_database``.

        Returns
        -------
        db : RedShiftDatabase

        Notes
        -----
        This creates a new connection if `name` is both not ``None`` and not
        equal to the current database.
        """
        if name == self.current_database or (
            name is None and name != self.current_database
        ):
            return self.database_class(self.current_database, self)
        else:
            url = self.con.url
            client_class = type(self)
            new_client = client_class(
                host=url.host,
                user=url.username,
                port=url.port,
                password=url.password,
                database=name,
            )
            return self.database_class(name, new_client)

    def schema(self, name):
        """Get a schema object from the current database for the schema named `name`.

        Parameters
        ----------
        name : str

        Returns
        -------
        schema : RedShiftSchema

        """
        return self.database().schema(name)

    @property
    def current_database(self):
        """The name of the current database this client is connected to."""
        return self.database_name

    def list_databases(self):
        # http://dba.stackexchange.com/a/1304/58517
        return [
            row.datname
            for row in self.con.execute(
                'SELECT datname FROM pg_database WHERE NOT datistemplate'
            )
        ]

    def list_schemas(self):
        """List all the schemas in the current database."""
        return self.inspector.get_schema_names()

    def set_database(self, name):
        raise NotImplementedError(
            'Cannot set database with RedShift client. To use a different'
            ' database, use client.database({!r})'.format(name)
        )

    @property
    def client(self):
        return self

    def table(self, name, database=None, schema=None):
        """Create a table expression that references a particular a table
        called `name` in a RedShift database called `database`.

        Parameters
        ----------
        name : str
            The name of the table to retrieve.
        database : str, optional
            The database in which the table referred to by `name` resides. If
            ``None`` then the ``current_database`` is used.
        schema : str, optional
            The schema in which the table resides.  If ``None`` then the
            `public` schema is assumed.

        Returns
        -------
        table : TableExpr
            A table expression.
        """
        if database is not None and database != self.current_database:
            return self.database(name=database).table(name=name, schema=schema)
        else:
            alch_table = self._get_sqla_table(name, schema=schema)
            node = self.table_class(alch_table, self, self._schemas.get(name))
            return self.table_expr_class(node)

    def list_tables(self, like=None, database=None, schema=None):
        if database is not None and database != self.current_database:
            return self.database(name=database).list_tables(
                like=like, schema=schema
            )
        else:
            parent = super(RedShiftClient, self)
            return parent.list_tables(like=like, schema=schema)

    def udf(
        self, pyfunc, in_types, out_type, schema=None, replace=False, name=None
    ):
        """Decorator that defines a PL/Python UDF in-database based on the
        wrapped function and turns it into an ibis function expression.

        Parameters
        ----------
        pyfunc : function
        in_types : List[ibis.expr.datatypes.DataType]
        out_type : ibis.expr.datatypes.DataType
        schema : str
            optionally specify the schema in which to define the UDF
        replace : bool
            replace UDF in database if already exists
        name: str
            name for the UDF to be defined in database

        Returns
        -------
        Callable

        Function that takes in ColumnExpr arguments and returns an instance
        inheriting from RedShiftUDFNode
        """

        return udf(
            client=self,
            python_func=pyfunc,
            in_types=in_types,
            out_type=out_type,
            schema=schema,
            replace=replace,
            name=name,
        )

def _get_schema_using_query(self, query: str) -> sch.Schema:
    raw_name = util.guid()
    name = self.con.dialect.identifier_preparer.quote_identifier(raw_name)
    type_info_sql = f"""\
    SELECT
    "column", "type" 
    FROM PG_TABLE_DEF
    WHERE tablename = {raw_name!r}
    """
    with self.con.connect() as con:
        con.execute(f"CREATE VIEW {name} AS {query}")
        try:
            type_info = con.execute(type_info_sql).fetchall()
        finally:
            con.execute(f"DROP VIEW {name}")
    tuples = [(col, self._get_type(typestr)) for col, typestr in type_info]
    a = sch.Schema.from_tuples(tuples)
    return a

_BRACKETS = "[]"
_STRING_REGEX = (
    """('[^\n'\\\\]*(?:\\\\.[^\n'\\\\]*)*'|"[^\n"\\\\"]*(?:\\\\.[^\n"\\\\]*)*")"""
)


def spaceless(parser):
    return SPACES.then(parser).skip(SPACES)


def spaceless_string(*strings: str):
    return spaceless(
        parsy.alt(*(parsy.string(s, transform=str.lower) for s in strings))
    )


SPACES = parsy.regex(r"\s*", re.MULTILINE)
RAW_NUMBER = parsy.decimal_digit.at_least(1).concat()
SINGLE_DIGIT = parsy.decimal_digit
PRECISION = SCALE = NUMBER = RAW_NUMBER.map(int)

LPAREN = spaceless_string("(")
RPAREN = spaceless_string(")")

LBRACKET = spaceless_string("[")
RBRACKET = spaceless_string("]")

LANGLE = spaceless_string("<")
RANGLE = spaceless_string(">")

COMMA = spaceless_string(",")
COLON = spaceless_string(":")
SEMICOLON = spaceless_string(";")

RAW_STRING = parsy.regex(_STRING_REGEX).map(ast.literal_eval)
FIELD = parsy.regex("[a-zA-Z_][a-zA-Z_0-9]*")


def _parse_numeric(
    text: str, ddp: tuple[int | None, int | None] = (None, None)
) -> dt.DataType:
    decimal = spaceless_string("decimal", "numeric").then(
        parsy.seq(LPAREN.then(PRECISION.skip(COMMA)), SCALE.skip(RPAREN))
        .optional(ddp)
        .combine(dt.Decimal)
    )

    brackets = spaceless(LBRACKET).then(spaceless(RBRACKET))

    pg_array = parsy.seq(decimal, brackets.at_least(1).map(len)).combine(
        lambda value_type, n: toolz.nth(n, toolz.iterate(dt.Array, value_type))
    )

    ty = pg_array | decimal
    return ty.parse(text)


def _get_type(self, typestr: str) -> dt.DataType:
    is_array = typestr.endswith(_BRACKETS)
    typestr_wob = typestr.replace(_BRACKETS, "")
    if "(" in typestr_wob:
        typestr_wo_length = (
            typestr_wob[: typestr_wob.index("(")]
            + typestr_wob[typestr_wob.index(")") + 1 :]
        )
    else:
        typestr_wo_length = typestr_wob
    typ = _type_mapping.get(typestr_wo_length)
    if typ is not None:
        return dt.Array(typ) if is_array else typ
    return _parse_numeric(typestr)

_type_mapping = {
        "boolean": dt.boolean,
        "varbyte": dt.binary,
        "character(1)": dt.string,
        "bigint": dt.int64,
        "smallint": dt.int16,
        "integer": dt.int32,
        "text": dt.string,
        "real": dt.float32,
        "double precision": dt.float64,
        "character": dt.string,
        "character varying": dt.string,
        "date": dt.date,
        "time without time zone": dt.time,
        "timestamp without time zone": dt.timestamp,
        "timestamp with time zone": dt.Timestamp("UTC"),
        "time with time zone": "time",
        "numeric": dt.float64,
        "geometry": dt.geometry,
        "geography": dt.geography,
    }


RedShiftClient._get_schema_using_query = _get_schema_using_query
RedShiftClient._get_type = _get_type