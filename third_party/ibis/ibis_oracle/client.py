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

import contextlib
import getpass
import os
from typing import Optional
import sqlalchemy as sa

from sqlalchemy.dialects.oracle.cx_oracle import OracleDialect_cx_oracle
from third_party.ibis.ibis_oracle.compiler import OracleDialect

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import ibis.expr.operations as ops
import ibis.backends.base_sqlalchemy.alchemy as alch
import cx_Oracle  # NOQA fail early if the driver is missing


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.CLOB)
def sa_oracle_CLOB(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.NCLOB)
def sa_oracle_NCLOB(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.LONG)
def sa_oracle_LONG(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.NUMBER)
def sa_oracle_NUMBER(_, satype, nullable=True):
    if not satype.precision and not satype.scale:
        return dt.Decimal(38, 0, nullable=nullable)
    return dt.Decimal(satype.precision, satype.scale, nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.BFILE)
def sa_oracle_BFILE(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.RAW)
def sa_oracle_RAW(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.DATE)
def sa_oracle_DATE(_, satype, nullable=True):
    return dt.Date(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.VARCHAR)
def sa_oracle_VARCHAR(_, satype, nullable=True):
    return dt.String(nullable=nullable)

@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.VARCHAR2)
def sa_oracle_VARCHAR2(_, satype, nullable=True):
    return dt.String(nullable=nullable)

@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.TIMESTAMP)
def sa_oracle_TIMESTAMP(_, satype, nullable=True):
    return dt.Timestamp(nullable=nullable)

class OracleTable(alch.AlchemyTable):
    pass


class OracleSchema(alch.AlchemyDatabaseSchema):
    pass


class OracleDatabase(alch.AlchemyDatabase):
    schema_class = OracleSchema


class OracleClient(alch.AlchemyClient):
    """The Ibis Oracle client class
    Attributes
    ----------
    con : sqlalchemy.engine.Engine
    """

    dialect = OracleDialect
    database_class = OracleDatabase
    table_class = OracleTable
    os.environ["TNS_ADMIN"] = "Wallet_Location_Path"

    def __init__(
        self,
        host: str = "localhost",
        user: str = getpass.getuser(),
        password: Optional[str] = None,
        port: int = 1521,
        database: str = None,
        protocol: str = "TCP",
        url: Optional[str] = None,
        driver: str = "cx_Oracle",
    ):
        if url is None:
            if driver != "cx_Oracle":
                raise NotImplementedError(
                    "cx_Oracle is currently the only supported driver"
                )
            dsn = """(description= (address=(protocol={})(host={})(port={}))
            (connect_data=(service_name={})))""".format(
                protocol, host, port, database
            )
            sa_url = sa.engine.url.URL(
                "oracle+cx_oracle",
                user,
                password,
                dsn,
            )
        else:
            sa_url = sa.engine.url.make_url(url)
        super().__init__(sa.create_engine(sa_url, max_identifier_length=128))
        self.database_name = database
        self.uurl = sa_url

    def find_db(self):
        return self.uurl

    @contextlib.contextmanager
    def begin(self):
        with super().begin() as bind:
            previous_timezone = bind.execute("SHOW TIMEZONE").scalar()
            bind.execute("SET TIMEZONE = UTC")
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
        db : OracleDatabase
        class:`third_party.ibis.ibis_oracle.client.OracleDatabase`
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
                user=url.username,
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
        schema : OracleSchema
        An :class:`third_party.ibis.ibis_oracle.client.OracleSchema` instance.
        """
        return self.database().schema(name)

    @property
    def current_database(self):
        """The name of the current database this client is connected to."""
        return self.database_name

    def list_databases(self):
        return [row.name for row in self.con.execute("select name from v$database")]

    def list_schemas(self):
        """List all the schemas in the current database."""
        return self.inspector.get_schema_names()

    def set_database(self, name):
        raise NotImplementedError(
            "Cannot set database with Oracle client. To use a different"
            " database, use client.database({!r})".format(name)
        )

    @property
    def client(self):
        return self

    def table(self, name, database=None, schema=None):
        """Create a table expression that references a particular a table
        called `name` in a Oracle database called `database`.
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
            return self.database(name=database).list_tables(like=like, schema=schema)
        else:
            parent = super(OracleClient, self)
            return parent.list_tables(like=like, schema=schema)

    def get_schema(self, name, schema=None):
        return self.table(name, schema=schema).schema()
    
    def sql(self, query):
        """
        Convert a Oracle query to an Ibis table expression

        Parameters
        ----------
        query: string
           SQL query to execute on connection

        Returns
        -------
        table : TableExpr
        """
        limited_query = "SELECT * FROM ({}) t0 WHERE ROWNUM <= 1".format(query)
        schema = self._get_schema_using_query(limited_query)
        return ops.SQLQueryResult(query, schema, self).to_expr()

    def _get_schema_using_query(self, limited_query):
        type_map = {
            "DB_TYPE_NVARCHAR": "string",
            "DB_TYPE_VARCHAR": "string",
            "DB_TYPE_TIMESTAMP": "timestamp",
            "DB_TYPE_DATE": "timestamp"
        }

        with self._execute(limited_query, results=True) as cur:
            names = [row[0].lower() for row in cur.proxy._cursor_description()]
            ibis_types = []
            for row in cur.proxy._cursor_description():
                if row[1].name == "DB_TYPE_NUMBER":
                    precision, scale = row[4], row[5]
                    if scale > 0:
                        ibis_datatype = "float64"
                    else:
                        ibis_datatype = "int64"
                else:
                    ibis_datatype = type_map[row[1].name]
                ibis_types.append(ibis_datatype)
                
        return sch.Schema(names, ibis_types)
