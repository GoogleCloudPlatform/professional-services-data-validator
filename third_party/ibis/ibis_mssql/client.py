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
import datetime
import getpass

import sqlalchemy as sa
from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc

import ibis.backends.base_sqlalchemy.alchemy as alch
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
from third_party.ibis.ibis_mssql.compiler import MSSQLDialect

import pyodbc  # NOQA fail early if the driver is missing

@dt.dtype.register(MSDialect_pyodbc, sa.dialects.mssql.UNIQUEIDENTIFIER)
def sa_string(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(MSDialect_pyodbc, sa.dialects.mssql.BIT)
def sa_boolean(_, satype, nullable=True):
    return dt.Boolean(nullable=nullable)


class MSSQLTable(alch.AlchemyTable):
    pass


class MSSQLSchema(alch.AlchemyDatabaseSchema):
    pass


class MSSQLDatabase(alch.AlchemyDatabase):
    schema_class = MSSQLSchema


class MSSQLClient(alch.AlchemyClient):
    """The Ibis MSSQL client class.

    Attributes
    ----------
    con : sqlalchemy.engine.Engine
    """

    dialect = MSSQLDialect
    database_class = MSSQLDatabase
    table_class = MSSQLTable

    def __init__(
        self,
        host='localhost',
        user=None,
        password=None,
        port=1433,
        database='master',
        url=None,
        driver='pyodbc',
        odbc_driver='ODBC Driver 17 for SQL Server',
    ):
        if url is None:
            if driver != 'pyodbc':
                raise NotImplementedError(
                    'pyodbc is currently the only supported driver'
                )
            user = user or getpass.getuser()
            url = sa.engine.url.URL(
                'mssql+pyodbc',
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                query={'driver': odbc_driver},
            )
        else:
            url = sa.engine.url.make_url(url)
        super().__init__(sa.create_engine(url))
        self.database_name = url.database

    @contextlib.contextmanager
    def begin(self):
        """Start transaction with client to database."""
        with super().begin() as bind:
            # set timezone utc
            yield bind
            # set timezone previous timezone

    def database(self, name=None):
        """Connect to a database called `name`.

        Parameters
        ----------
        name : string, optional
            The name of the database to connect to. If ``None``, return
            the database named ``self.current_database``.

        Returns
        -------
        db : MSSQLDatabase
            An :class:`ibis.sql.mssql.client.MSSQLDatabase` instance.

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
        name : string

        Returns
        -------
        schema : MSSQLSchema
            An :class:`ibis.sql.mssql.client.MSSQLSchema` instance.

        """
        return self.database().schema(name)

    @property
    def current_database(self):
        """Database client is currently connected to."""
        return self.database_name

    def list_databases(self):
        """List all databases for client to connect to."""
        return [
            row.name
            for row in self.con.execute(
                'SELECT name FROM master.dbo.sysdatabases'
            )
        ]

    def list_schemas(self):
        """List all the schemas in the current database."""
        return self.inspector.get_schema_names()

    def set_database(self, name):
        """Set current database that client is connected to."""
        raise NotImplementedError(
            'Cannot set database with MSSQL client. To use a different'
            ' database, use client.database({!r})'.format(name)
        )

    @property
    def client(self):
        return self

    def table(self, name, database=None, schema=None):
        """Create an expression that references a particular table.

        Parameters
        ----------
        name : string
            The name of the table to retrieve.
        database : string, optional
            The database in which the table referred to by `name` resides. If
            ``None`` then the ``current_database`` is used.
        schema : string, optional
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
        """
        List tables avilable in current database

        Parameters
        ----------
        like : string, default None
          e.g. 'foo*' to match all tables starting with 'foo'.
        database : string, default None
          Specific database to list available tables
        schema : string, default None
          Specific schema to list available tables

        Returns
        -------
        list
            A list with all tables available for the current database.
        """
        if database is not None and database != self.current_database:
            return self.database(name=database).list_tables(
                like=like, schema=schema
            )
        else:
            parent = super(MSSQLClient, self)
            return parent.list_tables(like=like, schema=schema)

    def sql(self, query):
        """
        Convert a MSSQL query to an Ibis table expression

        Parameters
        ----------
        query: string
           SQL query to execute on connection

        Returns
        -------
        table : TableExpr
        """
        limited_query = 'SELECT TOP 0 * FROM ({}) t0'.format(query)
        schema = self._get_schema_using_query(limited_query)
        return ops.SQLQueryResult(query, schema, self).to_expr()

    def _get_schema_using_query(self, limited_query):
        type_map = {
            int: 'int64',
            bool: 'boolean',
            float: 'float64',
            str: 'string',
            datetime.datetime: 'timestamp',
        }

        with self._execute(limited_query, results=True) as cur:
            names = [row[0] for row in cur.proxy._cursor_description()]
            ibis_types = [
                type_map[row[1]] for row in cur.proxy._cursor_description()
            ]
        return sch.Schema(names, ibis_types)

    def get_schema(self, name, schema=None):
        return self.table(name, schema=schema).schema()