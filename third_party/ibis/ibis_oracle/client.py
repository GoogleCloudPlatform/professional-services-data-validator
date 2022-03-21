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
from third_party.ibis.ibis_oracle.compiler import OracleDialect

import ibis.backends.base_sqlalchemy.alchemy as alch

import cx_Oracle  # NOQA fail early if the driver is missing


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
    os.environ['TNS_ADMIN'] = 'Wallet_Location_Path'

    def __init__(
        self,
        host: str = 'localhost',
        user: str = getpass.getuser(),
        password: Optional[str] = None,
        port: int = 1521,
        database: str = None,
        protocol: str ='TCP',
        url: Optional[str] = None,
        driver: str = 'cx_Oracle',
    ):
        if url is None:
            if driver != 'cx_Oracle':
                raise NotImplementedError(
                    'cx_Oracle is currently the only supported driver'
                )
            dsn='''(description= (address=(protocol={})(host={})(port={}))
            (connect_data=(service_name={})))'''.format(protocol, host, port, database)
            sa_url = sa.engine.url.URL(
                'oracle+cx_oracle', user, password, dsn,
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
                user=url.username, password=url.password, database=name,
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
        return [
            row.name for row in self.con.execute('select name from v$database')
        ]

    def list_schemas(self):
        """List all the schemas in the current database."""
        return self.inspector.get_schema_names()

    def set_database(self, name):
        raise NotImplementedError(
            'Cannot set database with Oracle client. To use a different'
            ' database, use client.database({!r})'.format(name)
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
            return self.database(name=database).list_tables(
                like=like, schema=schema
            )
        else:
            parent = super(OracleClient, self)
            return parent.list_tables(like=like, schema=schema)

    def get_schema(self, name, schema=None):
        return self.table(name, schema=schema).schema()
