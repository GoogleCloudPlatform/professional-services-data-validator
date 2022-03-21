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


import contextlib
import third_party.ibis.ibis_snowflake.alchemy as alch
from third_party.ibis.ibis_snowflake.compiler import SnowflakeDialect
from snowflake.sqlalchemy import URL
import sqlalchemy as sa
from sqlalchemy import create_engine
from ibis.client import Database
from . import compiler


class SnowflakeTable(alch.AlchemyTable):
    pass


class SnowflakeSchema(alch.AlchemyDatabaseSchema):
    pass


class SnowflakeDatabase(alch.AlchemyDatabase):
    schema_class = SnowflakeSchema


class SnowflakeClient(alch.AlchemyClient):
    """ An ibis Snowflake client implementation """
    dialect = SnowflakeDialect
    database_class = SnowflakeDatabase
    table_class = SnowflakeTable

    def __init__(self,
                 user=None, password=None, account=None, database=None, schema=None, url=None):
        """Construct a Snowflake Client.
        Parameters
        ----------

        user : str
            A Snowflake user name to connect with
        password : str
            Password for supplied username
        account :
            The Snowflake acc name <your_account_name>.<region_id>.<cloud> e.g (e.g. 'xy12345.east-us-2.azure').
        """
        self.snowflake_config = {
            "user": user,
            "password": password,
            "account": account,
            "database": database,
            "schema": schema
        }

        if url is None:
            sa_url = URL(account=account,
                         user=user,
                         password=password,
                         database=database,
                         schema=schema
            )
            #sa_url = 'snowflake://{user}:{password}@{account}/{database_name}'.format(user=user, password=password, account=account, database_name=database)
        else:
            sa_url = sa.engine.url.make_url(url)

        engine = create_engine(sa_url)
        super().__init__(engine)
        self.database_name = engine.url.database

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
        if name == self.current_database or (
            name is None and name != self.current_database
        ):
            return self.database_class(self.current_database, self)
        else:
            url = self.con.url
            client_class = type(self)
            new_client = client_class(
                user=url.user,
                password=url.password,
                account= url.account,
                database=name,
            )
            return self.database_class(name, new_client)

    def schema(self, name):
        return self.database().schema(name)

    @property
    def current_database(self):
        """The name of the current database this client is connected to."""
        return self.database_name

    def list_database(self):
        return [
            row.database_name
            for row in self.executeSQL('''
                select database_name
                from information_schema.databases
                order by database_name;
            ''')
        ]

    def list_schemas(self):
        return self.inspector.get_schema_names()

    def set_database(self, name):
        raise NotImplementedError(
            'Cannot set database with Snowflake client. To use a different'
            ' database, use client.database({!r})'.format(name)
        )
        #self.executeSQL(''' USE DATABASE {name}'''.format(name=name))


    @property
    def client(self):
        return self

    def table(self, name, database=None, schema=None):
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
            return self.con.table_names()

    # https://docs.snowflake.com/en/user-guide/sqlalchemy.html#opening-and-closing-a-connection
    def executeSQL(self, sql):
        connection = self.con.connect()
        try:
            response = connection.execute(sql)
        finally:
            connection.close()
        return response

    def get_schema(self, name, schema=None):
        return self.table(name, schema=schema).schema()