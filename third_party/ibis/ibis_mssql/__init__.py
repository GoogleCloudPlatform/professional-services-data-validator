# Copyright 2023 Google Inc.
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
from __future__ import annotations

from typing import Literal, Tuple, Iterable

import sqlalchemy as sa
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.mssql.compiler import MsSqlCompiler
from ibis.backends.mssql.datatypes import _type_from_result_set_info

import json


DEFAULT_DRIVER_NAME = "ODBC Driver 17 for SQL Server"


# The MSSQL backend uses the Ibis MSSQL compiler, but overrides
# the Backend class to use pyodbc instead of pymssql
class Backend(BaseAlchemyBackend):
    name = "mssql"
    compiler = MsSqlCompiler
    supports_create_or_replace = False

    _sqlglot_dialect = "tsql"

    def do_connect(
        self,
        host: str = "localhost",
        user: str = None,
        password: str = None,
        port: int = 1433,
        database: str = None,
        url: str = None,
        driver: Literal["pyodbc"] = "pyodbc",
        odbc_driver: str = DEFAULT_DRIVER_NAME,
        query: str = None,
    ) -> None:
        if url is None:
            if driver != "pyodbc":
                raise NotImplementedError(
                    "pyodbc is currently the only supported driver"
                )

            if query:
                query_dict = json.loads(query)
                if "driver" not in query:
                    query_dict["driver"] = odbc_driver or DEFAULT_DRIVER_NAME
            else:
                query_dict = {"driver": odbc_driver}

            alchemy_url = sa.engine.url.URL.create(
                f"mssql+{driver}",
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                query=query_dict,
            )
        else:
            alchemy_url = sa.engine.url.make_url(url)

        self.database_name = alchemy_url.database
        engine = sa.create_engine(
            alchemy_url,
            poolclass=sa.pool.StaticPool,
            # Pessimistic disconnect handling
            pool_pre_ping=True,
        )

        @sa.event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cur:
                cur.execute("SET DATEFIRST 1")

        self.client = engine
        return super().do_connect(engine)

    def _metadata(self, query):
        if query in self.list_tables():
            query = f"SELECT * FROM [{query}]"

        query = sa.text("EXEC sp_describe_first_result_set @tsql = :query").bindparams(
            query=query
        )
        with self.begin() as bind:
            for column in bind.execute(query).mappings():
                yield column["name"], _type_from_result_set_info(column)

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        list_pk_col_sql = """
            SELECT COL_NAME(ic.object_id, ic.column_id) AS column_name
            FROM sys.tables t
            INNER JOIN sys.indexes i ON (t.object_id = i.object_id)
            INNER JOIN sys.index_columns ic ON (i.object_id = ic.object_id AND i.index_id  = ic.index_id)
            INNER JOIN sys.schemas s ON (t.schema_id = s.schema_id)
            WHERE  s.name = ?
            AND    t.name = ?
            AND    i.is_primary_key = 1
            ORDER BY ic.column_id"""
        with self.begin() as con:
            result = con.exec_driver_sql(list_pk_col_sql, parameters=(database, table))
            return [_[0] for _ in result.cursor.fetchall()]

    def raw_column_metadata(
        self, database: str = None, table: str = None, query: str = None
    ) -> Iterable[Tuple]:
        """Define this method to allow DVT to test if backend specific transformations may be needed for comparison.
        Partner method to _metadata that retains raw data type information instead of converting to Ibis types.
        This works in the same way as _metadata by running a query over the DVT source, either schema.table or a
        custom query, and fetching the first row. From it we can detect data types of the row's columns.

        For SQL Server, we use the stored procedure 'sp_describe_first_result_set' to get column metadata.
        https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-describe-first-result-set-transact-sql

        Returns:
            Iterable[Tuple]: An iterable of tuples, each containing the standard 7 DB API fields:
                  https://peps.python.org/pep-0249/#description
        """

        assert (
            database and table
        ) or query, "Must provide either database/table or query"

        if database and table:
            # Properly quote and format the table name with schema
            quoted_table = f"[{database}].[{table}]"
            sql = f"SELECT TOP 1 * FROM {quoted_table}"
        elif query:
            sql = query

        # Use the stored procedure to get metadata
        metadata_query = sa.text(
            "EXEC sp_describe_first_result_set @tsql = :query"
        ).bindparams(query=sql)

        with self.begin() as con:
            result = con.execute(metadata_query)
            for column in result.mappings():
                # Extract relevant metadata from the result set and construct the metadata tuple (DB API format)
                # Note: Metadata may vary based on the SQL Server version and the specific query used.
                yield (
                    column["name"],
                    column["system_type_name"],  # type_code
                    None,  # display_size
                    None,  # internal_size
                    column["precision"],
                    column["scale"],
                    column["is_nullable"],
                )

    def is_char_type_padded(self, char_type: Tuple) -> bool:
        """Define this method if the backend supports character/string types that are padded and returns
        padded values, which DVT may want to trim"""
        return char_type[0] in ["char", "nchar"]

    def list_databases(self, schema=None):
        schema_like = f"%{schema or ''}%"
        list_database_sql = """
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name LIKE ?
        """
        with self.begin() as con:
            result = con.exec_driver_sql(list_database_sql, parameters=(schema_like,))
            return [_[0] for _ in result.cursor.fetchall()]

    def list_tables(self, table=None, schema=None, type_like: str = "%") -> list:
        schema_like = f"%{schema or ''}%"
        table_like = f"%{table or ''}%"
        list_table_sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema LIKE ?
            AND table_name LIKE ?
            AND table_type LIKE ?
        """
        with self.begin() as con:
            result = con.exec_driver_sql(
                list_table_sql, parameters=(schema_like, table_like, type_like)
            )
            return [_[0] for _ in result.cursor.fetchall()]

    def dvt_list_tables(self, like=None, database=None) -> list:
        """Duplicate of list_tables() but only returning tables in the output."""
        return self.list_tables(table=like, schema=database, type_like="BASE TABLE")
