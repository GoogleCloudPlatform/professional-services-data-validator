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

from typing import Literal

import sqlalchemy as sa
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.mssql.compiler import MsSqlCompiler
from ibis.backends.mssql.datatypes import _type_from_result_set_info

import third_party.ibis.ibis_mssql.datatypes
import json


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
        odbc_driver: str = "ODBC Driver 17 for SQL Server",
        query: str = None,
    ) -> None:
        if url is None:
            if driver != "pyodbc":
                raise NotImplementedError(
                    "pyodbc is currently the only supported driver"
                )

            if query:
                query = json.loads(query)
            else:
                query = {"driver": odbc_driver}

            alchemy_url = sa.engine.url.URL.create(
                f"mssql+{driver}",
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                query=query,
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
