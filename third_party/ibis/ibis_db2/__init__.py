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
import sqlalchemy as sa
import re

import ibis.expr.datatypes as dt
from typing import Iterable, Tuple
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from third_party.ibis.ibis_db2.compiler import Db2Compiler
from third_party.ibis.ibis_db2.datatypes import _get_type


class Backend(BaseAlchemyBackend):
    name = "db2"
    compiler = Db2Compiler

    def do_connect(
        self,
        host: str = "localhost",
        user: str = None,
        password: str = None,
        port: int = 50000,
        database: str = None,
        url: str = None,
        driver: str = "ibm_db_sa",
    ) -> None:
        if url is None:
            if driver != "ibm_db_sa":
                raise NotImplementedError(
                    "ibm_db_sa is currently the only supported driver"
                )
            sa_url = sa.engine.url.URL.create(
                "ibm_db_sa",
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
            )
        else:
            sa_url = sa.engine.url.make_url(url)

        engine = sa.create_engine(
            sa_url,
            poolclass=sa.pool.StaticPool,
            # Pessimistic disconnect handling
            pool_pre_ping=True,
        )
        self.database_name = database
        self.url = sa_url

        @sa.event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cur:
                cur.execute("SET TIMEZONE = UTC")

        super().do_connect(engine)

    def find_db(self):
        return self.url

    def _metadata(self, query) -> Iterable[Tuple[str, dt.DataType]]:
        if (
            re.search(r"^\s*SELECT\s", query, flags=re.MULTILINE | re.IGNORECASE)
            is not None
        ):
            query = f"({query})"

        with self.begin() as con:
            result = con.exec_driver_sql(f"SELECT * FROM {query} t0 LIMIT 1")
            cursor = result.cursor
            yield from (
                (column[0].lower(), _get_type(column[1]))
                for column in cursor.description
            )

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        list_pk_col_sql = """
            SELECT key.colname
            FROM syscat.tables tab
            INNER JOIN syscat.tabconst const ON const.tabschema = tab.tabschema AND const.tabname = tab.tabname and const.type = 'P'
            INNER JOIN syscat.keycoluse key ON const.tabschema = key.tabschema AND const.tabname = key.tabname AND const.constname = key.constname
            WHERE tab.type = 'T'
            AND tab.tabschema = ?
            AND tab.tabname = ?
            ORDER BY key.colseq"""
        with self.begin() as con:
            result = con.exec_driver_sql(
                list_pk_col_sql, parameters=(database.upper(), table.upper())
            )
            return [_[0] for _ in result.cursor.fetchall()]

    def raw_column_metadata(
        self, database: str = None, table: str = None, query: str = None
    ) -> Iterable[Tuple]:
        """Define this method to allow DVT to test if backend specific transformations may be needed for comparison.
        Partner method to _get_schema_using_query that retains raw data type information instead of converting
        to Ibis types.  This works in the same way as _get_schema_using_query by running a query over the DVT
        source, either schema.table or a custom query, and fetching the first row. From the cursor we can detect
        data types of the row's columns.

        In db2, if we knew the table and column name the underlying SQL data type can be obtained by
        running the following query - select COLNAME, TYPENAME from SYSCAT.COLUMNS
                                           where TABSCHEMA = '{schema_name.upper()}' and TABNAME='{table_name.upper()}'
                                           and COLNAME='{column_name.upper()}'
        This works for tables, but does not allow us to find the underlying SQL data type for queries. Cursor.description
        provides the underlying "compatible" SQL data types, so a fixed char and varchar column will have the same underlying
        SQL data types.

        Returns:
            list: A list of tuples containing the standard 7 DB API fields:
                  https://peps.python.org/pep-0249/#description
        """
        return ()

    def is_char_type_padded(self, char_type: Tuple) -> bool:
        """Define this method if the backend supports character/string types that are padded and returns
        padded values, which DVT may want to trim"""
        return char_type[0] == "CHARACTER"
