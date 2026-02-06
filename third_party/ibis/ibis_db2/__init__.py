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
import re
from typing import Iterable, Optional, Tuple, Dict, Any

import sqlalchemy as sa

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from third_party.ibis.ibis_db2.compiler import Db2Compiler
from third_party.ibis.ibis_db2.datatypes import _get_type


class Backend(BaseAlchemyBackend):
    name = "db2"
    compiler = Db2Compiler

    def do_connect(
        self,
        host: str = "localhost",
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 50000,
        database: Optional[str] = None,
        url: Optional[str] = None,
        driver: str = "ibm_db_sa",
        connect_args: Dict[str, Any] = None,
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
            connect_args=connect_args or {},
        )
        self.database_name = database
        self.url = sa_url

        # We cannot find a SET TIMEZONE command for LUW therefore commented this section out.
        # @sa.event.listens_for(engine, "connect")
        # def connect(dbapi_connection, connection_record):
        #    with dbapi_connection.cursor() as cur:
        #        cur.execute("SET TIMEZONE = 'UTC'")

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
    ) -> list:
        """Define this method to allow DVT to test if backend specific transformations may be needed for comparison.
        Partner method to _metadata that retains raw data type information instead of converting to Ibis types.
        This works in the same way as _metadata by running a query over the DVT source, either schema.table or a
        custom query, and fetching the first row. From the cursor we can detect data types of the row's columns.

        NOTE: This only works for table look-ups. For custom queries the raw data types are not available to us
              due to the IBM Db2 driver hiding the real data types.

        Returns:
            list: A list of tuples containing the standard 7 DB API fields:
                  https://peps.python.org/pep-0249/#description
        """
        assert (database and table) or query, "We should never receive all args=None"
        if database and table:
            # For table-based validation, query the system catalog to get the true data type.
            get_column_metadata_sql = """
                SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS
                FROM SYSCAT.COLUMNS
                WHERE TABSCHEMA = ? AND TABNAME = ?
                ORDER BY COLNO
            """
            with self.begin() as con:
                result = con.exec_driver_sql(
                    get_column_metadata_sql,
                    parameters=(database.upper(), table.upper()),
                )
                # Yield a 7-tuple mimicking cursor.description, with the true typename.
                for (
                    colname,
                    typename,
                    col_length,
                    col_scale,
                    nullable,
                ) in result.cursor.fetchall():
                    yield (
                        colname,
                        typename,
                        col_length,
                        col_length,
                        col_length,
                        col_scale,
                        nullable,
                    )
        elif query:
            # For custom queries, the system catalog cannot be used. Fall back to
            # cursor.description, which may not distinguish padded char types.
            source = f"({query})"
            with self.begin() as con:
                result = con.exec_driver_sql(f"SELECT * FROM {source} t0 LIMIT 0")
                cursor = result.cursor
                yield from (column for column in cursor.description)

    def is_char_type_padded(self, char_type: Tuple) -> bool:
        """Define this method if the backend supports character/string types that are padded and returns
        padded values, which DVT may want to trim"""
        type_code = char_type[0]
        if isinstance(type_code, str):
            return type_code.upper() == "CHARACTER"
        else:
            # From cursor.description for custom queries, this is a DBAPITypeObject.
            # It's not possible to distinguish padded char types in this case,
            # so we default to False to be safe and avoid trimming incorrectly.
            return False
