# Copyright 2025 Google Inc.
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

import json
import re
from typing import Iterable, Literal, Tuple, Optional

import sqlalchemy as sa
from sqlalchemy_sybase.base import (
    SybaseDialect,
    SybaseIdentifierPreparer,
    RESERVED_WORDS as SYBASE_RESERVED_WORDS,
)
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
import ibis.expr.datatypes as dt

from third_party.ibis.ibis_addon.api import dvt_handle_failed_column_type_inference
from third_party.ibis.ibis_sybase.compiler import SybaseCompiler
from third_party.ibis.ibis_sybase.datatypes import type_from_result_set_info


EXTRA_RESERVED_WORDS = set(
    [
        "count",
    ]
)


class DVTSybaseIdentifierPreparer(SybaseIdentifierPreparer):
    reserved_words = {
        x.lower() for x in SYBASE_RESERVED_WORDS.union(EXTRA_RESERVED_WORDS)
    }


SybaseDialect.preparer = DVTSybaseIdentifierPreparer


# The Sybase backend uses the Ibis MSSQL compiler.
class Backend(BaseAlchemyBackend):
    name = "sybase"
    compiler = SybaseCompiler
    supports_create_or_replace = False

    # _sqlglot_dialect = "tsql"

    def do_connect(
        self,
        host: str = "localhost",
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 5000,
        database: Optional[str] = None,
        url: Optional[str] = None,
        driver: Literal["pyodbc"] = "pyodbc",
        odbc_driver: Optional[str] = None,
        query: Optional[str] = None,
    ) -> None:
        if url is None:
            if driver != "pyodbc":
                raise NotImplementedError(
                    "pyodbc is currently the only supported driver"
                )

            if query:
                query_dict = json.loads(query)
                if "driver" not in query:
                    query_dict["driver"] = odbc_driver or "FreeTDS"
            else:
                query_dict = {"driver": odbc_driver}

            alchemy_url = sa.engine.url.URL.create(
                f"sybase+{driver}",
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

    def _handle_failed_column_type_inference(
        self, table: sa.Table, nulltype_cols: Iterable[str]
    ) -> sa.Table:
        breakpoint()
        return dvt_handle_failed_column_type_inference(self, table, nulltype_cols)

    def _metadata(self, query) -> Iterable[Tuple[str, dt.DataType]]:
        if (
            re.search(r"^\s*SELECT\s", query, flags=re.MULTILINE | re.IGNORECASE)
            is not None
        ):
            with self.begin() as con:
                result = con.execute(sa.text(f"SELECT TOP 1 * FROM ({query}) AS t0"))
                col_types = result.cursor.description
                yield from (
                    (c[0], type_from_result_set_info(c[0], c[1], c[4], c[5], c[6]))
                    for c in col_types
                )
        else:
            database, table = query.split(".", maxsplit=1)
            result = self.raw_sql(f"EXEC sp_columns [{table}], [{database}]")
            return [
                (_[3], type_from_result_set_info(_[3], _[5], _[6], _[8], _[10]))
                for _ in result.fetchall()
            ]

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        result = self.raw_sql(f"EXEC sp_pkeys [{table}], [{database}]")
        return [_[3] for _ in result.fetchall()]

    def raw_column_metadata_not_implemented(
        self, database: str = None, table: str = None, query: str = None
    ) -> list[Tuple]:
        """Define this method to allow DVT to test if backend specific transformations may be needed for comparison.
        Partner method to _metadata that retains raw data type information instead of converting
        to Ibis types.  This works in the same way as _metadata by running a query over the DVT
        source, either schema.table or a custom query, and fetching the metadata using sp_describe_first_result_set.

        THIS METHOD IS NOT IMPLEMENTED YET.


        Returns:
            list: A list of tuples containing the standard 7 DB API fields:
                  https://peps.python.org/pep-0249/#description
        """
        return []

    def is_char_type_padded(self, char_type: Tuple) -> bool:
        """Define this method if the backend supports character/string types that are padded and returns
        padded values, which DVT may want to trim"""
        return char_type[0] in ["char", "nchar"]

    def list_databases(self, schema=None):
        schema_like = f'"%{schema or ""}%"' if schema else "NULL"
        # I couldn't find SQL to report table/view owners so get the object list and then de-dupe the owner.
        list_table_sql = f"EXEC sp_tables NULL, {schema_like}, NULL, NULL"
        rows = self.raw_sql(list_table_sql).fetchall()
        owners = {row[1] for row in rows}
        return list(owners)

    def list_tables(self, table=None, schema=None, type_like: str = "%") -> list:
        """Return a list of table names from sp_table stored procedure using input filters."""
        schema_like = f'"%{schema or ""}%"' if schema else "NULL"
        table_like = f'"%{table or ""}%"' if table else "NULL"
        type_filter = "\"'TABLE','VIEW'\"" if type_like == "%" else f"\"'{type_like}'\""
        list_table_sql = (
            f"EXEC sp_tables {table_like}, {schema_like}, NULL, {type_filter}"
        )
        rows = self.raw_sql(list_table_sql).fetchall()
        return [_[2] for _ in rows]

    def dvt_list_tables(self, like=None, database=None) -> list:
        """Duplicate of list_tables() but only returning tables in the output."""
        return self.list_tables(table=like, schema=database, type_like="TABLE")
