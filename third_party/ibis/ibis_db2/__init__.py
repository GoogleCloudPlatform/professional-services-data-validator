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
from typing import Iterable, Optional, Tuple, Dict, Any, TYPE_CHECKING

import sqlalchemy as sa

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from third_party.ibis.ibis_db2.compiler import Db2Compiler
from third_party.ibis.ibis_db2.datatypes import _get_type

if TYPE_CHECKING:
    import ibis.expr.types as ir


DB2_HIDDEN_COLUMNS = ["db2_generated_docid_for_xml", "db2_generated_rowid_for_lob"]
FOR_BIT_DATA_MAP = {
    "CHARACTER": "CHARACTER_FOR_BIT_DATA",
    "CHAR": "CHAR_FOR_BIT_DATA",
    "VARCHAR": "VARCHAR_FOR_BIT_DATA",
}


class Backend(BaseAlchemyBackend):
    name = "db2"
    compiler = Db2Compiler

    char_datatype = "CHARACTER"

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
    ) -> Iterable[Tuple]:
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
            # SYSIBM.SYSCOLUMNS works on both LUW and z/OS. SYSCAT.COLUMNS is only valid on LUW.
            # FOR BIT DATA is not revealed in the TYPENAME column, we need to check CODEPAGE and
            # inject our own custom TYPENAME.
            get_column_metadata_sql = """
                SELECT NAME, TYPENAME, LENGTH, SCALE, NULLS, CODEPAGE
                FROM SYSIBM.SYSCOLUMNS
                WHERE TBCREATOR = ? AND TBNAME = ?
                ORDER BY COLNO
            """
            with self.begin() as con:
                result = con.exec_driver_sql(
                    get_column_metadata_sql,
                    parameters=(database.upper(), table.upper()),
                )
                rows = result.cursor.fetchall()

            for row in rows:
                colname, typename, col_length, col_scale, nullable, codepage = row
                if codepage == 0 and typename.upper() in FOR_BIT_DATA_MAP:
                    # Db2 does not expose FOR BIT DATA types so we customize the type name here.
                    typename = FOR_BIT_DATA_MAP[typename.upper()]

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
            return type_code.upper() == self.char_datatype
        else:
            # From cursor.description for custom queries, this is a DBAPITypeObject.
            # It's not possible to distinguish padded char types in this case,
            # so we default to False to be safe and avoid trimming incorrectly.
            return False

    def table(
        self,
        name: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> "ir.Table":
        """Intercept Ibis table() call and inject Db2 customizations before returning the table object."""
        return_table = super().table(name, database, schema)

        # Query raw metadata to find columns that are actually binary (FOR BIT DATA)
        # but reflected as strings by SQLAlchemy.
        raw_types = self.raw_column_metadata(schema or database, name) or []
        for_bit_data_cols = set()
        for col_name, type_name, *_ in raw_types:
            if type_name in FOR_BIT_DATA_MAP.values():
                for_bit_data_cols.add(col_name.lower())

        if for_bit_data_cols:
            # Create a new table object with FOR BIT DATA columns as binary.
            old_schema = return_table.schema()
            new_fields = {
                name: (dt.binary if name.lower() in for_bit_data_cols else dtype)
                for name, dtype in old_schema.items()
            }
            new_schema = sch.Schema(new_fields)
            op = return_table.op()
            new_op = op.copy(schema=new_schema)
            return_table = new_op.to_expr()

        return return_table
        # Failed attempt at dealing with hidden columns below for reference.
        # columns_to_drop = [_ for _ in return_table.columns if _ in DB2_HIDDEN_COLUMNS]
        # if columns_to_drop:
        #     return_table = return_table.drop(*columns_to_drop)
