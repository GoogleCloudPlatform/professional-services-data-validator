# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Iterable, Literal, Tuple

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import sqlalchemy as sa
from ibis.backends.postgres import Backend as PostgresBackend
from ibis.backends.postgres.datatypes import _BRACKETS, _parse_numeric, _type_mapping
import re


def do_connect(
    self,
    host: str = "localhost",
    user: str = None,
    password: str = None,
    port: int = 5432,
    database: str = None,
    schema: str = None,
    url: str = None,
    driver: Literal["psycopg2"] = "psycopg2",
) -> None:
    # Override do_connect() method to remove DDL queries to CREATE/DROP FUNCTION
    if driver != "psycopg2":
        raise NotImplementedError("psycopg2 is currently the only supported driver")

    alchemy_url = self._build_alchemy_url(
        url=url,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        driver=f"postgresql+{driver}",
    )
    self.database_name = alchemy_url.database
    connect_args = {}
    if schema is not None:
        connect_args["options"] = f"-csearch_path={schema}"

    engine = sa.create_engine(
        alchemy_url,
        connect_args=connect_args,
        poolclass=sa.pool.StaticPool,
        # Pessimistic disconnect handling
        pool_pre_ping=True,
    )

    @sa.event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        with dbapi_connection.cursor() as cur:
            cur.execute("SET TIMEZONE = UTC")

    # Equivalent of super().do_connect() below
    self.con = engine
    self._inspector = sa.inspect(self.con)
    self._schemas: dict[str, sch.Schema] = {}
    self._temp_views: set[str] = set()


def _metadata(self, query: str) -> sch.Schema:
    # This function is called when ibis has to figure out the datatypes of columns in a custom query validation OR
    # when ibis encounters a column with a datatype not supported in https://ibis-project.org/reference/datatypes
    # In the latter case, this just returns None for that column, resulting in a NotImplementedError elsewhere.
    query = (
        f"({query})"
        if re.search(r"^\s*SELECT\s", query, flags=re.MULTILINE | re.IGNORECASE)
        else query
    )
    with self.begin() as con:
        cur = con.exec_driver_sql(f"SELECT * FROM {query} t0 LIMIT 0")
        qry_cols = [
            f"('{column.name}'::text, {column.type_code},"
            + f"{column.table_oid if column.table_oid else 'NULL'}::int,"
            + f"{column.table_column if column.table_column else 'NULL'}::int, {idx})"
            for idx, column in enumerate(cur.cursor.description)
        ]
        type_info = con.exec_driver_sql(
            f"""SELECT name, CASE WHEN t0.attrelid is NULL
                                THEN format_type(t0.type_code, NULL)
                                ELSE format_type(t1.atttypid, t1.atttypmod) END AS type
                    FROM UNNEST(array[{','.join(qry_cols)}])
                    AS t0(name text, type_code int, attrelid int, attnum int, col_ord int)
                    LEFT JOIN pg_attribute t1 USING (attrelid, attnum) ORDER BY col_ord"""
        )
    yield from ((col, _get_type(typestr)) for col, typestr in type_info)


def _get_type(typestr: str) -> dt.DataType:
    is_array = typestr.endswith(_BRACKETS)
    # typ = _type_mapping.get(typestr.replace(_BRACKETS, ""))
    # handle bracket length
    typestr_wob = typestr.replace(_BRACKETS, "")
    if "(" in typestr_wob:
        typestr_wo_length = (
            typestr_wob[: typestr_wob.index("(")]
            + typestr_wob[typestr_wob.index(")") + 1 :]
        )
    else:
        typestr_wo_length = typestr_wob
    typ = _type_mapping.get(typestr_wo_length)
    # Added != "numeric" below for issue:
    #   https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1302
    # We do not want to map all numerics to decimal(None, None), we want "numeric" to be
    # passed to the _parse_numeric() function.
    # An alternative was to remove "numeric" from _type_mapping but that would be yet more monkey
    # patching, at least this function is already patched.
    if typ and typestr_wo_length == "numeric":
        return _parse_numeric(typestr)
    elif typ:
        return dt.Array(typ) if is_array else typ
    else:
        return None  # Type is not known - will result in a NotImplemented Error


def list_schemas(self, like=None):
    with self.begin() as con:
        # Databases on Postgres are not the same as schemas and for this method we need the schema list (SQL query reference: https://dba.stackexchange.com/a/127668)
        schemas = [
            row.nspname
            for row in con.exec_driver_sql(
                "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY 1"
            ).mappings()
        ]
    return self._filter_with_like(schemas, like)


def _list_primary_key_columns(self, database: str, table: str) -> list:
    """Return a list of primary key column names."""
    # From https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    list_pk_col_sql = """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                             AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = CAST(:raw_name AS regclass)
        AND    i.indisprimary
        """
    with self.begin() as con:
        result = con.execute(
            sa.text(list_pk_col_sql).bindparams(raw_name=f"{database}.{table}")
        )
        return [_[0] for _ in result.cursor.fetchall()]


def _raw_column_metadata(
    self, database: str = None, table: str = None, query: str = None
) -> Iterable[Tuple]:
    """Partner method to _metadata that retains raw data type information instead of converting to Ibis types.
    This works in the same way as _metadata by running a query over the DVT source, either schema.table or a
    custom query, and fetching the first row. From the cursor we can detect data types of the row's columns.

    Returns:
        list: A list of tuples containing the standard 7 DB API fields:
                https://peps.python.org/pep-0249/#description
    """

    assert (database and table) or query, "We should never receive all args=None"
    if database and table:
        source = f'"{database}"."{table}"'
    elif query:
        source = f"({query})"

    with self.begin() as con:
        cur = con.exec_driver_sql(f"SELECT * FROM {source} t0 LIMIT 0")
        qry_cols = [
            f"('{column.name}'::text,"
            + f"{column.type_code},"
            + f"{column.display_size if column.display_size else 'NULL'}::int,"
            + f"{column.internal_size if column.internal_size else 'NULL'}::int,"
            + f"{column.precision if column.precision else 'NULL'}::int,"
            + f"{column.scale if column.scale is not None else 'NULL'}::int,"
            + f"{column.null_ok if column.null_ok else 'NULL'}::int, {idx})"
            for idx, column in enumerate(cur.cursor.description)
        ]
        sql = f"""SELECT name, format_type(t0.type_code, NULL) AS data_type
                ,    display_size, internal_size, precision, scale, null_ok
                FROM UNNEST(array[{','.join(qry_cols)}])
                     AS t0(name text, type_code int, display_size int, internal_size int, precision int, scale int, null_ok int, col_order int)
                ORDER BY col_order"""
        yield from (
            (
                column.name,
                column.data_type,
                column.display_size,
                column.internal_size,
                column.precision,
                column.scale,
                column.null_ok,
            )
            for column in con.exec_driver_sql(sql)
        )


PostgresBackend._metadata = _metadata
PostgresBackend.list_databases = list_schemas
PostgresBackend.do_connect = do_connect
PostgresBackend.list_primary_key_columns = _list_primary_key_columns
PostgresBackend.raw_column_metadata = _raw_column_metadata
