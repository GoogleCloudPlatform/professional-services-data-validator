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

from typing import Literal

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import sqlalchemy as sa
from ibis import util
from ibis.backends.postgres import Backend as PostgresBackend
from ibis.backends.postgres.datatypes import _BRACKETS, _parse_numeric, _type_mapping


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
        alchemy_url, connect_args=connect_args, poolclass=sa.pool.StaticPool
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
    raw_name = util.guid()
    name = self._quote(raw_name)
    type_info_sql = """\
    SELECT
    attname,
    format_type(atttypid, atttypmod) AS type
    FROM pg_attribute
    WHERE attrelid = CAST(:raw_name AS regclass)
    AND attnum > 0
    AND NOT attisdropped
    ORDER BY attnum"""
    with self.begin() as con:
        con.exec_driver_sql(f"CREATE TEMPORARY VIEW {name} AS {query}")
        type_info = con.execute(sa.text(type_info_sql).bindparams(raw_name=raw_name))
        yield from ((col, _get_type(typestr)) for col, typestr in type_info)
        con.exec_driver_sql(f"DROP VIEW IF EXISTS {name}")


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
    if typ is not None and typestr_wo_length != "numeric":
        return dt.Array(typ) if is_array else typ
    return _parse_numeric(typestr)


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


PostgresBackend._metadata = _metadata
PostgresBackend.list_databases = list_schemas
PostgresBackend.do_connect = do_connect
