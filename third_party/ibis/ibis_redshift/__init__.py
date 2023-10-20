# Copyright 2023 Google LLC
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
import sqlalchemy as sa

import ibis.expr.datatypes as dt
from typing import Iterable, Literal, Tuple
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from third_party.ibis.ibis_redshift.compiler import RedshiftCompiler
from ibis import util
from ibis.backends.postgres.datatypes import _BRACKETS, _parse_numeric, _type_mapping


class Backend(BaseAlchemyBackend):
    name = "redshift"
    compiler = RedshiftCompiler

    def do_connect(
        self,
        host: str = "localhost",
        user: str = None,
        password: str = None,
        port: int = 5439,
        database: str = "public",
        schema: str = None,
        url: str = None,
        driver: Literal["psycopg2"] = "psycopg2",
    ) -> None:

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

        super().do_connect(engine)

    def list_databases(self, like=None):
        with self.begin() as con:
            # http://dba.stackexchange.com/a/1304/58517
            databases = [
                row.datname
                for row in con.exec_driver_sql(
                    "SELECT datname FROM pg_database WHERE NOT datistemplate"
                ).mappings()
            ]
        return self._filter_with_like(databases, like)

    def _metadata(self, query: str) -> Iterable[Tuple[str, dt.DataType]]:
        raw_name = util.guid()
        name = self._quote(raw_name)
        type_info_sql = """\
    SELECT
    "column", "type" 
    FROM PG_TABLE_DEF
    WHERE tablename = :raw_name
    """
        if self.inspector.has_table(query):
            query = f"TABLE {query}"
        with self.begin() as con:
            con.exec_driver_sql(f"CREATE VIEW {name} AS {query}")
            type_info = con.execute(
                sa.text(type_info_sql).bindparams(raw_name=raw_name)
            )
            yield from ((col, _get_type(typestr)) for col, typestr in type_info)
            con.exec_driver_sql(f"DROP VIEW IF EXISTS {name}")

    def _get_temp_view_definition(
        self, name: str, definition: sa.sql.compiler.Compiled
    ) -> str:
        yield f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {definition}"


def _get_type(typestr: str) -> dt.DataType:
    is_array = typestr.endswith(_BRACKETS)
    typestr_wob = typestr.replace(_BRACKETS, "")
    if "(" in typestr_wob:
        typestr_wo_length = (
            typestr_wob[: typestr_wob.index("(")]
            + typestr_wob[typestr_wob.index(")") + 1 :]
        )
    else:
        typestr_wo_length = typestr_wob
    typ = _type_mapping.get(typestr_wo_length)
    if typ is not None:
        return dt.Array(typ) if is_array else typ
    return _parse_numeric(typestr)
