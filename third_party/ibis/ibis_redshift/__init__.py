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
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine.reflection import ReflectionDefaults
from third_party.ibis.ibis_redshift.compiler import RedshiftCompiler
from ibis.backends.postgres.datatypes import PostgresType
from third_party.ibis.ibis_addon.api import cache_generator_results
from ibis import util


class RedshiftPsycopg2Dialect(PGDialect_psycopg2):
    name = "redshift"
    supports_native_enum = False
    supports_statement_cache = True

    def _set_backslash_escapes(self, connection):
        self._backslash_escapes = False

    def _load_domains(self, connection, schema=None, **kw):
        return []

    def _empty_reflection(self, schema, filter_names, default):
        if filter_names is None:
            return []
        return [((schema, table_name), default()) for table_name in filter_names]

    def get_multi_pk_constraint(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return self._empty_reflection(
            schema, filter_names, ReflectionDefaults.pk_constraint
        )

    def get_multi_foreign_keys(
        self,
        connection,
        schema,
        filter_names,
        scope,
        kind,
        postgresql_ignore_search_path=False,
        **kw,
    ):
        return self._empty_reflection(
            schema, filter_names, ReflectionDefaults.foreign_keys
        )

    def get_multi_indexes(self, connection, schema, filter_names, scope, kind, **kw):
        return self._empty_reflection(schema, filter_names, ReflectionDefaults.indexes)

    def get_multi_unique_constraints(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return self._empty_reflection(
            schema, filter_names, ReflectionDefaults.unique_constraints
        )

    def get_multi_check_constraints(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return self._empty_reflection(
            schema, filter_names, ReflectionDefaults.check_constraints
        )


registry.register(
    "redshift.psycopg2",
    "third_party.ibis.ibis_redshift",
    "RedshiftPsycopg2Dialect",
)


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
            driver=f"redshift+{driver}",
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
            execution_options={"isolation_level": "AUTOCOMMIT"},
        )

        @sa.event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cur:
                cur.execute("SET TIMEZONE = UTC")

        super().do_connect(engine)

    def list_databases(self, like=None):
        with self.con.connect() as con:
            # http://dba.stackexchange.com/a/1304/58517
            databases = [
                row.datname
                for row in con.exec_driver_sql(
                    "SELECT datname FROM pg_database WHERE NOT datistemplate"
                ).mappings()
            ]
        return self._filter_with_like(databases, like)

    @cache_generator_results
    def _metadata(self, query: str) -> Iterable[Tuple[str, dt.DataType]]:
        raw_name = util.guid().lower()
        name = self._quote(raw_name)
        type_info_sql = """
        SELECT
            "column_name",
            "data_type"
        FROM SVV_ALL_COLUMNS
        WHERE table_name = :raw_name
        ORDER BY ordinal_position
        """
        if self.inspector.has_table(query):
            query = f"TABLE {query}"
        with self.con.connect() as con:
            try:
                con.exec_driver_sql(
                    f"CREATE VIEW {name} AS {query} WITH NO SCHEMA BINDING"
                )
                type_info = con.execute(
                    sa.text(type_info_sql).bindparams(raw_name=raw_name)
                )
                yield from ((col, _get_type(typestr)) for col, typestr in type_info)
            finally:
                con.exec_driver_sql(f"DROP VIEW IF EXISTS {name}")

    def _get_temp_view_definition(
        self, name: str, definition: sa.sql.compiler.Compiled
    ) -> str:
        yield f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {definition}"

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        # TODO: Related to issue-1253, it's not clear if this is possible, we should revisit if it becomes a requirement.
        return None

    def dvt_tuple_in_supported(self) -> bool:
        """Return True if backend client supports native SQL tuple/struct IN expressions."""
        return False


def _get_type(typestr: str) -> dt.DataType:
    return PostgresType.from_string(typestr)
