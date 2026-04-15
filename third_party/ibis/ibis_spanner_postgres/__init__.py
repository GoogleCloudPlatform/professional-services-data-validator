# Copyright 2025 Google LLC
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

import contextlib
from typing import Iterable, Optional

from google.cloud import spanner

from ibis.backends.postgres import Backend as PostgresBackend
import sqlalchemy as sa
from sqlalchemy.dialects import registry as sa_registry

from third_party.ibis.ibis_addon.api import dvt_handle_failed_column_type_inference
from third_party.ibis.ibis_spanner_postgres.compiler import SpannerPostgresCompiler

sa_registry.register(
    "spanner_postgres.psycopg2",
    "third_party.ibis.ibis_spanner_postgres.dialect",
    "SpannerPostgresDialect_psycopg2",
)


class Backend(PostgresBackend):
    name = "spanner_postgres"
    compiler = SpannerPostgresCompiler
    # _compiler_class = SpannerPostgresCompiler

    def do_connect(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "u",
        password: str = "p",
        database: str = None,
        url: Optional[str] = None,
    ) -> None:
        alchemy_url = self._build_alchemy_url(
            url=url,
            host=host,
            port=port,
            user=None,
            password=None,
            database=database,
            driver=f"spanner_postgres+psycopg2",
        )
        self.database_name = alchemy_url.database

        connect_args = {"options": "-c timezone=UTC"}
        engine = sa.create_engine(
            alchemy_url,
            connect_args=connect_args,
            poolclass=sa.pool.StaticPool,
            pool_pre_ping=True,
        )

        @sa.event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cur:
                cur.execute("SET TIMEZONE = UTC")

        super(PostgresBackend, self).do_connect(engine)

    @contextlib.contextmanager
    def _safe_raw_sql(self, *args, **kwargs):
        # Use self.con.connect() below to avoid creating a transaction.
        # The SQLAlchemy default self.begin() creates an unnecessary transaction
        with self.con.connect() as con:
            yield con.execute(*args, **kwargs)

    def _handle_failed_column_type_inference(
        self, table: sa.Table, nulltype_cols: Iterable[str]
    ) -> sa.Table:
        return dvt_handle_failed_column_type_inference(self, table, nulltype_cols)

    def raw_column_metadata(
        self, database: str = None, table: str = None, query: str = None
    ):
        assert (database and table) or query, "We should never receive all args=None"
        if database and table:
            source = f'"{database}"."{table}"'
        elif query:
            source = f"({query})"

        oid_to_type = {
            16: "boolean",
            17: "bytea",
            20: "bigint",
            21: "smallint",
            23: "integer",
            25: "text",
            700: "real",
            701: "double precision",
            1043: "character varying",
            1082: "date",
            1114: "timestamp without time zone",
            1184: "timestamp with time zone",
            1700: "numeric",
            3802: "jsonb",
        }

        with self.con.connect() as con:
            cur = con.exec_driver_sql(f"SELECT * FROM {source} t0 LIMIT 0")
            for column in cur.cursor.description:
                data_type = oid_to_type.get(
                    column.type_code, f"unknown_{column.type_code}"
                )
                yield (
                    column.name,
                    data_type,
                    column.display_size,
                    column.internal_size,
                    column.precision,
                    column.scale,
                    column.null_ok,
                )

    @property
    def instance_id(self):
        return self.data_instance

    @property
    def version(self):
        return spanner.__version__

    @property
    def dataset_id(self):
        return self.dataset

    @property
    def current_database(self) -> str:
        return self.database_name

    def create_table(self):
        raise NotImplementedError("Method unimplemented for DVT")

    def create_view(self):
        raise NotImplementedError("Method unimplemented for DVT")

    def drop_table(self):
        raise NotImplementedError("Method unimplemented for DVT")

    def drop_view(self):
        raise NotImplementedError("Method unimplemented for DVT")

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        schema = database or "public"
        query = """
            SELECT ic.column_name
            FROM information_schema.indexes i
            JOIN information_schema.index_columns ic
              ON i.table_catalog = ic.table_catalog
             AND i.table_schema = ic.table_schema
             AND i.table_name = ic.table_name
             AND i.index_name = ic.index_name
            WHERE i.index_type = 'PRIMARY_KEY'
              AND i.table_name = :table_name
              AND i.table_schema = :table_schema
            ORDER BY ic.ordinal_position
        """
        with self.con.connect() as con:
            result = con.execute(
                sa.text(query).bindparams(table_name=table, table_schema=schema)
            )
            return [row[0] for row in result.fetchall()]

    def list_tables(self, like=None, database=None):
        raise NotImplementedError("Method unimplemented for DVT")

    def database(self, name=None):
        if name is None and self.dataset is None:
            raise ValueError(
                "Unable to determine Cloud Spanner dataset. Call "
                "client.database('my_dataset') or set_database('my_dataset') "
                "to assign your client a dataset."
            )
        return self.database_class(name or self.dataset, self)

    def list_databases(self, like=None):
        raise NotImplementedError("Method unimplemented for DVT")
