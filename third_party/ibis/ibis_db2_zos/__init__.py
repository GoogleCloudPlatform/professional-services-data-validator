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

from typing import Iterable, Optional, Dict, Any

import sqlalchemy as sa

from third_party.ibis.ibis_addon.api import dvt_handle_failed_column_type_inference
from third_party.ibis.ibis_db2 import Backend as Db2LUWBackend
from third_party.ibis.ibis_db2_zos.compiler import Db2zOSCompiler


class Backend(Db2LUWBackend):
    name = "db2_zos"
    compiler = Db2zOSCompiler

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

        @sa.event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cur:
                cur.execute("SET SESSION TIME ZONE = '+0:00'")

        super(Db2LUWBackend, self).do_connect(engine)

    def _handle_failed_column_type_inference(
        self, table: sa.Table, nulltype_cols: Iterable[str]
    ) -> sa.Table:
        return dvt_handle_failed_column_type_inference(self, table, nulltype_cols)
