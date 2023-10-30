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

        engine = sa.create_engine(sa_url, poolclass=sa.pool.StaticPool)
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
