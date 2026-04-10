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

from typing import Optional

from google.api_core import client_options
from google.cloud import spanner

from ibis.backends.postgres import Backend as PostgresBackend
import sqlalchemy as sa
from sqlalchemy.dialects import registry as sa_registry

from third_party.ibis.ibis_cloud_spanner import parse_instance_and_dataset

from third_party.ibis.ibis_spanner_postgres.compiler import SpannerPostgresCompiler


sa_registry.register(
    "spanner_postgres.psycopg2",
    "third_party.ibis.ibis_spanner_postgres.dialect",
    "SpannerPostgresDialect_psycopg2",
)


class Backend(PostgresBackend):
    name = "spanner_postgres"
    compiler = SpannerPostgresCompiler

    def do_connect(
        self,
        instance_id: str,
        database_id: Optional[str] = None,
        project_id: Optional[str] = None,
        credentials=None,
        api_endpoint: Optional[str] = None,
        host: str = "localhost",
        port: int = 5432,
        url: Optional[str] = None,
    ) -> None:
        alchemy_url = self._build_alchemy_url(
            url=url,
            host=host,
            port=port,
            user=None,
            password=None,
            database=database_id,
            driver=f"{self.name}+psycopg2",
        )
        self.database_name = alchemy_url.database

        engine = sa.create_engine(alchemy_url)
        super(PostgresBackend, self).do_connect(engine)

        return

        options = None
        if api_endpoint:
            options = client_options.ClientOptions(api_endpoint=api_endpoint)

        self.client = spanner.Client(
            project=project_id, credentials=credentials, client_options=options
        )
        self.instance = self.client.instance(instance_id)
        self.database_name = self.instance.database(database_id)
        (
            self.data_instance,
            self.dataset,
        ) = parse_instance_and_dataset(instance_id, database_id)

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
