# Copyright 2024 Google LLC
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

from typing import TYPE_CHECKING

import google.auth.credentials
import google.cloud.bigquery as bq
import pydata_google_auth
from pydata_google_auth import cache

from ibis.backends.bigquery import (
    Backend as BigQueryBackend,
    _create_client_info,
    CLIENT_ID,
    CLIENT_SECRET,
    EXTERNAL_DATA_SCOPES,
    SCOPES,
)
from ibis.backends.bigquery.client import parse_project_and_dataset

if TYPE_CHECKING:
    import google.cloud.bigquery_storage_v1


class Backend(BigQueryBackend):
    def __init__(self):
        super().__init__()
        self.storage_client = None

    def do_connect(
        self,
        project_id: str = None,
        dataset_id: str = "",
        credentials: google.auth.credentials.Credentials = None,
        application_name: str = None,
        auth_local_webserver: bool = True,
        auth_external_data: bool = False,
        auth_cache: str = "default",
        partition_column: str = "PARTITIONTIME",
        # Custom DVT arguments:
        bigquery_client: bq.Client = None,
        bqstorage_client: "google.cloud.bigquery_storage_v1.BigQueryReadClient" = None,
    ):
        """Copy of Ibis v5 BigQuery do_connect() customized for DVT, see original method for docs."""
        client_project_id = (
            bigquery_client.project if bigquery_client is not None else None
        )
        default_project_id = None

        if bigquery_client is None and credentials is None:
            scopes = SCOPES
            if auth_external_data:
                scopes = EXTERNAL_DATA_SCOPES

            if auth_cache == "default":
                credentials_cache = cache.ReadWriteCredentialsCache(
                    filename="ibis.json"
                )
            elif auth_cache == "reauth":
                credentials_cache = cache.WriteOnlyCredentialsCache(
                    filename="ibis.json"
                )
            elif auth_cache == "none":
                credentials_cache = cache.NOOP
            else:
                raise ValueError(
                    f"Got unexpected value for auth_cache = '{auth_cache}'. "
                    "Expected one of 'default', 'reauth', or 'none'."
                )

            credentials, default_project_id = pydata_google_auth.default(
                scopes,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                credentials_cache=credentials_cache,
                use_local_webserver=auth_local_webserver,
            )
            project_id = project_id or default_project_id

        self.billing_project = client_project_id or project_id
        self.data_project = project_id or self.billing_project
        self.dataset = None

        if bigquery_client is None:
            self.client = bq.Client(
                project=self.billing_project,
                credentials=credentials,
                client_info=_create_client_info(application_name),
            )
        else:
            self.client = bigquery_client
        self.partition_column = partition_column
        self.storage_client = bqstorage_client

    def _cursor_to_arrow(
        self,
        cursor,
        *,
        method=None,
        chunk_size: int = None,
    ):
        """Copy of Ibis v5 BigQuery _cursor_to_arrow() except can use custom DVT storage client"""
        if method is None:

            def method(result, storage_client=self.storage_client):
                return result.to_arrow(
                    progress_bar_type=None,
                    # Include DVT specific storage client.
                    bqstorage_client=storage_client,
                    create_bqstorage_client=bool(not self.storage_client),
                )

        query = cursor.query
        query_result = query.result(page_size=chunk_size)
        # workaround potentially not having the ability to create read sessions
        # in the dataset project
        orig_project = query_result._project
        query_result._project = self.billing_project
        try:
            arrow_obj = method(query_result)
        finally:
            query_result._project = orig_project
        return arrow_obj

    def _parse_project_and_dataset(self, dataset) -> tuple[str, str]:
        """Copied from Ibis code and modified to use self.data_project."""
        if not dataset and not self.dataset:
            raise ValueError("Unable to determine BigQuery dataset.")
        project, _, dataset = parse_project_and_dataset(
            self.data_project,
            dataset or f"{self.data_project}.{self.dataset}",
        )
        return project, dataset

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        # TODO: Related to issue-1253, it's not clear if this is possible, we should revisit if it becomes a requirement.
        return None

    def dvt_list_tables(self, like=None, database=None):
        return self.list_tables(like=like, database=database)
