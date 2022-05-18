# Copyright 2020 Google LLC
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

"""Output validation report to BigQuery tables"""

from google.cloud import bigquery

from data_validation import client_info
from data_validation.result_handlers.text import TextResultHandler


class BigQueryResultHandler(object):
    """Write results of data validation to BigQuery.

    Arguments:
        bigquery_client (google.cloud.bigquery.client.Client):
            BigQuery client for uploading results.
        table_id (str):
            Fully-qualified table ID (``project-id.dataset.table``) of
            destination table for results.
    """

    def __init__(self, bigquery_client, table_id="pso_data_validator.results"):
        self._bigquery_client = bigquery_client
        self._table_id = table_id

    @staticmethod
    def get_handler_for_project(
        project_id, table_id="pso_data_validator.results", credentials=None
    ):
        """Return BigQueryResultHandler instance for given project.

        Args:
            project_id (str): Project ID used for validation results.
            table_id (str): Table ID used for validation results.
            credentials (google.auth.credentials.Credentials):
                Explicit credentials to use in case default credentials
                aren't working properly.
        """
        info = client_info.get_http_client_info()
        client = bigquery.Client(
            project=project_id, client_info=info, credentials=credentials
        )
        return BigQueryResultHandler(client, table_id=table_id)

    def execute(self, config, result_df):
        text_handler = TextResultHandler("table")
        text_handler.print_formatted_(result_df)

        table = self._bigquery_client.get_table(self._table_id)
        chunk_errors = self._bigquery_client.insert_rows_from_dataframe(
            table, result_df
        )
        if any(chunk_errors):
            if (
                chunk_errors[0][0]["errors"][0]["message"]
                == "no such field: validation_status."
            ):
                raise RuntimeError(
                    f"Please update your BigQuery results table schema using the script : samples/bq_utils/rename_column_schema.sh.\n"
                    f"The lastest release of DVT has updated the column name 'status' to 'validation_status': {chunk_errors}"
                )
            elif (
                chunk_errors[0][0]["errors"][0]["message"]
                == "no such field: primary_keys."
            ):
                raise RuntimeError(
                    f"Please update your BigQuery results table schema using the script : samples/bq_utils/add_columns_schema.sh.\n"
                    f"The lastest release of DVT has added two fields 'primary_keys' and 'num_random_rows': {chunk_errors}"
                )
            raise RuntimeError(f"could not write rows: {chunk_errors}")

        return result_df
