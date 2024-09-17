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

import logging

from google.cloud import bigquery

from data_validation import client_info
from data_validation.result_handlers import text as text_handler


class BigQueryResultHandler(object):
    """Write results of data validation to BigQuery.

    Arguments:
        bigquery_client (google.cloud.bigquery.client.Client):
            BigQuery client for uploading results.
        table_id (str):
            Fully-qualified table ID (``project-id.dataset.table``) of
            destination table for results.
    """

    def __init__(
        self,
        bigquery_client,
        status_list: list = None,
        table_id: str = "pso_data_validator.results",
        text_format: str = "table",
    ):
        self._bigquery_client = bigquery_client
        self._table_id = table_id
        self._status_list = status_list
        self._text_format = text_format

    @staticmethod
    def get_handler_for_project(
        project_id,
        status_list=None,
        table_id: str = "pso_data_validator.results",
        credentials=None,
        text_format: str = "table",
    ):
        """Return BigQueryResultHandler instance for given project.

        Args:
            project_id (str): Project ID used for validation results.
            table_id (str): Table ID used for validation results.
            credentials (google.auth.credentials.Credentials):
                Explicit credentials to use in case default credentials
                aren't working properly.
            status_list (list): provided status to filter the results with
            text_format (str, optional):
                This allows the user to influence the text results written via logger.debug.
                See: https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/871
        """
        info = client_info.get_http_client_info()
        client = bigquery.Client(
            project=project_id, client_info=info, credentials=credentials
        )
        return BigQueryResultHandler(
            client, status_list=status_list, table_id=table_id, text_format=text_format
        )

    def execute(self, result_df):
        if self._status_list is not None:
            result_df = text_handler.filter_validation_status(
                self._status_list, result_df
            )

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
                    f"The latest release of DVT has updated the column name 'status' to 'validation_status': {chunk_errors}"
                )
            elif (
                chunk_errors[0][0]["errors"][0]["message"]
                == "no such field: primary_keys."
            ):
                raise RuntimeError(
                    f"Please update your BigQuery results table schema using the script : samples/bq_utils/add_columns_schema.sh.\n"
                    f"The latest release of DVT has added two fields 'primary_keys' and 'num_random_rows': {chunk_errors}"
                )
            raise RuntimeError(f"Could not write rows: {chunk_errors}")

        if result_df.empty:
            logging.info("No results to write to BigQuery")
        else:
            logging.info(
                f'Results written to BigQuery, run id: {result_df.iloc[0]["run_id"]}'
            )

        # Handler also logs results after saving to BigQuery.
        logger = logging.getLogger()
        if logger.isEnabledFor(logging.DEBUG):
            # Checking log level to avoid evaluating a large Dataframe that will never be logged.
            logging.debug(
                text_handler.get_formatted(result_df, format=self._text_format)
            )

        return result_df
