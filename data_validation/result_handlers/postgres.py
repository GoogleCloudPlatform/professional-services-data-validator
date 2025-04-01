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

"""Output validation report to PostgreSQL table"""

import logging
from typing import TYPE_CHECKING

import sqlalchemy

from data_validation import clients, consts
from data_validation.result_handlers.base_backend import (
    BaseBackendResultHandler,
    RESULTS_TABLE_SCHEMA,
    RH_WRITE_MESSAGE,
    RH_NO_WRITE_MESSAGE,
)


if TYPE_CHECKING:
    from pandas import DataFrame
    from ibis.backends.base import BaseBackend


class PostgresResultHandler(BaseBackendResultHandler):
    """Write results of data validation to PostgreSQL."""

    def __init__(
        self,
        client: "BaseBackend",
        status_list: list = None,
        table_id: str = "pso_data_validator.results",
        text_format: str = consts.FORMAT_TYPE_TABLE,
    ):
        self._client = client
        self._table_id = table_id
        self._status_list = status_list
        self._text_format = text_format

    @staticmethod
    def get_handler_for_connection(
        connection_config: dict,
        status_list=None,
        table_id: str = "pso_data_validator.results",
        text_format: str = consts.FORMAT_TYPE_TABLE,
    ):
        """Return PostgresResultHandler instance for given connection config.

        Args:
            table_id (str): Table ID used for validation results.
            status_list (list): provided status to filter the results with
            text_format (str, optional):
                This allows the user to influence the text results written via logger.debug.
                See: https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/871
        """
        client = clients.get_data_client(connection_config)
        return PostgresResultHandler(
            client,
            status_list=status_list,
            table_id=table_id,
            text_format=text_format,
        )

    def _set_current_schema(self, schema_name: str):
        """Set the current schema in a PostgreSQL backend.

        We need to do this due to Ibis limitations:
            NotImplementedError: Creating tables from a different database is not yet implemented
            NotImplementedError: Inserting data to a table from a different database is not yet implemented
        """
        with self._client.begin() as con:
            _ = con.exec_driver_sql(f"SET schema '{schema_name}'")

    def execute(self, result_df: "DataFrame"):
        """Store the validation results Dataframe to an Ibis Backend."""
        result_df = self._filter_by_status_list(result_df)

        if "." in self._table_id:
            schema_name, table_name = self._table_id.split(".")
        else:
            schema_name, table_name = None, self._table_id

        if schema_name:
            self._set_current_schema(schema_name)
        try:
            _ = clients.get_ibis_table(self._client, schema_name, table_name)
            # Do nothing, the table exists.
        except sqlalchemy.exc.NoSuchTableError:
            self._client.create_table(table_name, schema=RESULTS_TABLE_SCHEMA)

        if not result_df.empty:
            self._client.insert(table_name, result_df)

        if result_df.empty:
            logging.info(RH_NO_WRITE_MESSAGE)
        else:
            logging.info(
                f"{RH_WRITE_MESSAGE} to {self._table_id}, run id: {result_df.iloc[0][consts.CONFIG_RUN_ID]}"
            )

        self._call_text_handler(result_df)

        return result_df
