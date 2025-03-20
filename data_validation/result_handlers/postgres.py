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

from data_validation import clients, consts
from data_validation.result_handlers import text as text_handler


RH_WRITE_MESSAGE = "Results written to PostgreSQL"


class PostgresResultHandler(object):
    """Write results of data validation to PostgreSQL.

    Arguments:
        postgres_client
        table_id (str):
            Fully-qualified table id (``schema.table``) of destination table for results.
    """

    def __init__(
        self,
        postgres_client,
        status_list: list = None,
        table_id: str = "pso_data_validator.results",
        text_format: str = consts.FORMAT_TYPE_TABLE,
    ):
        self._postgres_client = postgres_client
        self._table_id = table_id
        self._status_list = status_list
        self._text_format = text_format

    @staticmethod
    def get_handler_for_connection(
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
        client = None
        return PostgresResultHandler(
            client,
            status_list=status_list,
            table_id=table_id,
            text_format=text_format,
        )

    def execute(self, result_df):
        pass
