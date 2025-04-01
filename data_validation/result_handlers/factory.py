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

"""Build a result handler object."""

from typing import TYPE_CHECKING

import google.oauth2.service_account

from data_validation import consts, exceptions
from data_validation.result_handlers.bigquery import BigQueryResultHandler
from data_validation.result_handlers.postgres import PostgresResultHandler
from data_validation.result_handlers.text import TextResultHandler

if TYPE_CHECKING:
    from data_validation.result_handlers.base_backend import BaseBackendResultHandler


def build_result_handler(
    result_handler_config: dict,
    validation_type: str,
    filter_status: list,
    text_format: str = None,
) -> "BaseBackendResultHandler":
    """Return a result handler object based on supplied args."""
    text_format = text_format or consts.FORMAT_TYPE_TABLE
    if not result_handler_config:
        if validation_type == consts.SCHEMA_VALIDATION:
            cols_filter_list = consts.SCHEMA_VALIDATION_COLUMN_FILTER_LIST
        else:
            cols_filter_list = consts.COLUMN_FILTER_LIST
        # Handler that display results to stdout.
        return TextResultHandler(
            text_format,
            filter_status,
            cols_filter_list,
        )

    result_type = result_handler_config[consts.RH_TYPE]
    if result_type == consts.SOURCE_TYPE_BIGQUERY:
        project_id = result_handler_config[consts.PROJECT_ID]
        table_id = result_handler_config[consts.TABLE_ID]
        key_path = result_handler_config.get(consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH)
        if key_path:
            credentials = (
                google.oauth2.service_account.Credentials.from_service_account_file(
                    key_path
                )
            )
        else:
            credentials = None
        api_endpoint = result_handler_config.get(consts.API_ENDPOINT)
        return BigQueryResultHandler.get_handler_for_project(
            project_id,
            filter_status,
            table_id=table_id,
            credentials=credentials,
            api_endpoint=api_endpoint,
            text_format=text_format,
        )
    elif result_type == consts.SOURCE_TYPE_POSTGRES:
        table_id = result_handler_config[consts.TABLE_ID]
        return PostgresResultHandler.get_handler_for_connection(
            result_handler_config[consts.RH_CONN],
            filter_status,
            table_id=table_id,
            text_format=text_format,
        )
    else:
        raise exceptions.ResultHandlerException(
            f"Unknown ResultHandler Class: {result_type}"
        )
