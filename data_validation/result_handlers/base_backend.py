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

import logging
from typing import TYPE_CHECKING

import ibis

from data_validation.result_handlers import text as text_handler


if TYPE_CHECKING:
    from pandas import DataFrame


RESULTS_TABLE_SCHEMA = ibis.schema(
    {
        "validation_name": "!string",
        "validation_type": "!string",
        "aggregation_type": "string",
        "source_table_name": "string",
        "source_column_name": "string",
        "source_agg_value": "string",
        "target_table_name": "string",
        "target_column_name": "string",
        "target_agg_value": "string",
        "group_by_columns": "string",
        "primary_keys": "string",
        "num_random_rows": "int64",
        "difference": "float64",
        "pct_difference": "float64",
        "pct_threshold": "float64",
        "validation_status": "!string",
        "run_id": "!string",
        "labels": "array<str>",
        "start_time": "!timestamp('UTC')",
        "end_time": "!timestamp('UTC')",
    }
)


class BaseBackendResultHandler:
    """Write results of data validation to a backend."""

    _table_id: str = None
    _status_list: list = None
    _text_format: str = None

    def _filter_by_status_list(self, result_df: "DataFrame") -> "DataFrame":
        if self._status_list is not None:
            result_df = text_handler.filter_validation_status(
                self._status_list, result_df
            )
        return result_df

    def _call_text_handler(self, result_df: "DataFrame"):
        # Handler can also output results to stdout after saving to backend.
        logger = logging.getLogger()
        if logger.isEnabledFor(logging.DEBUG):
            # Checking log level to avoid evaluating a large Dataframe that will never be logged.
            logging.debug(
                text_handler.get_formatted(result_df, format=self._text_format)
            )
