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

from data_validation import consts, util
from data_validation.result_handlers import text as text_handler


if TYPE_CHECKING:
    from pandas import DataFrame


RH_WRITE_MESSAGE = "Results written"
RH_NO_WRITE_MESSAGE = "No results to write"

RESULTS_TABLE_SCHEMA = ibis.schema(
    {
        consts.VALIDATION_NAME: "!string",
        consts.VALIDATION_TYPE: "!string",
        consts.AGGREGATION_TYPE: "string",
        consts.SOURCE_TABLE_NAME: "string",
        consts.SOURCE_COLUMN_NAME: "string",
        consts.SOURCE_AGG_VALUE: "string",
        consts.TARGET_TABLE_NAME: "string",
        consts.TARGET_COLUMN_NAME: "string",
        consts.TARGET_AGG_VALUE: "string",
        consts.GROUP_BY_COLUMNS: "string",
        consts.CONFIG_PRIMARY_KEYS: "string",
        consts.NUM_RANDOM_ROWS: "int64",
        consts.VALIDATION_DIFFERENCE: "float64",
        consts.VALIDATION_PCT_DIFFERENCE: "float64",
        consts.VALIDATION_PCT_THRESHOLD: "float64",
        consts.VALIDATION_STATUS: "!string",
        consts.CONFIG_RUN_ID: "!string",
        consts.CONFIG_LABELS: "array<str>",
        consts.CONFIG_START_TIME: "!timestamp('UTC')",
        consts.CONFIG_END_TIME: "!timestamp('UTC')",
    }
)


class BaseBackendResultHandler:
    """Write results of data validation to a backend."""

    _table_id: str = None
    _status_list: list = None
    _text_format: str = None

    def _filter_by_status_list(self, result_df: "DataFrame") -> "DataFrame":
        if self._status_list is not None:
            result_df = util.timed_call(
                "Filter by validation status",
                text_handler.filter_validation_status,
                self._status_list,
                result_df,
            )
        return result_df

    def _call_text_handler(self, result_df: "DataFrame"):
        # Handler can also output results to stdout after saving to backend.
        logger = logging.getLogger()
        if logger.isEnabledFor(logging.DEBUG):

            def _fn():
                # Checking log level to avoid evaluating a large Dataframe that will never be logged.
                logging.debug(
                    text_handler.get_formatted(result_df, format=self._text_format)
                )

            util.timed_call("Call text handler", _fn)
