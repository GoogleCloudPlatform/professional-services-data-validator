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

""" A ResultHandler class is supplied to the DataValidation manager class.

The execute function of any result handler is used to process
the validation results.  It expects to receive the config
for the validation and Pandas DataFrame with the results
from the validation run.

Output validation report to text-based log
"""
from typing import TYPE_CHECKING

from data_validation import consts


if TYPE_CHECKING:
    from pandas import DataFrame


def filter_validation_status(status_list, result_df: "DataFrame"):
    return result_df[result_df.validation_status.isin(status_list)]


def get_formatted(
    result_df: "DataFrame", format: str = "table", cols_filter_list: list = None
) -> str:
    """Expose formatting logic so it can be used in BigQuery handler."""
    cols_filter_list = cols_filter_list or consts.COLUMN_FILTER_LIST
    if format == "text":
        return result_df.drop(cols_filter_list, axis=1).to_string(index=False)
    elif format == "csv":
        return result_df.to_csv(index=False, lineterminator="\n")
    elif format == "json":
        return result_df.to_json(orient="index")
    else:
        return result_df.drop(cols_filter_list, axis=1).to_markdown(
            tablefmt="fancy_grid", index=False
        )


class TextResultHandler(object):
    def __init__(
        self, format, status_list=None, cols_filter_list=consts.COLUMN_FILTER_LIST
    ):
        self.format = format
        self.cols_filter_list = cols_filter_list
        self.status_list = status_list

    def _get_formatted(self, result_df):
        return get_formatted(result_df, self.format, self.cols_filter_list)

    def print_formatted_(self, result_df) -> "DataFrame":
        """
        Utility for printing formatted results
        :param result_df
        """
        if self.status_list is not None:
            result_df = filter_validation_status(self.status_list, result_df)

        print(self._get_formatted(result_df))

        if self.format not in consts.FORMAT_TYPES:
            error_msg = (
                f"format [{self.format}] not supported, results printed in default(table) mode. "
                f"Supported formats are [text, csv, json, table]"
            )
            raise ValueError(error_msg)

        return result_df

    def execute(self, result_df) -> str:
        return self.print_formatted_(result_df)
