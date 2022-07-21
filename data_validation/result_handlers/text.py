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
for the vlaidation and Pandas DataFrame with the results
from the validation run.

Output validation report to text-based log
"""

from data_validation import consts


class TextResultHandler(object):
    def __init__(self, format, cols_filter_list=consts.COLUMN_FILTER_LIST):
        self.format = format
        self.cols_filter_list = cols_filter_list

    def print_formatted_(self, result_df):
        """
        Utility for printing formatted results
        :param result_df
        """
        if self.format == "text":
            print(result_df.to_string(index=False))
        elif self.format == "csv":
            print(result_df.to_csv(index=False))
        elif self.format == "json":
            print(result_df.to_json(orient="index"))
        else:
            print(
                result_df.drop(self.cols_filter_list, axis=1).to_markdown(
                    tablefmt="fancy_grid", index=False
                )
            )

        if self.format not in consts.FORMAT_TYPES:
            error_msg = (
                f"format [{self.format}] not supported, results printed in default(table) mode. "
                f"Supported formats are [text, csv, json, table]"
            )
            raise ValueError(error_msg)

        return result_df

    def execute(self, config, result_df):
        self.print_formatted_(result_df)
        return result_df
