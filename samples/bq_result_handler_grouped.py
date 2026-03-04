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

import os

from data_validation import data_validation
from data_validation.result_handlers import bigquery as bqhandler

PROJECT_ID = os.environ["PROJECT_ID"]
BQ_CONN = {"source_type": "BigQuery", "project_id": PROJECT_ID}
GROUPED_CONFIG_COUNT_VALID = {
    # BigQuery Specific Connection Config
    "source_conn": BQ_CONN,
    "target_conn": BQ_CONN,
    # Validation Type
    "type": "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    "grouped_columns": [
        {
            "field_alias": "starttime",
            "source_column": "starttime",
            "target_column": "starttime",
            "cast": "date",
        },
    ],
    "aggregates": [
        {
            "field_alias": "count",
            "source_column": None,
            "target_column": None,
            "type": "count",
        },
    ],
}

result_handler = bqhandler.BigQueryResultHandler.get_handler_for_project(PROJECT_ID)
validator = data_validation.DataValidation(
    GROUPED_CONFIG_COUNT_VALID, verbose=True, result_handler=result_handler
)
df = validator.execute()
