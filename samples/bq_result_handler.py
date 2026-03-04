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
CONFIG_COUNT_VALID = {
    # BigQuery Specific Connection Config
    "source_conn": BQ_CONN,
    "target_conn": BQ_CONN,
    # Validation Type
    "type": "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    "aggregates": [
        {
            "field_alias": "count",
            "source_column": None,
            "target_column": None,
            "type": "count",
        },
        {
            "field_alias": "count__tripduration",
            "source_column": "tripduration",
            "target_column": "tripduration",
            "type": "count",
        },
        {
            "field_alias": "count__start_station_name",
            "source_column": "start_station_name",
            "target_column": "start_station_name",
            "type": "count",
        },
        {
            "field_alias": "sum__tripduration",
            "source_column": "tripduration",
            "target_column": "tripduration",
            "type": "sum",
        },
        {
            "field_alias": "max__tripduration",
            "source_column": "tripduration",
            "target_column": "tripduration",
            "type": "max",
        },
        {
            "field_alias": "min__tripduration",
            "source_column": "tripduration",
            "target_column": "tripduration",
            "type": "min",
        },
        {
            "field_alias": "avg__tripduration",
            "source_column": "tripduration",
            "target_column": "tripduration",
            "type": "avg",
        },
    ],
}

result_handler = bqhandler.BigQueryResultHandler.get_handler_for_project(PROJECT_ID)
validator = data_validation.DataValidation(
    CONFIG_COUNT_VALID, verbose=True, result_handler=result_handler
)
df = validator.execute()
