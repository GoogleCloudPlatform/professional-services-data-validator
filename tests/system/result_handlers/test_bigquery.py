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

import datetime
import pathlib
import time

import google.cloud.bigquery
import pandas
import pandas.testing

from data_validation import consts


REPO_ROOT = pathlib.Path(__file__).parent.parent.parent.parent
SCHEMA_PATH = REPO_ROOT / "terraform" / "results_schema.json"
_NAN = float("nan")
GET_DATAFRAME_TIMEOUT_SECONDS = 30


def get_now():
    # Round to nearest seconds. For some reason, when microsecond precision is
    # used the time ends up 1 microsecond different in the round-trip. Not
    # sure if it's due to pandas, arrow, or BigQuery.
    now = datetime.datetime.now(datetime.timezone.utc)
    return datetime.datetime(
        now.year,
        now.month,
        now.day,
        now.hour,
        now.minute,
        now.second,
        tzinfo=datetime.timezone.utc,
    )


def create_bigquery_results_table(bigquery_client, table_id):
    schema = bigquery_client.schema_from_json(SCHEMA_PATH)
    table = google.cloud.bigquery.Table(table_id, schema=schema)
    return bigquery_client.create_table(table)


def get_dataframe(bigquery_client, table_id):
    timeout = time.time() + GET_DATAFRAME_TIMEOUT_SECONDS
    while True:
        # Run a query rather than call list_rows so that rows are fetched from
        # the streaming buffer.
        result = bigquery_client.query(
            "SELECT run_id, start_time, end_time, source_table_name, "
            "source_column_name, target_table_name, target_column_name, "
            "validation_type, aggregation_type, validation_name, "
            "source_agg_value, target_agg_value, group_by_columns, "
            "difference, pct_difference, pct_threshold, validation_status, labels "
            f" FROM `{table_id}` ORDER BY target_agg_value ASC"
        ).to_dataframe()

        if len(result.index) > 0 or time.time() > timeout:
            return result


def get_handler(bigquery_client, table_id):
    import data_validation.result_handlers.bigquery

    return data_validation.result_handlers.bigquery.BigQueryResultHandler(
        bigquery_client, table_id=table_id
    )


def test_execute_with_nan(bigquery_client, bigquery_dataset_id):
    table_id = f"{bigquery_dataset_id}.test_execute_with_nan"
    object_under_test = get_handler(bigquery_client, table_id)
    create_bigquery_results_table(bigquery_client, table_id)
    end = get_now()
    start = end - datetime.timedelta(minutes=1)
    df = pandas.DataFrame(
        {
            "run_id": ["grouped-test"] * 6,
            "start_time": [start] * 6,
            "end_time": [end] * 6,
            "source_table_name": [
                "schema.test_source",
                "schema.test_source",
                _NAN,
                _NAN,
                "schema.test_source",
                "schema.test_source",
            ],
            "source_column_name": [
                "source_column",
                "source_column",
                _NAN,
                _NAN,
                "source_column",
                "source_column",
            ],
            "target_table_name": [
                "schema.test_target",
                "schema.test_target",
                "schema.test_target",
                "schema.test_target",
                _NAN,
                _NAN,
            ],
            "target_column_name": [
                "target_column",
                "target_column",
                "target_column",
                "target_column",
                _NAN,
                _NAN,
            ],
            "validation_type": ["Column"] * 6,
            "aggregation_type": ["count"] * 6,
            "validation_name": ["count"] * 6,
            "source_agg_value": ["2", "4", _NAN, _NAN, "6", "8"],
            "target_agg_value": ["1", "3", "5", "7", "8", "9"],
            "group_by_columns": [
                '{"grp_a": "a", "grp_i": "0"}',
                '{"grp_a": "a", "grp_i": "1"}',
                '{"grp_a": "b", "grp_i": "0"}',
                '{"grp_a": "b", "grp_i": "1"}',
                '{"grp_a": "c", "grp_i": "0"}',
                '{"grp_a": "c", "grp_i": "1"}',
            ],
            "difference": [-1.0, -1.0, _NAN, _NAN, _NAN, _NAN],
            "pct_difference": [-50.0, -25.0, _NAN, _NAN, _NAN, _NAN],
            "pct_threshold": [25.0, 25.0, _NAN, _NAN, _NAN, _NAN],
            "validation_status": [
                consts.VALIDATION_STATUS_FAIL,
                consts.VALIDATION_STATUS_SUCCESS,
                _NAN,
                _NAN,
                _NAN,
                _NAN,
            ],
            "labels": [[{"key": "name", "value": "test_label"}]] * 6,
        }
    )
    object_under_test.execute(None, df)
    result = get_dataframe(bigquery_client, table_id)
    pandas.testing.assert_frame_equal(result, df)
    bigquery_client.delete_table(table_id)
