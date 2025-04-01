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
        cols = ",".join(
            [
                consts.CONFIG_RUN_ID,
                consts.CONFIG_START_TIME,
                consts.CONFIG_END_TIME,
                consts.SOURCE_TABLE_NAME,
                consts.SOURCE_COLUMN_NAME,
                consts.TARGET_TABLE_NAME,
                consts.TARGET_COLUMN_NAME,
                consts.VALIDATION_TYPE,
                consts.AGGREGATION_TYPE,
                consts.VALIDATION_NAME,
                consts.SOURCE_AGG_VALUE,
                consts.TARGET_AGG_VALUE,
                consts.GROUP_BY_COLUMNS,
                consts.CONFIG_PRIMARY_KEYS,
                consts.NUM_RANDOM_ROWS,
                consts.VALIDATION_DIFFERENCE,
                consts.VALIDATION_PCT_DIFFERENCE,
                consts.VALIDATION_PCT_THRESHOLD,
                consts.VALIDATION_STATUS,
                consts.CONFIG_LABELS,
            ]
        )
        result = bigquery_client.query(
            f"SELECT {cols} FROM `{table_id}` ORDER BY {consts.TARGET_AGG_VALUE} ASC"
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
            consts.CONFIG_RUN_ID: ["grouped-test"] * 6,
            consts.CONFIG_START_TIME: [start] * 6,
            consts.CONFIG_END_TIME: [end] * 6,
            consts.SOURCE_TABLE_NAME: [
                "schema.test_source",
                "schema.test_source",
                _NAN,
                _NAN,
                "schema.test_source",
                "schema.test_source",
            ],
            consts.SOURCE_COLUMN_NAME: [
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
            consts.TARGET_COLUMN_NAME: [
                "target_column",
                "target_column",
                "target_column",
                "target_column",
                _NAN,
                _NAN,
            ],
            consts.VALIDATION_TYPE: ["Column"] * 6,
            consts.AGGREGATION_TYPE: ["count"] * 6,
            consts.VALIDATION_NAME: ["count"] * 6,
            consts.SOURCE_AGG_VALUE: ["2", "4", _NAN, _NAN, "6", "8"],
            consts.TARGET_AGG_VALUE: ["1", "3", "5", "7", "8", "9"],
            consts.GROUP_BY_COLUMNS: [
                '{"grp_a": "a", "grp_i": "0"}',
                '{"grp_a": "a", "grp_i": "1"}',
                '{"grp_a": "b", "grp_i": "0"}',
                '{"grp_a": "b", "grp_i": "1"}',
                '{"grp_a": "c", "grp_i": "0"}',
                '{"grp_a": "c", "grp_i": "1"}',
            ],
            consts.CONFIG_PRIMARY_KEYS: [None, None, None, None, None, None],
            consts.NUM_RANDOM_ROWS: [_NAN, _NAN, _NAN, _NAN, _NAN, _NAN],
            consts.VALIDATION_DIFFERENCE: [-1.0, -1.0, _NAN, _NAN, _NAN, _NAN],
            consts.VALIDATION_PCT_DIFFERENCE: [-50.0, -25.0, _NAN, _NAN, _NAN, _NAN],
            consts.VALIDATION_PCT_THRESHOLD: [25.0, 25.0, _NAN, _NAN, _NAN, _NAN],
            consts.VALIDATION_STATUS: [
                consts.VALIDATION_STATUS_FAIL,
                consts.VALIDATION_STATUS_SUCCESS,
                _NAN,
                _NAN,
                _NAN,
                _NAN,
            ],
            consts.CONFIG_LABELS: [[{"key": "name", "value": "test_label"}]] * 6,
        }
    )
    object_under_test.execute(df)
    result = get_dataframe(bigquery_client, table_id)
    pandas.testing.assert_frame_equal(result, df)
    bigquery_client.delete_table(table_id)
