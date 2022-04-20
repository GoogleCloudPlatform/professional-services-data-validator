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

import json
import pandas
import pytest
import random
from datetime import datetime, timedelta

from data_validation import consts


SOURCE_TABLE_FILE_PATH = "source_table_data.json"
TARGET_TABLE_FILE_PATH = "target_table_data.json"

SOURCE_CONN_CONFIG = {
    "source_type": "FileSystem",
    "table_name": "my_table",
    "file_path": SOURCE_TABLE_FILE_PATH,
    "file_type": "json",
}

TARGET_CONN_CONFIG = {
    "source_type": "FileSystem",
    "table_name": "my_table",
    "file_path": TARGET_TABLE_FILE_PATH,
    "file_type": "json",
}

SAMPLE_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "col_a",
            "target_column": "col_a",
            "field_alias": "count_col_a",
            "type": "count",
        },
        {
            "source_column": "col_b",
            "target_column": "col_b",
            "field_alias": "count_col_b",
            "type": "count",
        },
    ],
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

SAMPLE_THRESHOLD_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "col_a",
            "target_column": "col_a",
            "field_alias": "count_col_a",
            "type": "count",
        },
        {
            "source_column": "col_b",
            "target_column": "col_b",
            "field_alias": "count_col_b",
            "type": "count",
        },
    ],
    consts.CONFIG_THRESHOLD: 150.0,
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

# Grouped Column Row confg
SAMPLE_GC_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_MAX_RECURSIVE_QUERY_SIZE: 50,
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [
        {
            consts.CONFIG_FIELD_ALIAS: "date_value",
            consts.CONFIG_SOURCE_COLUMN: "date_value",
            consts.CONFIG_TARGET_COLUMN: "date_value",
            consts.CONFIG_CAST: "date",
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "text_value",
            "target_column": "text_value",
            "field_alias": "count_text_value",
            "type": "count",
        },
    ],
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

# Grouped Column Row confg
SAMPLE_MULTI_GC_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_MAX_RECURSIVE_QUERY_SIZE: 50,
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [
        {
            consts.CONFIG_FIELD_ALIAS: "date_value",
            consts.CONFIG_SOURCE_COLUMN: "date_value",
            consts.CONFIG_TARGET_COLUMN: "date_value",
            consts.CONFIG_CAST: "date",
        },
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        }
    ],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "text_value",
            "target_column": "text_value",
            "field_alias": "count_text_value",
            "type": "count",
        },
    ],
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

SAMPLE_GC_CALC_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_MAX_RECURSIVE_QUERY_SIZE: 50,
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [
        {
            consts.CONFIG_FIELD_ALIAS: "date_value",
            consts.CONFIG_SOURCE_COLUMN: "date_value",
            consts.CONFIG_TARGET_COLUMN: "date_value",
            consts.CONFIG_CAST: "date",
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_CALCULATED_FIELDS: [
        {
            "source_calculated_columns": ["text_constant"],
            "target_calculated_columns": ["text_constant"],
            "field_alias": "length_text_constant",
            "type": "length",
            "depth": 0,
        },
        {
            "source_calculated_columns": ["text_constant"],
            "target_calculated_columns": ["text_constant"],
            "field_alias": "upper_text_constant",
            "type": "upper",
            "depth": 0,
        },
        {
            "source_calculated_columns": [
                "length_text_constant",
                "upper_text_constant",
            ],
            "target_calculated_columns": [
                "length_text_constant",
                "upper_text_constant",
            ],
            "field_alias": "concat_multi",
            "type": "concat",
            "depth": 1,
        },
        {
            "source_calculated_columns": ["concat_multi"],
            "target_calculated_columns": ["concat_multi"],
            "field_alias": "concat_length",
            "type": "length",
            "depth": 2,
        },
    ],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "text_value",
            "target_column": "text_value",
            "field_alias": "count_text_value",
            "type": "count",
        },
        {
            "source_column": "length_text_constant",
            "target_column": "length_text_constant",
            "field_alias": "sum_length",
            "type": "sum",
        },
        {
            "source_column": "text_numeric",
            "target_column": "text_numeric",
            "field_alias": "sum_text_numeric",
            "type": "sum",
            "cast": "int64",
        },
        {
            "source_column": "concat_length",
            "target_column": "concat_length",
            "field_alias": "sum_concat_length",
            "type": "sum",
        },
    ],
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

SAMPLE_RANDOM_ROW_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "int_value",
            "target_column": "int_value",
            "field_alias": "count_int_value",
            "type": "sum",
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_USE_RANDOM_ROWS: True,
    consts.CONFIG_RANDOM_ROW_BATCH_SIZE: 10,
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

# Row confg
SAMPLE_ROW_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: consts.ROW_VALIDATION,
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_COMPARISON_FIELDS: [
        {
            consts.CONFIG_FIELD_ALIAS: "int_value",
            consts.CONFIG_SOURCE_COLUMN: "int_value",
            consts.CONFIG_TARGET_COLUMN: "int_value",
            consts.CONFIG_CAST: None,
        },
        {
            consts.CONFIG_FIELD_ALIAS: "text_value",
            consts.CONFIG_SOURCE_COLUMN: "text_value",
            consts.CONFIG_TARGET_COLUMN: "text_value",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

# Row confg
SAMPLE_JSON_ROW_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: consts.ROW_VALIDATION,
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "pkey",
            consts.CONFIG_SOURCE_COLUMN: "pkey",
            consts.CONFIG_TARGET_COLUMN: "pkey",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_COMPARISON_FIELDS: [
        {
            consts.CONFIG_FIELD_ALIAS: "col_b",
            consts.CONFIG_SOURCE_COLUMN: "col_b",
            consts.CONFIG_TARGET_COLUMN: "col_b",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_RESULT_HANDLER: None,
    consts.CONFIG_FORMAT: "table",
}

JSON_DATA = """[{"col_a":1,"col_b":"a"},{"col_a":1,"col_b":"b"}]"""
JSON_COLA_ZERO_DATA = """[{"col_a":null,"col_b":"a"}]"""
JSON_BAD_DATA = """[{"col_a":0,"col_b":"a"},{"col_a":1,"col_b":"b"},{"col_a":2,"col_b":"c"},{"col_a":3,"col_b":"d"},{"col_a":4,"col_b":"e"}]"""
JSON_PK_DATA = (
    """[{"pkey":1, "col_a":1,"col_b":"a"},{"pkey":2, "col_a":1,"col_b":"b"}]"""
)
JSON_PK_BAD_DATA = """[{"pkey":1, "col_a":0,"col_b":"b"},{"pkey":2, "col_a":1,"col_b":"c"},{"pkey":3, "col_a":2,"col_b":"d"},{"pkey":4, "col_a":3,"col_b":"e"},{"pkey":5, "col_a":4,"col_b":"f"}]"""

STRING_CONSTANT = "constant"

SOURCE_QUERY_DATA = [
    {
        "date": "2020-01-01",
        "int_val": 1,
        "double_val": 2.3,
        "text_constant": STRING_CONSTANT,
        "text_numeric": "2",
        "text_val": "hello",
        "text_val_two": "goodbye",
    }
]
SOURCE_DF = pandas.DataFrame(SOURCE_QUERY_DATA)
JOIN_ON_FIELDS = ["date"]
NON_OBJECT_FIELDS = pandas.Index(["int_val", "double_val"])

RANDOM_STRINGS = ["a", "b", "c", "d", None]
RANDOM_STRINGS = ["a", "b", "c", "d"]


@pytest.fixture
def module_under_test():
    import data_validation.data_validation

    return data_validation.data_validation


def _create_table_file(table_path, data):
    """Create JSON File"""
    with open(table_path, "w") as f:
        f.write(data)


def _generate_fake_data(
    rows=10, initial_id=0, second_range=60 * 60 * 24, int_range=100, random_strings=None
):
    """Return a list of dicts with given number of rows.

    Data Keys:
        id: a unique int per row
        timestamp_value: a random timestamp in the past {second_range} back
        date_value: a random date in the past {second_range} back
        int_value: a random int value inside 0 to {int_range}
        text_value: a random string from supplied list
    """
    data = []
    random_strings = random_strings or RANDOM_STRINGS
    for i in range(initial_id, initial_id + rows):
        rand_seconds = random.randint(0, second_range)
        rand_timestamp = datetime.now() - timedelta(seconds=rand_seconds)
        rand_date = rand_timestamp.date()

        row = {
            "id": i,
            "date_value": rand_date,
            "timestamp_value": rand_timestamp,
            "int_value": random.randint(0, int_range),
            "text_constant": STRING_CONSTANT,
            "text_numeric": "2",
            "text_value": random.choice(random_strings),
            "text_value_two": random.choice(random_strings),
        }
        data.append(row)

    return data


def _get_fake_json_data(data):
    for row in data:
        row["date_value"] = str(row["date_value"])
        row["timestamp_value"] = str(row["timestamp_value"])
        row["text_constant"] = row["text_constant"]
        row["text_numeric"] = row["text_numeric"]
        row["text_value"] = row["text_value"]
        row["text_value_two"] = row["text_value_two"]

    return json.dumps(data)


def test_import(module_under_test):
    assert True


def test_data_validation_client(module_under_test, fs):
    """Test getting a Data Validation Client"""
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()
    assert int(result_df.source_agg_value[0]) == 2


def test_get_pandas_schema(module_under_test):
    """Test extracting pandas schema from dataframes for Ibis Pandas."""
    pandas_schema = module_under_test.DataValidation._get_pandas_schema(
        SOURCE_DF, SOURCE_DF, JOIN_ON_FIELDS
    )

    assert (pandas_schema.index == NON_OBJECT_FIELDS).all()


def test_zero_source_value(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]

    assert col_a_pct_diff == 100


def test_zero_target_value(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]

    assert col_a_pct_diff == -100


def test_zero_both_values(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]

    assert col_a_pct_diff == 0.0


def test_status_success_validation(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_threshold = col_a_result_df.pct_threshold.values[0]
    col_a_status = col_a_result_df.validation_status.values[0]

    assert col_a_pct_threshold == 0.0
    assert col_a_status == consts.VALIDATION_STATUS_SUCCESS


def test_status_fail_validation(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()
    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_threshold = col_a_result_df.pct_threshold.values[0]
    col_a_status = col_a_result_df.validation_status.values[0]

    assert col_a_pct_threshold == 0.0
    assert col_a_status == consts.VALIDATION_STATUS_FAIL


def test_threshold_equals_diff(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_BAD_DATA)

    client = module_under_test.DataValidation(SAMPLE_THRESHOLD_CONFIG)
    result_df = client.execute()
    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]
    col_a_pct_threshold = col_a_result_df.pct_threshold.values[0]
    col_a_status = col_a_result_df.validation_status.values[0]

    assert col_a_pct_diff == 150.0
    assert col_a_pct_threshold == 150.0
    assert col_a_status == consts.VALIDATION_STATUS_SUCCESS


def test_grouped_column_level_validation_perfect_match(module_under_test, fs):
    data = _generate_fake_data(second_range=0)
    json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, json_data)

    client = module_under_test.DataValidation(SAMPLE_GC_CONFIG)
    result_df = client.execute()

    expected_date_result = '{"date_value": "%s"}' % str(datetime.now().date())
    assert expected_date_result == result_df["group_by_columns"].max()

    assert result_df["difference"].sum() == 0


def test_calc_field_validation_calc_match(module_under_test, fs):
    num_rows = 100
    data = _generate_fake_data(rows=num_rows, second_range=0)
    json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, json_data)

    client = module_under_test.DataValidation(SAMPLE_GC_CALC_CONFIG)
    result_df = client.execute()
    calc_val_df = result_df[result_df["validation_name"] == "sum_length"]
    calc_val_df2 = result_df[result_df["validation_name"] == "sum_concat_length"]
    calc_val_df3 = result_df[result_df["validation_name"] == "sum_text_numeric"]

    assert calc_val_df["source_agg_value"].sum() == str(num_rows * len(STRING_CONSTANT))

    assert calc_val_df2["source_agg_value"].sum() == str(
        num_rows * (len(STRING_CONSTANT + str(len(STRING_CONSTANT))))
    )

    assert calc_val_df3["source_agg_value"].sum() == str(num_rows * 2)


def test_grouped_column_level_validation_non_matching(module_under_test, fs):
    data = _generate_fake_data(rows=10, second_range=0)
    trg_data = _generate_fake_data(initial_id=11, rows=1, second_range=0)
    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data + trg_data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)
    client = module_under_test.DataValidation(SAMPLE_GC_CONFIG)
    result_df = client.execute()
    validation_df = result_df[result_df["validation_name"] == "count_text_value"]
    # TODO: this value is 0 because a COUNT() on no rows returns Null
    assert result_df["difference"].sum() == 1

    expected_date_result = '{"date_value": "%s"}' % str(datetime.now().date())
    grouped_column = validation_df["group_by_columns"].max()
    assert expected_date_result == grouped_column


def test_grouped_column_level_validation_smart_count(module_under_test, fs):
    data = _generate_fake_data(rows=100, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data + data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_GC_CONFIG)
    result_df = client.execute()
    expected_date_result = '{"date_value": "%s"}' % str(datetime.now().date())

    assert expected_date_result == result_df["group_by_columns"].max()

    smart_count_df = result_df[result_df["validation_name"] == "count_text_value"]
    assert smart_count_df["source_agg_value"].astype(int).sum() == 100
    assert smart_count_df["target_agg_value"].astype(int).sum() == 200


def test_grouped_column_level_validation_multiple_aggregations(module_under_test):
    data = _generate_fake_data(rows=10, second_range=0)
    trg_data = _generate_fake_data(initial_id=11, rows=1, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data + trg_data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_MULTI_GC_CONFIG)
    result_df = client.execute()
    validation_df = result_df  # [result_df["validation_name"] == "count_text_value"]
    # Expect 11 rows, one for each PK value
    assert len(validation_df) == 11
    assert validation_df["source_agg_value"].astype(float).sum() == 10
    assert validation_df["target_agg_value"].astype(float).sum() == 11


def test_row_level_validation(module_under_test, fs):
    data = _generate_fake_data(rows=100, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG)
    result_df = client.execute()

    str_comparison_df = result_df[result_df["validation_name"] == "text_value"]
    int_comparison_df = result_df[result_df["validation_name"] == "int_value"]

    assert len(result_df) == 200
    assert len(str_comparison_df) == 100
    assert len(int_comparison_df) == 100


def test_fail_row_level_validation(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_PK_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_PK_BAD_DATA)

    client = module_under_test.DataValidation(SAMPLE_JSON_ROW_CONFIG)
    result_df = client.execute()

    # based on shared keys
    fail_df = result_df[result_df["validation_status"] == consts.VALIDATION_STATUS_FAIL]
    assert len(fail_df) == 5


def test_bad_join_row_level_validation(module_under_test, fs):
    data = _generate_fake_data(rows=100, second_range=0)
    target_data = _generate_fake_data(initial_id=100, rows=1, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(target_data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG)
    result_df = client.execute()

    comparison_df = result_df[
        result_df["validation_status"] == consts.VALIDATION_STATUS_FAIL
    ]
    # 2 validations * (100 source + 1 target)
    assert len(result_df) == 202
    assert len(comparison_df) == 202


def test_random_row_level_validation(module_under_test, fs):
    data = _generate_fake_data(rows=100, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_RANDOM_ROW_CONFIG)
    result_df = client.execute()

    # Random Row Validation with 10 rows
    ids = [int(json.loads(c)["id"]) for c in result_df["group_by_columns"]]
    assert len(result_df) == 10
    assert result_df["difference"].sum() == 0
    assert ids != [i for i in range(10)]
    assert ids != [i for i in range(90, 100)]
