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

import numpy
import pandas
import pytest

from ibis.pandas.client import PandasClient

from data_validation import consts, exceptions


SOURCE_TABLE_FILE_PATH = "source_table_data.json"
TARGET_TABLE_FILE_PATH = "target_table_data.json"
SAMPLE_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": {
        "source_type": "Pandas",
        "table_name": "my_table",
        "file_path": SOURCE_TABLE_FILE_PATH,
        "file_type": "json",
    },
    "target_conn": {
        "source_type": "Pandas",
        "table_name": "my_table",
        "file_path": TARGET_TABLE_FILE_PATH,
        "file_type": "json",
    },
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
    consts.CONFIG_RESULT_HANDLER: None,
}

JSON_DATA = """[{"col_a":0,"col_b":"a"},{"col_a":1,"col_b":"b"}]"""
JSON_COLA_ZERO_DATA = """[{"col_a":null,"col_b":"a"}]"""

SOURCE_QUERY_DATA = [
    {"date": "2020-01-01", "int_val": 1, "double_val": 2.3, "text_val": "hello"}
]
SOURCE_DF = pandas.DataFrame(SOURCE_QUERY_DATA)
JOIN_ON_FIELDS = ["date"]
NON_OBJECT_FIELDS = pandas.Index(["int_val", "double_val"])

ORACLE_CONN_CONFIG = {
    "source_type": "Oracle",
    "host": "127.0.0.1",
    "port": 1521,
}


@pytest.fixture
def module_under_test():
    import data_validation.data_validation

    return data_validation.data_validation


def test_import(module_under_test):
    # TODO: make better tests than just verifying import
    assert True


def _create_table_file(table_path, data):
    """ Create JSON File """
    with open(table_path, "w") as f:
        f.write(data)


def test_data_validation_client(module_under_test, fs):
    """ Test getting a Data Validation Client """
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    assert int(result_df.source_agg_value[0]) == 2


def test_get_pandas_schema(module_under_test):
    """ Test extracting pandas schema from dataframes for Ibis Pandas."""
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

    assert numpy.isnan(col_a_pct_diff)


def test_get_pandas_data_client(module_under_test, fs):
    conn_config = SAMPLE_CONFIG["source_conn"]
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    ibis_client = module_under_test.DataValidation.get_data_client(conn_config)

    assert isinstance(ibis_client, PandasClient)


def test_get_oracle_data_client(module_under_test):
    with pytest.raises(
        exceptions.DataClientConnectionFailure, match=r".*pip install cx_Oracle"
    ):
        module_under_test.DataValidation.get_data_client(ORACLE_CONN_CONFIG)
