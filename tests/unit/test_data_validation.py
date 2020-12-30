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

import pandas
import pytest

from pyfakefs.fake_filesystem_unittest import patchfs

from data_validation import consts


TABLE_FILE_PATH = "table_data.json"
SAMPLE_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": {
        "source_type": "Pandas",
        "table_name": "my_table",
        "file_path": TABLE_FILE_PATH,
        "file_type": "json",
    },
    "target_conn": {
        "source_type": "Pandas",
        "table_name": "my_table",
        "file_path": TABLE_FILE_PATH,
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
    consts.CONFIG_AGGREGATES: [{'source_column': None, 'target_column': None, 'field_alias': 'count', 'type': 'count'}],
    consts.CONFIG_RESULT_HANDLER: None,
}

JSON_DATA = """[{"col_a":0,"col_b":"b"}]"""

SOURCE_QUERY_DATA = [
    {"date": "2020-01-01", "int_val": 1, "double_val": 2.3, "text_val": "hello"}
]
SOURCE_DF = pandas.DataFrame(SOURCE_QUERY_DATA)
JOIN_ON_FIELDS = ["date"]
NON_OBJECT_FIELDS = pandas.Index(["int_val", "double_val"])


@pytest.fixture
def module_under_test():
    import data_validation.data_validation

    return data_validation.data_validation


def test_import(module_under_test):
    # TODO: make better tests than just verifying import
    assert True


def _create_table_file():
    """ Create JSON File """
    with open(TABLE_FILE_PATH, "w") as f:
        f.write(JSON_DATA)


@patchfs
def test_data_validation_client(module_under_test, fs):
    """ Test getting a Data Validation Client """
    _create_table_file()
    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    assert int(result_df.source_agg_value[0]) == 1


def test_get_pandas_schema(module_under_test):
    """ Test extracting pandas schema from dataframes for Ibis Pandas."""
    pandas_schema = module_under_test.DataValidation._get_pandas_schema(
        SOURCE_DF, SOURCE_DF, JOIN_ON_FIELDS
    )

    assert (pandas_schema.index == NON_OBJECT_FIELDS).all()
