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

import pytest

from pyfakefs.fake_filesystem_unittest import patchfs

from data_validation import consts


TABLE_FILE_PATH = "table_data.json"
SAMPLE_CONFIG = {
    # BigQuery Specific Connection Config
    consts.CONFIG_SOURCE_CONN: {
        consts.SOURCE_TYPE: "Pandas",
        "table_name": "my_table",
        "file_path": TABLE_FILE_PATH,
        "file_type": "json",
    },
    consts.CONFIG_TARGET_CONN: {
        consts.SOURCE_TYPE: "Pandas",
        "table_name": "my_table",
        "file_path": TABLE_FILE_PATH,
        "file_type": "json",
    },
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "bigquery-public-data.new_york_citibike",
    consts.CONFIG_TABLE_NAME: "citibike_trips",
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_RESULT_HANDLER: {
        consts.CONFIG_TYPE: "BigQuery",
        consts.PROJECT_ID: "my-project",
        consts.TABLE_ID: "dataset.table_name",
    },
}

JSON_DATA = """[{"col_a":0,"col_b":"b"}]"""


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
    module_under_test.DataValidation(SAMPLE_CONFIG)
