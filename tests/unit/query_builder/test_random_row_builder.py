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

from data_validation import clients
from data_validation.query_builder.query_builder import QueryBuilder


TABLE_FILE_PATH = "table_data.json"
CONN_CONFIG = {
    "source_type": "FileSystem",
    "table_name": "my_table",
    "file_path": TABLE_FILE_PATH,
    "file_type": "json",
}

JSON_DATA = """
[
{"col_a":0,"col_b":"a"},{"col_a":1,"col_b":"b"},{"col_a":2,"col_b":"c"},
{"col_a":3,"col_b":"a"},{"col_a":4,"col_b":"b"},{"col_a":5,"col_b":"c"},
{"col_a":6,"col_b":"a"},{"col_a":7,"col_b":"b"},{"col_a":8,"col_b":"c"},
{"col_a":9,"col_b":"a"},{"col_a":10,"col_b":"b"},{"col_a":11,"col_b":"c"},
{"col_a":12,"col_b":"a"},{"col_a":13,"col_b":"b"},{"col_a":14,"col_b":"c"},
{"col_a":15,"col_b":"a"},{"col_a":16,"col_b":"b"},{"col_a":17,"col_b":"c"},
{"col_a":18,"col_b":"a"},{"col_a":19,"col_b":"b"},{"col_a":20,"col_b":"c"},
{"col_a":21,"col_b":"a"},{"col_a":22,"col_b":"b"},{"col_a":23,"col_b":"c"}
]"""


@pytest.fixture
def module_under_test():
    import data_validation.query_builder.random_row_builder

    return data_validation.query_builder.random_row_builder


def _create_table_file(table_path, data):
    """Create JSON File"""
    with open(table_path, "w") as f:
        f.write(data)


def test_import(module_under_test):
    assert module_under_test is not None


def test_init(module_under_test):
    primary_keys = ["abc", "def"]
    builder = module_under_test.RandomRowBuilder(primary_keys, 100)

    assert builder.primary_keys == primary_keys


def test_compile(module_under_test, fs):
    _create_table_file(TABLE_FILE_PATH, JSON_DATA)
    client = clients.get_data_client(CONN_CONFIG)
    primary_keys = ["col_a"]
    builder = module_under_test.RandomRowBuilder(primary_keys, 10)

    query = builder.compile(
        client, None, CONN_CONFIG["table_name"], QueryBuilder([], [], [], [], [], None)
    )
    df = client.execute(query)

    assert list(df.columns) == primary_keys
    assert len(df) == 10
