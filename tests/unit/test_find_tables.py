# Copyright 2024 Google LLC
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
from unittest import mock

from data_validation import consts


S1_TABLES = [
    ("s1", "t1"),
    ("s1", "t2"),
    ("s1", "t3"),
]

SCHEMA_TABLE_OBJ = {
    consts.CONFIG_SCHEMA_NAME: "schema",
    consts.CONFIG_TABLE_NAME: "table",
}
OTHER_SCHEMA_TABLE_OBJ = {
    consts.CONFIG_SCHEMA_NAME: "schema",
    consts.CONFIG_TABLE_NAME: "other_table",
}
SOURCE_TABLE_MAP = {
    "schema_table": SCHEMA_TABLE_OBJ,
}
TARGET_TABLE_MAP = {
    "schema_table": SCHEMA_TABLE_OBJ,
    "schema_other_table": OTHER_SCHEMA_TABLE_OBJ,
}
RESULT_TABLE_CONFIGS = [
    {
        "schema_name": "schema",
        "table_name": "table",
        "target_schema_name": "schema",
        "target_table_name": "table",
    }
]


@pytest.fixture
def module_under_test():
    from data_validation import find_tables

    return find_tables


def test__compare_match_tables(module_under_test):
    """Test matching tables from source and target."""
    table_configs = module_under_test._compare_match_tables(
        SOURCE_TABLE_MAP, TARGET_TABLE_MAP
    )

    assert table_configs == RESULT_TABLE_CONFIGS


@pytest.mark.parametrize(
    ("tables_list,expected_result"),
    (
        # Test that lone asterisk is expanded.
        (
            [
                {"schema_name": "s1", "table_name": "*"},
            ],
            [
                {
                    "schema_name": _[0],
                    "table_name": _[1],
                    "target_schema_name": _[0],
                    "target_table_name": _[1],
                }
                for _ in S1_TABLES
            ],
        ),
        # Test that arg format s1.t1=s2.t1 is not expanded.
        (
            [
                {
                    "schema_name": "s1",
                    "table_name": "t1",
                    "target_schema_name": "s2",
                    "target_table_name": "t1",
                },
            ],
            [
                {
                    "schema_name": "s1",
                    "table_name": "t1",
                    "target_schema_name": "s2",
                    "target_table_name": "t1",
                },
            ],
        ),
        # Test that arg format s1.t1 is not expanded.
        (
            [
                {"schema_name": "s1", "table_name": "t1"},
            ],
            [
                {
                    "schema_name": "s1",
                    "table_name": "t1",
                },
            ],
        ),
        # Test that asterisk mixed with other characters is not expanded
        (
            [
                {"schema_name": "s1", "table_name": "t*"},
            ],
            [
                {
                    "schema_name": "s1",
                    "table_name": "t*",
                },
            ],
        ),
    ),
)
def test_expand_tables_of_asterisk(
    module_under_test, tables_list: list, expected_result: list
):
    with mock.patch(
        "data_validation.clients.get_all_tables",
        return_value=S1_TABLES,
    ) as _:
        result = module_under_test.expand_tables_of_asterisk(
            tables_list, mock.Mock(), mock.Mock()
        )
        assert result == expected_result
