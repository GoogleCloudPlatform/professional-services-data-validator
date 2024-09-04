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


S1_TABLES = [
    ("s1", "t1"),
    ("s1", "t2"),
    ("s1", "t3"),
]


@pytest.fixture
def module_under_test():
    from data_validation import find_tables

    return find_tables


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
        # Test that asterisk mixed woth other characters is not expanded
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
