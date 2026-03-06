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
    "source_table_map,target_table_map,schema_map,expected",
    [
        # Test matching with a valid schema mapping.
        (
            {
                "prod.table1": {
                    consts.CONFIG_SCHEMA_NAME: "prod",
                    consts.CONFIG_TABLE_NAME: "table1",
                }
            },
            {
                "test.table1": {
                    consts.CONFIG_SCHEMA_NAME: "test",
                    consts.CONFIG_TABLE_NAME: "table1",
                }
            },
            {"prod": "test"},
            [
                {
                    "schema_name": "prod",
                    "table_name": "table1",
                    "target_schema_name": "test",
                    "target_table_name": "table1",
                }
            ],
        ),
        # Test that no match is found without a schema mapping.
        (
            {
                "prod.table1": {
                    consts.CONFIG_SCHEMA_NAME: "prod",
                    consts.CONFIG_TABLE_NAME: "table1",
                }
            },
            {
                "test.table1": {
                    consts.CONFIG_SCHEMA_NAME: "test",
                    consts.CONFIG_TABLE_NAME: "table1",
                }
            },
            {},
            [],
        ),
    ],
)
def test_compare_match_tables_with_mapping(
    module_under_test, source_table_map, target_table_map, schema_map, expected
):
    """Test matching tables from source and target with schema mapping."""
    table_configs = module_under_test._compare_match_tables(
        source_table_map, target_table_map, schema_map=schema_map
    )

    assert table_configs == expected


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
        # Test that arg format s1.*=s2.t1 is not expanded.
        (
            [
                {
                    "schema_name": "s1",
                    "table_name": "*",
                    "target_schema_name": "s2",
                    "target_table_name": "t1",
                },
            ],
            [
                {
                    "schema_name": "s1",
                    "table_name": "*",
                    "target_schema_name": "s2",
                    "target_table_name": "t1",
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


@pytest.mark.parametrize(
    ("table_list,expected_result"),
    (
        (
            # Test upper case names are coerced to lower case when possible.
            [
                ("own", "tab1"),
                # This table will remain upper case because there's already a lower case one.
                ("own", "TAB1"),
                # The key for this will be successfully lower cased.
                ("own", "TAB3"),
                # This table will remain upper case because there's already a lower case one, even though it is later in the list.
                ("own", "TAB4"),
                ("own", "tab4"),
            ],
            {
                "own.tab1": {
                    "schema_name": "own",
                    "table_name": "tab1",
                },
                "own.TAB1": {
                    "schema_name": "own",
                    "table_name": "TAB1",
                },
                "own.tab3": {
                    "schema_name": "own",
                    "table_name": "TAB3",
                },
                "own.tab4": {
                    "schema_name": "own",
                    "table_name": "tab4",
                },
                "own.TAB4": {
                    "schema_name": "own",
                    "table_name": "TAB4",
                },
            },
        ),
    ),
)
def test__get_table_map_from_obj_list_mixed_case(
    module_under_test, table_list: list, expected_result: dict
):
    """Test matching tables from source and target."""
    table_configs = module_under_test._get_table_map_from_obj_list(table_list)

    assert table_configs == expected_result


@mock.patch("data_validation.find_tables.get_mapped_table_configs")
@mock.patch("data_validation.clients.get_data_client")
@mock.patch("data_validation.state_manager.StateManager.get_connection_config")
def test_find_tables_using_string_matching_parsing(
    mock_get_conn_config, mock_get_client, mock_get_configs, module_under_test
):
    """Test parsing logic in find_tables_using_string_matching."""
    mock_args = mock.MagicMock()
    mock_args.source_conn = "source"
    mock_args.target_conn = "target"
    mock_args.allowed_schemas = "s1,s2=t2"
    mock_args.include_views = False
    mock_args.score_cutoff = 0.8

    mock_get_conn_config.return_value = {}
    mock_get_client.return_value = mock.MagicMock()
    mock_get_configs.return_value = []

    module_under_test.find_tables_using_string_matching(mock_args)

    mock_get_configs.assert_called_once_with(
        mock.ANY,
        mock.ANY,
        allowed_schemas=["s1", "s2"],
        include_views=False,
        score_cutoff=0.8,
        schema_map={"s2": "t2"},
    )
