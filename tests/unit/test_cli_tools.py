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

import argparse
import logging
import pytest
from unittest import mock

from data_validation import cli_tools, consts


TEST_CONN = '{"source_type":"Example"}'
CLI_ARGS = {
    "command": "validate",
    "validate_cmd": "column",
    "source_conn": TEST_CONN,
    "target_conn": TEST_CONN,
    "tables_list": "my_schema.my_table",
    "sum": "col_a,col_b",
    "count": "col_a,col_b",
    "config_file": "example_test.yaml",
    "labels": "name=test_run",
    "threshold": 30.0,
    "verbose": True,
}

CLI_ADD_CONNECTION_ARGS = [
    "connections",
    "add",
    "--connection-name",
    "test",
    "BigQuery",
    "--project-id",
    "example-project",
]

CLI_ADD_CONNECTION_BAD_ARGS = [
    "connections",
    "add",
    "--bad-name",
    "test",
    "BigQuery",
]

CLI_ADD_ORACLE_STD_CONNECTION_ARGS = [
    "connections",
    "add",
    "--connection-name",
    "ora_std_test",
    "Oracle",
    "--password=p",
    "--host=localhost",
    "--port=1521",
    "--user=u",
    "--database=d",
]

CLI_ADD_ORACLE_WALLET_CONNECTION_ARGS = [
    "connections",
    "add",
    "--connection-name",
    "ora_wal_test",
    "Oracle",
    "--url=oracle+cx_oracle://@dvt_user_db",
]

TEST_VALIDATION_CONFIG = {
    "source": "example",
    "target": "example",
    "result_handler": {},
    "validations": [
        {
            "type": "Column",
            "table_name": "citibike_trips",
            "schema_name": "bigquery-public-data.new_york_citibike",
            "target_schema_name": "bigquery-public-data.new_york_citibike",
            "target_table_name": "citibike_trips",
            "labels": [],
            "threshold": 0.0,
            "format": "table",
            "filters": [],
            "aggregates": [
                {
                    "source_column": None,
                    "target_column": None,
                    "field_alias": "count",
                    "type": "count",
                }
            ],
        }
    ],
}

WRITE_SUCCESS_STRING = "Success! Config output written to"

CLI_FIND_TABLES_ARGS = [
    "find-tables",
    "--source-conn",
    TEST_CONN,
    "--target-conn",
    TEST_CONN,
    "--allowed-schemas",
    "my_schema",
]


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CLI_ARGS),
)
def test_get_parsed_args(mock_args):
    """Test arg parser values with validate command."""
    args = cli_tools.get_parsed_args()
    assert args.command == "validate"
    assert args.labels == "name=test_run"
    assert args.threshold == 30.0
    assert args.verbose


def test_configure_arg_parser_list_connections():
    """Test configuring arg parse in different ways."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(["connections", "list"])

    assert args.command == "connections"
    assert args.connect_cmd == "list"


def test_get_connection_config_from_args():
    """Test configuring arg parse in different ways."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_ADD_CONNECTION_ARGS)
    conn = cli_tools.get_connection_config_from_args(args)

    assert conn["project_id"] == "example-project"


def test_create_and_list_connections(caplog, fs):
    caplog.set_level(logging.INFO)
    # Create Connection
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_ADD_CONNECTION_ARGS)
    conn = cli_tools.get_connection_config_from_args(args)
    cli_tools.store_connection(args.connection_name, conn)

    assert WRITE_SUCCESS_STRING in caplog.records[0].msg

    # List Connection
    cli_tools.list_connections()
    assert "Connection Name: test : BigQuery" in caplog.records[1].msg


def test_bad_add_connection():
    with pytest.raises(SystemExit):
        parser = cli_tools.configure_arg_parser()
        _ = parser.parse_args(CLI_ADD_CONNECTION_BAD_ARGS)


@mock.patch(
    "data_validation.gcs_helper.write_file",
)
def test_create_connections_oracle(mock_write_file):
    # Create standard connection
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_ADD_ORACLE_STD_CONNECTION_ARGS)
    conn = cli_tools.get_connection_config_from_args(args)
    assert "url" not in conn
    cli_tools.store_connection(args.connection_name, conn)

    # Create wallet based connection
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_ADD_ORACLE_WALLET_CONNECTION_ARGS)
    conn = cli_tools.get_connection_config_from_args(args)
    assert "url" in conn
    cli_tools.store_connection(args.connection_name, conn)


def test_configure_arg_parser_list_and_run_validation_configs():
    """Test configuring arg parse in different ways."""
    parser = cli_tools.configure_arg_parser()

    args = parser.parse_args(["configs", "list"])
    assert args.command == "configs"
    assert args.validation_config_cmd == "list"

    args = parser.parse_args(["configs", "run"])
    assert args.command == "configs"
    assert args.validation_config_cmd == "run"


def test_create_and_list_and_get_validations(caplog, fs):
    caplog.set_level(logging.INFO)
    # Create validation config file
    cli_tools.store_validation("example_validation.yaml", TEST_VALIDATION_CONFIG)
    assert WRITE_SUCCESS_STRING in caplog.records[0].msg

    # List validation configs
    cli_tools.print_validations_in_dir()
    assert "Validation YAMLs found:" in caplog.records[2].msg

    # Retrieve the stored validation config
    yaml_config = cli_tools.get_validation("example_validation.yaml")
    assert yaml_config == TEST_VALIDATION_CONFIG


def test_find_tables_config():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_FIND_TABLES_ARGS)

    allowed_schemas = cli_tools.get_arg_list(args.allowed_schemas)
    assert allowed_schemas[0] == "my_schema"


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("key=value", [("key", "value")]),
        ("key1=value1,key2=value2", [("key1", "value1"), ("key2", "value2")]),
        (
            "key='longer value',key1='hyphen-value'",
            [("key", "'longer value'"), ("key1", "'hyphen-value'")],
        ),
        ("name=", [("name", "")]),
    ],
)
def test_get_labels(test_input, expected):
    """Test get labels."""
    res = cli_tools.get_labels(test_input)
    assert res == expected


@pytest.mark.parametrize(
    "test_input",
    [
        ("key==value"),
        ("key1=value1,badkey"),
        ("key"),
        (","),
        ("key=value,key"),
        ("key:value"),
    ],
)
def test_get_labels_err(test_input):
    """Ensure that Value Error is raised when incorrect label argument is provided."""
    with pytest.raises(ValueError):
        cli_tools.get_labels(test_input)


@pytest.mark.parametrize(
    "test_input,expected",
    [(0, 0.0), (50, 50.0), (100, 100.0)],
)
def test_threshold_float(test_input, expected):
    """Test threshold float function."""
    res = cli_tools.threshold_float(test_input)
    assert res == expected


@pytest.mark.parametrize(
    "test_input",
    [(-4), (float("nan")), (float("inf")), ("string")],
)
def test_threshold_float_err(test_input):
    """Test that threshold float only accepts positive floats."""
    with pytest.raises(argparse.ArgumentTypeError):
        cli_tools.threshold_float(test_input)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "src.schema.src_table=targ.schema.targ_table",
            [
                {
                    "schema_name": "src.schema",
                    "table_name": "src_table",
                    "target_schema_name": "targ.schema",
                    "target_table_name": "targ_table",
                }
            ],
        ),
        (
            'src_schema."odd.table"=targ_schema.targ_table',
            [
                {
                    "schema_name": "src_schema",
                    "table_name": "odd.table",
                    "target_schema_name": "targ_schema",
                    "target_table_name": "targ_table",
                }
            ],
        ),
        (
            "src_schema.src_table = targ_schema. targ_table",
            [
                {
                    "schema_name": "src_schema",
                    "table_name": "src_table",
                    "target_schema_name": "targ_schema",
                    "target_table_name": "targ_table",
                }
            ],
        ),
        (
            "src_schema.src_table",
            [{"schema_name": "src_schema", "table_name": "src_table"}],
        ),
        (
            "src_schema.src_table = targ_table",
            [
                {
                    "schema_name": "src_schema",
                    "table_name": "src_table",
                    "target_table_name": "targ_table",
                }
            ],
        ),
        (
            "src.schema.src_table = targ.schema.targ_table",
            [
                {
                    "schema_name": "src.schema",
                    "table_name": "src_table",
                    "target_schema_name": "targ.schema",
                    "target_table_name": "targ_table",
                }
            ],
        ),
        (
            'src.schema."src.table"',
            [{"schema_name": "src.schema", "table_name": "src.table"}],
        ),
        (
            '[{"schema_name": "src", "table_name": "table", "target_schema_name": "trg", "target_table_name": "trg_table"}]',
            [
                {
                    "schema_name": "src",
                    "table_name": "table",
                    "target_schema_name": "trg",
                    "target_table_name": "trg_table",
                }
            ],
        ),
    ],
)
def test_get_tables_list(test_input, expected):
    """Test get tables list."""
    res = cli_tools.get_tables_list(test_input)
    assert res == expected


@pytest.mark.parametrize(
    "test_input",
    [
        ("schema.table=targ_schema.targ_table=extra_schema"),
        ("no_schema=target_table"),
        ("schema,table=target_table"),
    ],
)
def test_get_tables_list_err(test_input):
    """Test get tables list errors correclty."""
    with pytest.raises(ValueError):
        cli_tools.get_tables_list(test_input)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("col_a,col_b", ["col_a", "col_b"]),
        ("col_a", ["col_a"]),
        ("col a,col b", ["col a", "col b"]),
    ],
)
def test_get_arg_list(test_input, expected):
    """Test get aggregations list of columns."""
    res = cli_tools.get_arg_list(test_input)
    assert res == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "project.dataset.table",
            {"type": "BigQuery", "project_id": "project", "table_id": "dataset.table"},
        ),
        (
            "project.data.data.table",
            {
                "type": "BigQuery",
                "project_id": "project",
                "table_id": "data.data.table",
            },
        ),
    ],
)
def test_get_result_handler(test_input, expected):
    """Test get result handler config dictionary."""
    res = cli_tools.get_result_handler(test_input)
    assert res == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "id < 5:row_id <5",
            [{"type": "custom", "source": "id < 5", "target": "row_id <5"}],
        ),
        ("id < 5", [{"type": "custom", "source": "id < 5", "target": "id < 5"}]),
        (
            "name != 'John'",
            [
                {
                    "type": "custom",
                    "source": "name != 'John'",
                    "target": "name != 'John'",
                }
            ],
        ),
        (
            "name != 'St. John''s'",
            [
                {
                    "type": "custom",
                    "source": "name != 'St. John''s'",
                    "target": "name != 'St. John''s'",
                }
            ],
        ),
        (
            "mod_timestamp >= '2024-04-01 16:00:00 UTC':mod_timestamp >= '2020-04-01 16:00:00 UTC'",
            [
                {
                    "type": "custom",
                    "source": "mod_timestamp >= '2024-04-01 16:00:00 UTC'",
                    "target": "mod_timestamp >= '2020-04-01 16:00:00 UTC'",
                }
            ],
        ),
    ],
)
def test_get_filters(test_input, expected):
    """Test get filters."""
    res = cli_tools.get_filters(test_input)
    assert res == expected


@pytest.mark.parametrize(
    "test_input",
    [(""), ("source:"), ("invalid:filter:count")],
)
def test_get_filters_err(test_input):
    """Test get filters function returns error."""
    with pytest.raises(argparse.ArgumentTypeError):
        cli_tools.get_filters(test_input)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (["schema.table"], ("schema", "table")),
        (["full.schema.table"], ("full.schema", "table")),
        (['full.schema."table.name"'], ("full.schema", "table.name")),
    ],
)
def test_split_table(test_input, expected):
    """Test split table into schema and table name."""
    res = cli_tools.split_table(test_input)
    assert res == expected


def test_split_table_no_schema():
    """Test split table not requiring schema."""
    expected = (None, "target_table")
    res = cli_tools.split_table(["target_table"], schema_required=False)
    assert res == expected


@pytest.mark.parametrize(
    "test_input",
    [(["table"])],
)
def test_split_table_err(
    test_input,
):
    """Test split table throws the right errors."""
    with pytest.raises(ValueError):
        cli_tools.split_table(test_input)


@pytest.mark.parametrize(
    "list_of_cols,max_col_count,pre_build_config,expected_result",
    [
        (
            [str(_) for _ in range(5)],
            6,
            {"a": "a", consts.CONFIG_ROW_HASH: "*", consts.CONFIG_ROW_CONCAT: None},
            # Fewer columns than threshold therefore no change.
            None,
        ),
        (
            [str(_) for _ in range(5)],
            6,
            {"a": "a", consts.CONFIG_ROW_HASH: None, consts.CONFIG_ROW_CONCAT: "*"},
            # Fewer columns than threshold therefore no change.
            None,
        ),
        (
            [str(_) for _ in range(5)],
            3,
            {"a": "a", consts.CONFIG_ROW_HASH: "*", consts.CONFIG_ROW_CONCAT: None},
            # 5 columns with max of 3 should give us 2 configs of CONFIG_ROW_HASH.
            [
                {
                    "a": "a",
                    consts.CONFIG_ROW_HASH: "0,1,2",
                    consts.CONFIG_ROW_CONCAT: None,
                },
                {
                    "a": "a",
                    consts.CONFIG_ROW_HASH: "3,4",
                    consts.CONFIG_ROW_CONCAT: None,
                },
            ],
        ),
        (
            [str(_) for _ in range(5)],
            3,
            {"a": "a", consts.CONFIG_ROW_HASH: None, consts.CONFIG_ROW_CONCAT: "*"},
            # 5 columns with max of 3 should give us 2 configs of CONFIG_ROW_CONCAT.
            [
                {
                    "a": "a",
                    consts.CONFIG_ROW_HASH: None,
                    consts.CONFIG_ROW_CONCAT: "0,1,2",
                },
                {
                    "a": "a",
                    consts.CONFIG_ROW_HASH: None,
                    consts.CONFIG_ROW_CONCAT: "3,4",
                },
            ],
        ),
    ],
)
def test_concat_column_count_configs(
    list_of_cols: list,
    max_col_count: int,
    pre_build_config: dict,
    expected_result: list,
):
    arg_name = (
        consts.CONFIG_ROW_HASH
        if pre_build_config[consts.CONFIG_ROW_HASH]
        else consts.CONFIG_ROW_CONCAT
    )
    result = cli_tools._concat_column_count_configs(
        list_of_cols,
        pre_build_config,
        arg_name,
        max_col_count,
    )
    if expected_result:
        assert result == expected_result
    else:
        assert result == [pre_build_config]


@pytest.mark.parametrize(
    "test_input",
    ["tests/resources/custom-query.sql"],
)
def test_get_query_from_file(test_input: str):
    query = cli_tools.get_query_from_file(test_input)
    assert query == "SELECT * FROM bigquery-public-data.usa_names.usa_1910_2013"


@pytest.mark.parametrize(
    "test_input,expect_exception",
    [
        (" 1; ", False),
        (" SELECT * FROM bigquery-public-data.usa_names.usa_1910_2013; ", False),
        (" ", True),
    ],
)
def test_get_query_from_inline(test_input: str, expect_exception: bool):
    # Assert query format
    if expect_exception:
        # Assert exception.
        with pytest.raises(ValueError):
            query = cli_tools.get_query_from_inline(test_input)
    else:
        query = cli_tools.get_query_from_inline(test_input)
        assert query in test_input
