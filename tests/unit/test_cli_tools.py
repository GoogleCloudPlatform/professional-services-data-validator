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
from unittest import mock

from data_validation import cli_tools


TEST_CONN = '{"source_type":"Example"}'
CLI_ARGS = {
    "command": "run",
    "type": "Column",
    "source_conn": TEST_CONN,
    "target_conn": TEST_CONN,
    "tables_list": '[{"schema_name":"my_schema","table_name":"my_table"}]',
    "sum": '["col_a","col_b"]',
    "count": '["col_a","col_b"]',
    "config_file": "example_test.yaml",
    "verbose": True,
}


@mock.patch(
    "argparse.ArgumentParser.parse_args", return_value=argparse.Namespace(**CLI_ARGS),
)
def test_get_parsed_args(mock_args):
    """Test arg parser values."""
    args = cli_tools.get_parsed_args()
    assert args.command == "run"
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
    args = parser.parse_args(
        [
            "connections",
            "add",
            "--connection-name",
            "test",
            "BigQuery",
            "--project-id",
            "example-project",
        ]
    )
    conn = cli_tools.get_connection_config_from_args(args)

    assert conn["project_id"] == "example-project"


def test_list_connections(capsys):
    connections = cli_tools.get_connections()
    cli_tools.list_connections()

    captured = capsys.readouterr()
    result = "".join(["Connection Name: %s\n" % c for c in connections])

    assert captured.out == result
