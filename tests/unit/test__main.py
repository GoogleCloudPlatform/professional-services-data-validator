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
import pytest
from unittest import mock

from data_validation import __main__ as main


TEST_CONN = '{"source_type":"Example"}'
CLI_ARGS = {
    "command": "store",
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
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CLI_ARGS),
)
def test_configure_arg_parser(mock_args):
    """Test arg parser values."""
    args = main.configure_arg_parser()
    assert args.command == "store"
    assert args.verbose
