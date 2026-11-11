# Copyright 2026 Google LLC
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

"""System tests for Spanner databases using the PostgreSQL dialect.

The PostgreSQL dialect is not currently supported by DVT, these tests confirm
we fail early with a clear exception.
"""

import os
from unittest import mock

import pytest

from data_validation import cli_tools, clients, consts, exceptions
from tests.system.data_sources.common_functions import run_test_from_cli_args

PROJECT_ID = os.environ["PROJECT_ID"]
SPANNER_INSTANCE = os.getenv("SPANNER_INSTANCE", "span1")
SPANNER_PG_DATABASE = os.getenv("SPANNER_PG_DATABASE", "pso_data_validator_postgres")

SPANNER_PG_CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SPANNER,
    "project_id": PROJECT_ID,
    "instance_id": SPANNER_INSTANCE,
    "database_id": SPANNER_PG_DATABASE,
}

POSTGRESQL_DIALECT_ERROR = "uses the PostgreSQL dialect which is not supported"


def mock_get_connection_config(*args):
    if args[1] in ("spanner-pg-conn", "mock-conn"):
        return SPANNER_PG_CONN


def test_get_data_client_postgresql_dialect():
    """Connecting to a PostgreSQL dialect database raises a clear exception."""
    with pytest.raises(exceptions.DataClientConnectionFailure) as exc_info:
        _ = clients.get_data_client(SPANNER_PG_CONN)
    assert POSTGRESQL_DIALECT_ERROR in str(exc_info.value)
    assert SPANNER_PG_DATABASE in str(exc_info.value)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_postgresql_dialect():
    """A validation command against a PostgreSQL dialect database raises a clear exception."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=public.dvt_core_types",
        ]
    )
    with pytest.raises(exceptions.DataClientConnectionFailure) as exc_info:
        _ = run_test_from_cli_args(args)
    assert POSTGRESQL_DIALECT_ERROR in str(exc_info.value)
