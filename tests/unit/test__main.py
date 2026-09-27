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
import pytest

from data_validation import cli_tools, exceptions, config_manager, consts
from data_validation import __main__ as main

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
    "verbose": True,
}

VALIDATE_COLUMN_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "validate",
    "validate_cmd": "column",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_SOURCE_CONN: TEST_CONN,
    consts.CONFIG_TARGET_CONN: TEST_CONN,
    consts.CONFIG_FILE: None,
    consts.CONFIG_FILE_JSON: None,
}
BROKEN_VALIDATE_COLUMN_CONFIG_MISSING_COMMAND = {
    "verbose": False,
    "log_level": "INFO",
    "validate_cmd": "column",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_SOURCE_CONN: TEST_CONN,
    consts.CONFIG_TARGET_CONN: TEST_CONN,
    consts.CONFIG_FILE: None,
    consts.CONFIG_FILE_JSON: None,
}  # same as VALIDATE_COLUMN_CONFIG but without the command item
BROKEN_VALIDATE_COLUMN_CONFIG_INCORRECT_COMMAND = {
    "verbose": False,
    "log_level": "INFO",
    "command": "incorrectcommand",
    "validate_cmd": "column",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_SOURCE_CONN: TEST_CONN,
    consts.CONFIG_TARGET_CONN: TEST_CONN,
    consts.CONFIG_FILE: None,
    consts.CONFIG_FILE_JSON: None,
}  # same as VALIDATE_COLUMN_CONFIG but with the command item replaced
VALIDATE_ROW_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "validate",
    "validate_cmd": "row",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.ROW_VALIDATION,
    consts.CONFIG_SOURCE_CONN: TEST_CONN,
    consts.CONFIG_TARGET_CONN: TEST_CONN,
    consts.CONFIG_FILE: None,
    consts.CONFIG_FILE_JSON: None,
}  # same as VALIDATE_COLUMN_CONFIG but with 2 items replaced
VALIDATE_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "configs",
    "validation_config_cmd": "run",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.ROW_VALIDATION,
    consts.CONFIG_FILE: "test.yaml",
    "config_dir": None,
    "kube_completions": None,
}
VALIDATE_SCHEMA_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "validate",
    "validate_cmd": "schema",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.SCHEMA_VALIDATION,
    consts.CONFIG_SOURCE_CONN: TEST_CONN,
    consts.CONFIG_TARGET_CONN: TEST_CONN,
    consts.CONFIG_FILE: None,
    consts.CONFIG_FILE_JSON: None,
}
CONNECTION_LIST_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "list",
}
CONNECTION_ADD_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "add",
    "connect_type": consts.SOURCE_TYPE_BIGQUERY,
    consts.SECRET_MANAGER_TYPE: "gcp",
    consts.SECRET_MANAGER_PROJECT_ID: "dummy-gcp-project",
    consts.PROJECT_ID: "dummy-gcp-project",
    consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH: None,
    "connection_name": "dummy-bq-connection",
    consts.API_ENDPOINT: None,
    consts.STORAGE_API_ENDPOINT: None,
    consts.CLIENT_PROJECT_ID: None,
    consts.BILLING_PROJECT_ID: None,
}
CONNECTION_DESCRIBE_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "describe",
    "connection_name": "dummy-bq-connection",
    "output_format": "yaml",
}
CONNECTION_DELETE_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "delete",
    "connection_name": "dummy-bq-connection",
}
BROKEN_CONNECTION_CONFIG_INCORRECT_COMMAND = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "incorrectconnectioncommand",
    "connect_type": consts.SOURCE_TYPE_BIGQUERY,
    consts.SECRET_MANAGER_TYPE: "gcp",
    consts.SECRET_MANAGER_PROJECT_ID: "dummy-gcp-project",
    consts.PROJECT_ID: "dummy-gcp-project",
    consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH: None,
    "connection_name": "dummy-bq-connection",
    consts.API_ENDPOINT: None,
    consts.CLIENT_PROJECT_ID: None,
}  # same as CONNECTION_ADD_ARGS but with the command item replaced
FIND_TABLES_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "find-tables",
}
DEPLOY_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "deploy",
}
GENERATE_PARTITIONS_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "generate-table-partitions",
    "partition_num": 9,
    "parts_per_file": 5,
    "tables_list": "my_schema.my_table",
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
}
QUERY_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "query",
    "conn": "dummy-bq-connection",
    "query": "SELECT 1 AS TEST",
    "output_format": consts.FORMAT_TYPE_PYTHON,
}


class MockIbisClient(object):
    _source_type = consts.SOURCE_TYPE_BIGQUERY
    name = "bigquery"


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CLI_ARGS),
)
def test_configure_arg_parser(mock_args):
    """Test arg parser values."""
    args = cli_tools.get_parsed_args()
    file_path = cli_tools.get_arg_config_file(args)

    assert file_path == "example_test.yaml"


@mock.patch("data_validation.validation_runner.run_validation")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_COLUMN_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_COLUMN_CONFIG),
)
def test_successful_column_validation_with_mocked_run_validation(
    mock_args, mock_build, mock_run
):
    main.main()
    mock_args.assert_called_once()
    mock_build.assert_called_once()
    mock_run.assert_called_once()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**BROKEN_VALIDATE_COLUMN_CONFIG_MISSING_COMMAND),
)
def test_throws_for_malformed_input_config_missing_command(mock_args):
    with pytest.raises(Exception) as e_info:
        main.main()
    mock_args.assert_called_once()
    assert e_info.value.args[0] == "'Namespace' object has no attribute 'command'"


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**BROKEN_VALIDATE_COLUMN_CONFIG_INCORRECT_COMMAND),
)
def test_throws_for_malformed_input_config_incorrect_command(mock_args):
    with pytest.raises(ValueError) as e_info:
        main.main()
    mock_args.assert_called_once()
    assert (
        e_info.value.args[0]
        == "Positional Argument 'incorrectcommand' is not supported"
    )


@mock.patch("data_validation.validation_runner.run_validation")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_ROW_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_ROW_CONFIG),
)
def test_successful_row_validation_with_mocked_run_validation(
    mock_args, mock_build, mock_run
):
    main.main()
    mock_args.assert_called_once()
    mock_build.assert_called_once()
    mock_run.assert_called_once()


@mock.patch("data_validation.validation_runner.run_validation")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_CONFIG),
)
def test_successful_validation_config_with_mocked_run_validation(
    mock_args, mock_build, mock_run
):
    main.main()
    mock_args.assert_called_once()
    mock_build.assert_called_once()
    mock_run.assert_called_once()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_LIST_ARGS),
)
@mock.patch("data_validation.cli_tools.list_connections")
def test_successful_connection_list_with_mocked_list_connections(mock_run, mock_args):
    main.main()
    mock_args.assert_called_once()
    mock_run.assert_called_once()


@mock.patch("data_validation.clients.get_data_client")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_ADD_ARGS),
)
def test_successful_connection_add_with_mocked_list_connections(mock_args, mock_run):
    main.main()
    mock_args.assert_called_once()
    mock_run.assert_called_once()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_DESCRIBE_ARGS),
)
@mock.patch("data_validation.cli_tools.describe_connection")
def test_successful_connection_describe_with_mocked_describe_connection(
    mock_describe, mock_args
):
    main.main()
    mock_args.assert_called_once()
    mock_describe.assert_called_once()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_DELETE_ARGS),
)
@mock.patch("data_validation.cli_tools.delete_connection")
def test_successful_connection_delete_with_mocked_delete_connection(
    mock_run, mock_args
):
    main.main()
    mock_args.assert_called_once()
    mock_run.assert_called_once()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**BROKEN_CONNECTION_CONFIG_INCORRECT_COMMAND),
)
def test_throws_for_malformed_input_connection_config_incorrect_command(mock_args):
    with pytest.raises(ValueError) as e_info:
        main.main()
    mock_args.assert_called_once()
    assert (
        e_info.value.args[0]
        == "Connections Argument 'incorrectconnectioncommand' is not supported"
    )


@mock.patch("data_validation.__main__.find_tables_using_string_matching")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**FIND_TABLES_ARGS),
)
def test_successful_find_tables_with_mock(mock_args, mock_run):
    main.main()
    mock_args.assert_called_once()
    mock_run.assert_called_once()


@mock.patch("data_validation.validation_runner.run_validation")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_SCHEMA_CONFIG,
            MockIbisClient(),
            MockIbisClient(),
            verbose=False,
        )
    ],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_SCHEMA_CONFIG),
)
def test_successful_schema_validation_with_mocked_run_validation(
    mock_args, mock_build, mock_run
):
    """Test schema validation with mocked dependencies."""
    main.main()
    mock_args.assert_called_once()
    mock_build.assert_called_once()
    mock_run.assert_called_once()


@mock.patch("data_validation.clients.get_data_client")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**QUERY_CONFIG),
)
def test_successful_query_with_mocked_get_data_client(mock_args, mock_get_data_client):
    main.main()
    mock_args.assert_called_once()
    mock_get_data_client.assert_called_once()


@mock.patch("data_validation.__main__.PartitionBuilder")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            GENERATE_PARTITIONS_CONFIG,
            MockIbisClient(),
            MockIbisClient(),
            verbose=False,
        )
    ],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**GENERATE_PARTITIONS_CONFIG),
)
def test_successful_generate_partitions_with_mocked_partition_builder(
    mock_args, mock_build, mock_run
):
    main.main()
    mock_args.assert_called_once()
    mock_build.assert_called_once()
    mock_run.assert_called_once()


@mock.patch("data_validation.app.app.run")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**DEPLOY_ARGS),
)
def test_successful_deploy_with_mocked_app_run(mock_args, mock_run):
    main.main()
    mock_args.assert_called_once()
    mock_run.assert_called_once()


@mock.patch("data_validation.cli_tools.store_validation")
@mock.patch("data_validation.gcs_helper.list_gcs_directory", return_value=[])
@mock.patch("data_validation.gcs_helper._is_gcs_path", return_value=False)
@mock.patch("os.path.exists", return_value=True)
@mock.patch("os.listdir", return_value=[])
def test_store_config_dir_yaml_success(
    mock_listdir, mock_exists, mock_is_gcs, mock_list_gcs, mock_store_validation
):
    """Test storing validation configs inside a directory succeeds and handles naming collisions with suffixes."""
    config_mgr_1 = config_manager.ConfigManager(
        {"type": "Column", "schema_name": "s", "table_name": "t"},
        MockIbisClient(),
        MockIbisClient(),
        verbose=False,
    )
    config_mgr_2 = config_manager.ConfigManager(
        {"type": "Column", "schema_name": "s", "table_name": "t"},
        MockIbisClient(),
        MockIbisClient(),
        verbose=False,
    )

    args = argparse.Namespace(config_dir="my_dir", source_conn="src", target_conn="tgt")

    main.store_config_dir(args, [config_mgr_1, config_mgr_2], is_json=False)

    # Assert store_validation was called twice
    assert mock_store_validation.call_count == 2

    # First call: s_t.yaml
    call_1 = mock_store_validation.call_args_list[0]
    assert call_1.args[0] == "my_dir/s.t.yaml"

    # Second call: s_t_1.yaml (collision handled)
    call_2 = mock_store_validation.call_args_list[1]
    assert call_2.args[0] == "my_dir/s.t_1.yaml"


@mock.patch("os.path.exists", return_value=True)
@mock.patch("os.listdir", return_value=["existing_file.yaml"])
@mock.patch("data_validation.gcs_helper._is_gcs_path", return_value=False)
def test_store_config_dir_not_empty_raises(mock_is_gcs, mock_listdir, mock_exists):
    """Test storing validations in a non-empty directory raises ValueError to prevent accidental overrides."""
    config_mgr = config_manager.ConfigManager(
        {"type": "Column", "schema_name": "s", "table_name": "t"},
        MockIbisClient(),
        MockIbisClient(),
        verbose=False,
    )
    args = argparse.Namespace(config_dir="my_dir")

    with pytest.raises(ValueError) as exc_info:
        main.store_config_dir(args, [config_mgr], is_json=False)

    assert "is not empty. Aborting." in str(exc_info.value)


def test_store_config_dir_custom_query_raises():
    """Test that attempting to save custom-query validations to a config directory raises ValueError."""
    config_mgr = config_manager.ConfigManager(
        {"type": consts.CUSTOM_QUERY, "schema_name": "s", "table_name": "t"},
        MockIbisClient(),
        MockIbisClient(),
        verbose=False,
    )
    args = argparse.Namespace(config_dir="my_dir")

    with pytest.raises(ValueError) as exc_info:
        main.store_config_dir(args, [config_mgr], is_json=False)

    assert main.CUSTOM_QUERY_DIR_SUPPORT_ERROR in str(exc_info.value)


@mock.patch("data_validation.config_manager.ConfigManager.build_config_manager")
@mock.patch("data_validation.cli_tools.get_pre_build_configs")
def test_build_config_managers_from_args_raises_build_config_exception(
    mock_get_pre, mock_build_mgr
):
    mock_get_pre.return_value = [
        {consts.CONFIG_PRE_BUILD_TABLE_OBJ: {consts.CONFIG_TABLE_NAME: "test_table"}}
    ]
    mock_build_mgr.side_effect = ValueError("Invalid schema")

    args = argparse.Namespace()
    with pytest.raises(exceptions.BuildConfigException) as exc_info:
        main.build_config_managers_from_args(args, "column")

    assert "Validation failed for table 'test_table': Invalid schema" in str(
        exc_info.value
    )
    assert isinstance(exc_info.value.__cause__, ValueError)


@mock.patch("data_validation.config_manager.ConfigManager.build_config_manager")
@mock.patch("data_validation.cli_tools.get_pre_build_configs")
def test_build_config_managers_from_args_raises_build_config_exception_when_table_obj_missing(
    mock_get_pre, mock_build_mgr
):
    mock_get_pre.return_value = [{}]
    mock_build_mgr.side_effect = ValueError("Invalid schema")

    args = argparse.Namespace()
    with pytest.raises(exceptions.BuildConfigException) as exc_info:
        main.build_config_managers_from_args(args, "column")

    assert "Validation build failed: Invalid schema" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


@mock.patch("data_validation.config_manager.ConfigManager.build_config_manager")
@mock.patch("data_validation.cli_tools.get_pre_build_configs")
def test_build_config_managers_from_args_raises_build_config_exception_when_table_name_missing(
    mock_get_pre, mock_build_mgr
):
    mock_get_pre.return_value = [{consts.CONFIG_PRE_BUILD_TABLE_OBJ: {}}]
    mock_build_mgr.side_effect = ValueError("Invalid schema")

    args = argparse.Namespace()
    with pytest.raises(exceptions.BuildConfigException) as exc_info:
        main.build_config_managers_from_args(args, "column")

    assert "Validation build failed: Invalid schema" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)
