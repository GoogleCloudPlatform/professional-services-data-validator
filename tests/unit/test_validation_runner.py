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

import json
from unittest import mock

import pytest

from data_validation import exceptions, validation_runner


@mock.patch("data_validation.validation_runner.util.print_output")
@mock.patch("data_validation.validation_runner.util.ibis_table_to_sql")
@mock.patch("data_validation.validation_runner.DataValidation")
def test_dry_run_uses_serialized_output(mock_validation, mock_sql, mock_print):
    validator = mock.Mock()
    mock_validation.return_value.__enter__.return_value = validator
    mock_sql.side_effect = ["SELECT 1", "SELECT 2"]
    manager = mock.Mock()
    manager.config = {}

    validation_runner.run_validation(manager, dry_run=True)

    mock_print.assert_called_once()
    assert json.loads(mock_print.call_args.args[0]) == {
        "source_query": "SELECT 1",
        "target_query": "SELECT 2",
    }
    validator.execute.assert_not_called()


@mock.patch("data_validation.validation_runner.DataValidation")
def test_run_validation_logs_run_id_with_config_file(mock_data_validation, caplog):
    import logging
    from data_validation import consts

    validator = mock.Mock()
    validator.run_metadata.run_id = "run-123"
    mock_data_validation.return_value.__enter__.return_value = validator
    manager = mock.Mock()
    manager.full_source_table = "test_schema.test_table"
    manager.config = {consts.CONFIG_FILE: "0001.yaml"}
    caplog.set_level(logging.INFO)
    validation_runner.run_validation(manager)
    assert (
        "Run id run-123 started for table test_schema.test_table (config file: 0001.yaml)"
        in caplog.messages
    )


@mock.patch("data_validation.validation_runner.DataValidation")
def test_run_validation_exception_handling(mock_data_validation):
    """Test that exceptions in run_validation are wrapped with table names."""
    mock_validator = mock.Mock()
    mock_validator.execute.side_effect = ValueError("Some execution error")
    mock_data_validation.return_value.__enter__.return_value = mock_validator

    mock_config_manager = mock.Mock()
    mock_config_manager.full_source_table = "test_schema.test_table"
    mock_config_manager.config = {}

    with pytest.raises(exceptions.ValidationException) as exc_info:
        validation_runner.run_validation(mock_config_manager)

    assert (
        "Validation failed for table 'test_schema.test_table': Some execution error"
        in str(exc_info.value)
    )
    assert isinstance(exc_info.value.__cause__, ValueError)
