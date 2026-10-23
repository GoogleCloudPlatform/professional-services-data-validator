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

from unittest import mock

import pytest

from data_validation import exceptions, validation_runner


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
