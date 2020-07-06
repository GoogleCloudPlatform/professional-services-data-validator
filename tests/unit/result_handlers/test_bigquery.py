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

from unittest import mock

from google.cloud import bigquery
import pytest


@pytest.fixture
def module_under_test():
    from data_validation.result_handlers import bigquery

    return bigquery


def test_get_handler_for_project_sets_user_agent(module_under_test, monkeypatch):
    mock_client = mock.create_autospec(bigquery.Client)
    monkeypatch.setattr(bigquery, "Client", value=mock_client)
    module_under_test.BigQueryResultHandler.get_handler_for_project(
        "test-project", table_id="some_dataset.some_table"
    )
    mock_client.assert_called_once()
    user_agent = mock_client.call_args[1]["client_info"].to_user_agent()
    assert "google-pso-tool/data-validator" in user_agent
