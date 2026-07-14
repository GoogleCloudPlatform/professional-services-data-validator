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

import pytest
from google.auth import credentials

from data_validation import clients, consts


@mock.patch("google.auth.default")
def test_bigquery_custom_endpoints(mock_default):
    mock_credentials = mock.create_autospec(credentials.Credentials)
    mock_default.return_value = (mock_credentials, "test-project")

    api_endpoint = "https://bigquery-custom.googleapis.com"
    storage_api_endpoint = "https://bigquerystorage-custom.googleapis.com"

    conn_config = {
        consts.SOURCE_TYPE: consts.SOURCE_TYPE_BIGQUERY,
        consts.PROJECT_ID: "test-project",
        consts.API_ENDPOINT: api_endpoint,
        consts.STORAGE_API_ENDPOINT: storage_api_endpoint,
    }

    ibis_client = clients.get_data_client(conn_config)

    assert ibis_client.client._connection.API_BASE_URL == api_endpoint
    assert ibis_client.storage_client.api_endpoint == storage_api_endpoint
