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

from data_validation import gcs_helper


@pytest.fixture(autouse=True)
def clear_storage_client_cache():
    gcs_helper._get_storage_client.cache_clear()
    yield
    gcs_helper._get_storage_client.cache_clear()


@mock.patch("data_validation.gcs_helper.storage.Client")
def test_get_gcs_bucket_caches_storage_client(mock_storage_client):
    """Verify storage.Client is created only once across multiple get_gcs_bucket calls."""
    bucket_a = gcs_helper.get_gcs_bucket("gs://bucket-a/path/to/file.yaml")
    bucket_b = gcs_helper.get_gcs_bucket("gs://bucket-b/other/file.yaml")

    assert mock_storage_client.call_count == 1
    client_instance = mock_storage_client.return_value
    client_instance.bucket.assert_any_call("bucket-a")
    client_instance.bucket.assert_any_call("bucket-b")
    assert bucket_a == client_instance.bucket.return_value
    assert bucket_b == client_instance.bucket.return_value


@mock.patch("data_validation.gcs_helper.storage.Client")
def test_get_gcs_bucket_wraps_value_error(mock_storage_client):
    """Verify ValueError raised by client.bucket() is wrapped with GCS Path Failure."""
    mock_storage_client.return_value.bucket.side_effect = ValueError("bad bucket")

    with pytest.raises(
        ValueError, match="GCS Path Failure gs://bad/path -> bad bucket"
    ):
        gcs_helper.get_gcs_bucket("gs://bad/path")


def test_get_gcs_file_path():
    result_path = gcs_helper._get_gcs_file_path(
        "gs://my-bucket/state/file/path/name.json"
    )
    assert result_path == "state/file/path/name.json"
