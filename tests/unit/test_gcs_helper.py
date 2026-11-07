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

from types import SimpleNamespace
from unittest import mock

import pytest

from data_validation import gcs_helper


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


OBJECTS = ["dir/a.yaml", "dir/b.connection.json", "dir/sub/c.yaml", "other/d.yaml"]


def _fake_bucket():
    """Mimic Cloud Storage list_blobs(prefix, delimiter="/") semantics."""

    def list_blobs(prefix, delimiter):
        for name in OBJECTS:
            if name.startswith(prefix) and delimiter not in name[len(prefix) :]:
                yield SimpleNamespace(name=name)

    return SimpleNamespace(list_blobs=list_blobs)


@pytest.mark.parametrize("directory_path", ["gs://bucket/dir", "gs://bucket/dir/"])
def test_list_gcs_directory_with_or_without_trailing_slash(directory_path):
    with mock.patch.object(gcs_helper, "get_gcs_bucket", return_value=_fake_bucket()):
        assert gcs_helper.list_gcs_directory(directory_path) == [
            "a.yaml",
            "b.connection.json",
        ]


def test_list_gcs_directory_bucket_root():
    with mock.patch.object(gcs_helper, "get_gcs_bucket", return_value=_fake_bucket()):
        assert gcs_helper.list_gcs_directory("gs://bucket") == []
