# Copyright 2024 Google LLC
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

import logging
import os
from typing import List

from google.cloud import storage
from data_validation import client_info


def _is_gcs_path(file_path: str) -> bool:
    return True if file_path.startswith("gs://") else False


def get_validation_path(name: str) -> str:
    """Returns the full path to a validation."""
    if _is_gcs_path(name):
        return name
    return os.path.join("./", name)


def get_gcs_bucket(gcs_file_path: str) -> storage.Bucket:
    """Returns storage.Bucket given GCS file path with prefix."""
    bucket_name = gcs_file_path[5:].split("/")[0]
    info = client_info.get_http_client_info()
    storage_client = storage.Client(client_info=info)
    try:
        return storage_client.bucket(bucket_name)
    except ValueError as e:
        raise ValueError("GCS Path Failure {} -> {}".format(gcs_file_path, e))


def _get_gcs_file_path(gcs_file_path: str) -> str:
    """
    Returns relative object file path i.e. `path/to/file.yaml`
    given full GCS file path with prefix.
    """
    return "".join(gcs_file_path[5:].split("/", 1)[1:])


def _read_gcs_file(file_path: str, download_as_text: bool = False):
    gcs_bucket = get_gcs_bucket(file_path)
    blob = gcs_bucket.blob(_get_gcs_file_path(file_path))
    if not blob:
        raise ValueError(f"Invalid Cloud Storage Path: {file_path}")
    return blob.download_as_text() if download_as_text else blob.download_as_bytes()


def _write_gcs_file(file_path: str, data: str):
    gcs_bucket = get_gcs_bucket(file_path)
    blob = gcs_bucket.blob(_get_gcs_file_path(file_path))
    blob.upload_from_string(data)


def read_file(file_path: str, download_as_text: bool = False):
    if _is_gcs_path(file_path):
        return _read_gcs_file(file_path, download_as_text)
    else:
        with open(file_path, "r") as f:
            return f.read()


def write_file(file_path: str, data: str, include_log: bool = True):
    if _is_gcs_path(file_path):
        _write_gcs_file(file_path, data)
    else:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            file.write(data)

    if include_log:
        logging.info("Success! Config output written to {}".format(file_path))


def list_gcs_directory(directory_path: str) -> List[str]:
    gcs_prefix = _get_gcs_file_path(directory_path)
    gcs_bucket = get_gcs_bucket(directory_path)
    blobs = [
        f.name.replace(gcs_prefix, "")
        for f in gcs_bucket.list_blobs(prefix=gcs_prefix, delimiter="/")
    ]
    return blobs
