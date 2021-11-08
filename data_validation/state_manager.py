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
"""A utility to manage Data Validations long-lived configurations and state.

The majority of this work is file system management of connections
and validation files.
"""

import enum
import json
import os
from google.cloud import storage
from typing import Dict, List
from yaml import dump, load, Dumper, Loader

from data_validation import client_info
from data_validation import consts


class FileSystem(enum.Enum):
    LOCAL = 1
    GCS = 2


class StateManager(object):
    def __init__(self, file_system_root_path: str = None, verbose: bool = False):
        """Initialize a StateManager which handles configuration
        and state management files.

        Args:
            file_system_root_path (String): A root file system path
                eg. "gs://bucket/data-validation/" or "/path/to/files/"
        """
        raw_dir_path = (
            file_system_root_path
            or os.environ.get(consts.ENV_DIRECTORY_VAR)
            or consts.DEFAULT_ENV_DIRECTORY
        )
        self.file_system_root_path = os.path.expanduser(raw_dir_path)
        self.file_system = self._get_file_system()
        self.verbose = verbose
        self.setup()

    def create_connection(self, name: str, config: Dict[str, str]):
        """Create a connection file and store the given config as JSON.

        Args:
            name (String): The name of the connection.
            config (Dict): A dictionary with the connection details.
        """
        connection_path = self._get_connection_path(name)
        self._write_file(connection_path, json.dumps(config))

    def get_connection_config(self, name: str) -> Dict[str, str]:
        """Get a connection configuration from the expected file.

        Args:
            name: The name of the connection.
        Returns:
            A dict of the connection values from the file.
        """
        connection_path = self._get_connection_path(name)
        conn_str = self._read_file(connection_path)

        return json.loads(conn_str)

    def list_connections(self) -> List[str]:
        """Returns a list of the connection names that exist."""
        file_names = self._list_directory(self._get_connections_directory())
        return [
            file_name.split(".")[0]
            for file_name in file_names
            if file_name.endswith(".connection.json")
        ]

    def _get_connections_directory(self) -> str:
        """Returns the connections directory path."""
        if self.file_system == FileSystem.LOCAL:
            return self.file_system_root_path

        return os.path.join(self.file_system_root_path, "connections/")

    def _get_connection_path(self, name: str) -> str:
        """Returns the full path to a connection.

        Args:
            name: The name of the connection.
        """
        return os.path.join(
            self._get_connections_directory(), f"{name}.connection.json"
        )

    def create_validation_yaml(self, name: str, yaml_config: Dict[str, str]):
        """Create a validation file and store the given config as YAML.

        Args:
            name (String): The name of the validation.
            yaml_config (Dict): A dictionary with the validation details.
        """
        validation_path = self._get_validation_path(name)
        yaml_config_str = dump(yaml_config, Dumper=Dumper)
        self._write_file(validation_path, yaml_config_str)

    def get_validation_config(self, name: str) -> Dict[str, str]:
        """Get a validation configuration from the expected file.

        Args:
            name: The name of the validation.
        Returns:
            A dict of the validation values from the file.
        """
        validation_path = self._get_validation_path(name)
        validation_bytes = self._read_file(validation_path)
        return load(validation_bytes, Loader=Loader)

    def list_validations(self):
        file_names = self._list_directory(self._get_validations_directory())
        return [
            file_name.split(".")[0]
            for file_name in file_names
            if file_name.endswith(".yaml")
        ]

    def _get_validations_directory(self):
        """Returns the validations directory path."""
        if self.file_system == FileSystem.LOCAL:
            # Validation configs should be written to tool root dir, not consts.DEFAULT_ENV_DIRECTORY as connections are
            return "./"
        return os.path.join(self.file_system_root_path, "validations/")

    def _get_validation_path(self, name: str) -> str:
        """Returns the full path to a validation.

        Args:
            name: The name of the validation.
        """
        return os.path.join(self._get_validations_directory(), f"{name}")

    def _read_file(self, file_path: str) -> str:
        if self.file_system == FileSystem.GCS:
            return self._read_gcs_file(file_path)
        else:
            return open(file_path, "r").read()

    def _write_file(self, file_path: str, data: str):
        if self.file_system == FileSystem.GCS:
            self._write_gcs_file(file_path, data)
        else:
            with open(file_path, "w") as file:
                file.write(data)

        print("Success! Config output written to {}".format(file_path))

    def _list_directory(self, directory_path: str) -> List[str]:
        if self.file_system == FileSystem.GCS:
            return self._list_gcs_directory(directory_path)
        else:
            return os.listdir(directory_path)

    def _get_file_system(self) -> FileSystem:
        if self.file_system_root_path.startswith("gs://"):
            return FileSystem.GCS
        else:
            return FileSystem.LOCAL

    def setup(self):
        if self.file_system == FileSystem.GCS:
            self.setup_gcs()
        else:
            if not os.path.exists(self._get_connections_directory()):
                os.makedirs(self._get_connections_directory())

    # GCS File Management Section
    def setup_gcs(self):
        info = client_info.get_http_client_info()
        self.storage_client = storage.Client(client_info=info)
        try:
            self.gcs_bucket = self._get_gcs_bucket()
        except ValueError as e:
            raise ValueError(
                "GCS Path Failure {} -> {}".format(self.file_system_root_path, e)
            )

    def _get_gcs_bucket(self):
        bucket_name = self.file_system_root_path[5:].split("/")[0]
        return self.storage_client.bucket(bucket_name)

    def _get_gcs_file_path(self, gcs_file_path: str):
        return str.join("", gcs_file_path[5:].split("/", 1)[1:])

    def _read_gcs_file(self, file_path: str) -> str:
        gcs_file_path = self._get_gcs_file_path(file_path)
        blob = self.gcs_bucket.get_blob(gcs_file_path)

        return blob.download_as_bytes()

    def _write_gcs_file(self, file_path: str, data: str):
        gcs_file_path = self._get_gcs_file_path(file_path)
        blob = self.gcs_bucket.blob(gcs_file_path)
        blob.upload_from_string(data)

    def _list_gcs_directory(self, directory_path: str) -> List[str]:
        gcs_prefix = self._get_gcs_file_path(directory_path)
        blobs = [
            f.name.replace(gcs_prefix, "")
            for f in self.gcs_bucket.list_blobs(prefix=gcs_prefix, delimiter="/")
            if f.name.replace(gcs_prefix, "")
        ]

        return blobs
