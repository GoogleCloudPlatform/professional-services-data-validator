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
from typing import Dict, List

from data_validation import consts, gcs_helper


class FileSystem(enum.Enum):
    LOCAL = 1
    GCS = 2


class StateManager(object):
    def __init__(self, file_system_root_path: str = None, verbose: bool = False):
        """Initialize a StateManager which handles connection configs locally
        and in GCS.

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
        gcs_helper.write_file(connection_path, json.dumps(config))

    def get_connection_config(self, name: str) -> Dict[str, str]:
        """Get a connection configuration from the expected file.

        Args:
            name: The name of the connection.
        Returns:
            A dict of the connection values from the file.
        """
        connection_path = self._get_connection_path(name)
        conn_str = gcs_helper.read_file(connection_path)

        return json.loads(conn_str)

    def list_connections(self) -> List[str]:
        """Returns a list of the connection names that exist."""
        file_names = self._list_directory(self._get_connections_directory())
        return [
            file_name.rsplit(".", 2)[0]
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

    def _list_directory(self, directory_path: str) -> List[str]:
        if self.file_system == FileSystem.GCS:
            return gcs_helper.list_gcs_directory(directory_path)
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

    def setup_gcs(self):
        try:
            gcs_helper.get_gcs_bucket(self.file_system_root_path)
        except ValueError as e:
            raise ValueError(
                "GCS Path Failure {} -> {}".format(self.file_system_root_path, e)
            )
