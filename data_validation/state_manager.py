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
""" The StateManager is a utility to help manage Data Validations long-lived
configurations and state. The majority of this work is file system management
of connections and validation files.
"""

import copy
import json
import logging
import os
from enum import Enum

from data_validation import consts
from data_validation import clients
from data_validation.result_handlers.bigquery import BigQueryResultHandler
from data_validation.result_handlers.text import TextResultHandler
from data_validation.validation_builder import ValidationBuilder


class FileSystem(Enum):
    LOCAL = 1
    GCS = 2


class StateManager(object):

    def __init__(self, file_system_root_path: str=None, verbose: bool=False):
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
        self._validate_path_requirements()

    def create_connection(self, name: str, config: dict[str]):
        """Create a connection file and store the given config as JSON.

        Args:
            name (String): The name of the connection.
            config (Dict): A dictionary with the connection details.
        """
        connection_path = self._get_connection_path(name)
        self._write_file(connection_path, json.dumps(config))

    def get_connection_config(self, name: str) -> dict[str]:
        """Get a connection configuration from the expected file.

        Args:
            name: The name of the connection.
        Returns:
            A dict of the connection values from the file.
        """
        connection_path = self._get_connection_path(name)
        conn_str = self._read_file(connection_path)

        return json.loads(conn_str)

    def list_connections(self) -> list[str]:
        """Returns a list of the connection names that exist."""
        file_names = self._list_directory(self._get_connections_directory())
        return [file_name.split(".")[0] for file_name in file_names if file_name.endswith(".connection.json")]

    def _get_connections_directory(self) -> str:
      """Returns the connections directory path."""
      return os.path.join(self.file_system_root_path, "connections")

    def _get_connection_path(self, name: str) -> str:
      """Returns the full path to a connection.

      Args:
          name: The name of the connection.
      """
      return os.path.join(self._get_connections_directory(), f"{name}.connection.json")

    def _read_file(self, file_path: str):
        if self.file_system == FileSystem.GCS:
            raise Exception("GCS File read not supported")
        else:
            return open(file_path, "r").read()

    def _write_file(self, file_path: str, data: str):
        if self.file_system == FileSystem.GCS:
            raise Exception("GCS File write not supported")
        else:
            with open(file_path, "w") as file:
                file.write(data)

    def _list_directory(self, directory_path: str) -> list[str]:
        if self.file_system == FileSystem.GCS:
            raise Exception("GCS File list not supported")
        else:
            return os.listdir(directory_path)

    def _get_file_system(self) -> FileSystem:
        if self.file_system_root_path.startswith("gs://"):
            return FileSystem.GCS
        else:
            return FileSystem.LOCAL

    def _validate_path_requirements(self):
        if self.file_system == FileSystem.GCS:
            # No current GCS validations are run
            pass
        else:
            if not os.path.exists(self._get_connections_directory()):
                os.makedirs(self._get_connections_directory())
