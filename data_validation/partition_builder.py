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

import os
import logging
import numpy
from typing import List, Dict
from argparse import Namespace

from data_validation import cli_tools, consts
from data_validation.config_manager import ConfigManager
from data_validation.query_builder.partition_row_builder import PartitionRowBuilder
from data_validation.validation_builder import ValidationBuilder


class PartitionBuilder:
    def __init__(self, config_managers: List[ConfigManager], args: Namespace) -> None:

        self.config_managers = config_managers
        self.table_count = len(config_managers)
        self.args = args
        self.config_dir = self._get_arg_config_dir()
        self.primary_key = self._get_primary_key()
        self.partition_key = self._get_arg_partition_key()

    def _get_arg_config_dir(self) -> str:
        """Return String yaml config folder path."""
        if not self.args.config_dir:
            raise ValueError("YAML Config Dir Path was not supplied.")

        return self.args.config_dir

    def _get_primary_key(self) -> str:
        """Return the first Primary Key"""
        # Filter for only first primary key (multi-pk filter not supported)
        primary_keys = cli_tools.get_arg_list(self.args.primary_keys)
        primary_key = primary_keys[0]
        return primary_key

    def _get_arg_partition_key(self) -> str:
        """Return Partition Key. If not supplied, defaults to Primary Key"""
        if not self.args.partition_key:
            logging.warning(
                "Partition key cannot be found. Will default to Primary key"
            )
            return self.primary_key

        return self.args.partition_key

    def _get_yaml_from_config(self, config_manager: ConfigManager) -> Dict:
        """Return dict objects formatted for yaml validations.

        Args:
            config_managers (list[ConfigManager]): List of config manager instances.
        """
        yaml_config = {
            consts.YAML_SOURCE: self.args.source_conn,
            consts.YAML_TARGET: self.args.target_conn,
            consts.YAML_RESULT_HANDLER: config_manager.result_handler_config,
            consts.YAML_VALIDATIONS: [config_manager.get_yaml_validation_block()],
        }
        return yaml_config

    def partition_configs(self) -> None:
        """Takes a list of ConfigManager object and splits each it into multiple
        ConfigManager objects applying supplied partition logic.

        Returns:
            None
        """

        # Default partition logic: Partition key
        # Add necessary checks and routes when support for hashmod or partitionkeymod is added
        partition_filters = self._get_partition_key_filters()
        yaml_configs_list = self._add_partition_filters(partition_filters)
        self._store_partitions(yaml_configs_list)

    def _get_partition_key_filters(self) -> List[List[str]]:
        """Generate Partition filters for primary_key type partition for all
        Configs/Tables.

        Returns:
            A list of lists of partition filters for each table
        """

        master_filter_list = []

        for config_manager in self.config_managers:

            validation_builder = ValidationBuilder(config_manager)

            source_partition_row_builder = PartitionRowBuilder(
                self.partition_key,
                config_manager.source_client,
                config_manager.source_schema,
                config_manager.source_table,
                validation_builder.source_builder,
            )

            target_partition_row_builder = PartitionRowBuilder(
                self.partition_key,
                config_manager.target_client,
                config_manager.target_schema,
                config_manager.target_table,
                validation_builder.target_builder,
            )

            # Get Source and Target row Count
            source_count_query = source_partition_row_builder.get_count_query()
            source_count = source_count_query.execute()

            target_count_query = target_partition_row_builder.get_count_query()
            target_count = target_count_query.execute()

            if source_count != target_count:
                logging.warning(
                    "Source and Target table row counts do not match,"
                    "proceeding with max(source_count, target_count)"
                )
            row_count = max(source_count, target_count)

            # If supplied partition_num is greater than count(*) coalesce it
            if self.args.partition_num > row_count:
                partition_count = row_count
                logging.warning(
                    "Supplied partition num is greater than row count, "
                    "truncating it to row count"
                )
            else:
                partition_count = self.args.partition_num

            # Get Source and Target Primary key Min
            source_min_query = source_partition_row_builder.get_min_query()
            source_min = source_min_query.execute()

            target_min_query = target_partition_row_builder.get_min_query()
            target_min = target_min_query.execute()

            # If Primary key is non numeric, raise Type Error
            accepted_data_types = [int, numpy.int32, numpy.int64]
            if not (
                type(source_min) in accepted_data_types
                and type(target_min) in accepted_data_types
            ):
                raise TypeError(
                    f"Supplied Partition key is not of type Numeric: "
                    f"{self.partition_key}"
                )

            if source_min != target_min:
                logging.warning(
                    "min(partition_key) for Source and Target tables do not"
                    "match, proceeding with min(source_min, target_min)"
                )
            lower_bound = min(source_min, target_min)

            # Get Source and Target Primary key Max
            source_max_query = source_partition_row_builder.get_max_query()
            source_max = source_max_query.execute()

            target_max_query = target_partition_row_builder.get_max_query()
            target_max = target_max_query.execute()

            if source_min != target_min:
                logging.warning(
                    "max(partition_key) for Source and Target tables do not"
                    "match, proceeding with max(source_max, target_max)"
                )

            upper_bound = max(source_max, target_max)

            filter_list = []  # Store partition filters per config/table
            i = 0
            marker = lower_bound
            partition_step = (upper_bound - lower_bound) // partition_count
            while i < partition_count:
                lower_val = marker
                upper_val = marker + partition_step

                if i == partition_count - 1:
                    upper_val = upper_bound + 1

                partition_filter = (
                    f"{self.partition_key} >= {lower_val} "
                    f"and {self.partition_key} < {upper_val}"
                )
                filter_list.append(partition_filter)

                i += 1
                marker += partition_step

            master_filter_list.append(filter_list)

        return master_filter_list

    def _add_partition_filters(
        self,
        partition_filters: List[List[str]],
    ) -> List[Dict]:
        """Add Partition Filters to ConfigManager and return a list of dict
        ConfigManager objects.

        Args:
            partition_filters (List[List[str]]): List of List of Partition filters
            for all Table/ConfigManager objects

        Returns:
            yaml_configs_list (List[Dict]): List of YAML configs for all tables
        """

        table_count = len(self.config_managers)
        yaml_configs_list = [None] * table_count
        for ind in range(table_count):
            config_manager = self.config_managers[ind]
            filter_list = partition_filters[ind]

            yaml_configs_list[ind] = {
                "target_folder_name": config_manager.full_source_table,
                "partitions": [],
            }
            for pos in range(len(filter_list)):
                filter_dict = {
                    "type": "custom",
                    "source": filter_list[pos],
                    "target": filter_list[pos],
                }
                # Append partition new filter
                config_manager.filters.append(filter_dict)

                # Build and append partition YAML
                yaml_config = self._get_yaml_from_config(config_manager)
                target_file_name = "0" * (4 - len(str(pos))) + str(pos) + ".yaml"
                yaml_configs_list[ind]["partitions"].append(
                    {"target_file_name": target_file_name, "yaml_config": yaml_config}
                )

                # Pop last partition filter
                config_manager.filters.pop()

        return yaml_configs_list

    def _store_partitions(self, yaml_configs_list: List[Dict]) -> None:
        """Save Partitions to target folder

        Args:
            yaml_configs_list (List[Dict]): List of YAML configs for all tables

        Returns:
            None
        """
        logging.info(f"Writing table partition configs to directory: {self.config_dir}")

        for table in yaml_configs_list:
            target_folder_name = table["target_folder_name"]
            target_folder_path = os.path.join(self.config_dir, target_folder_name)
            for partition in table["partitions"]:
                yaml_config = partition["yaml_config"]
                target_file_name = partition["target_file_name"]
                target_file_path = os.path.join(target_folder_path, target_file_name)
                cli_tools.store_partition(
                    target_file_path, yaml_config, target_folder_path
                )

        logging.info(
            f"Success! Table partition configs written to directory: {self.config_dir}"
        )
