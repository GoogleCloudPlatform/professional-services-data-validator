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

from argparse import Namespace
import os
from typing import List
import ibis
from data_validation import cli_tools, clients, consts
from data_validation.config_manager import ConfigManager
from data_validation.query_builder.query_builder import QueryBuilder
from data_validation.query_builder.partition_row_builder import PartitionRowBuilder
from data_validation.validation_builder import ValidationBuilder

class PartitionBuilder:
    def __init__(self, config_managers, validation_type, args) -> None:

        self.config_managers = config_managers
        self.table_count = len(config_managers)
        self.validation_type = validation_type
        self.args = args
        self.config_dir = self._get_arg_config_dir()
        self.partition_type = self._get_arg_partition_type()
        
    def _get_arg_config_dir(self) -> str:
        """Return String yaml config folder path."""
        if not self.args.config_dir:
            raise ValueError("YAML Config Dir Path was not supplied.")

        return self.args.config_dir

    def _get_arg_partition_type(self) -> str:
        """Return the type of Partition Logic to be used from args

        Returns:
            Type of Partition logic to be used to split Config-Files
        """
        if not self.args.partition_type:  # Use default Partition logic if not supplied
            return consts.DEFAULT_PARTITION_TYPE
        elif self.args.partition_type not in consts.PARTITION_TYPES:
            # Already handled in argparser argument choices,
            # check kept for explicit calls
            raise ValueError(f"Unknown Partition Type: {self.args.partition_type}")

        return self.args.partition_type

    def _get_yaml_from_config(self, config_manager):
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

    def partition_configs(self):
        """Takes a list of ConfigManager object and splits each it into multiple
        ConfigManager objects applying supplied partition logic.

        Args:
            args (Namespace): User specified Arguments.
            config_managers (List[ConfigManager]): List of config manager instances.

        Returns:
            None
        """

        if self.partition_type == "primary_key":
            partition_filters = self._get_primary_key_partition_filters()
        elif self.partition_type == "primary_key_mod":
            # TODO: Add support for Primary_key + Mod
            raise ValueError(f"Partition Type: '{self.partition_type}' is not supported")
        elif self.partition_type == "hash_mod":
            # TODO: Add support for Hash + Mod
            raise ValueError(f"Partition Type: '{self.partition_type}' is not supported")

        self._add_partition_filters_and_store(partition_filters)


    def _get_primary_key_partition_filters(self) -> List[List[str]]:
        """Generate Partition filters for primary_key type partition for all
        Configs/Tables.

        Returns:
            A list of lists of partition filters for each table
        """

        master_filter_list = []

        # Filter for only first primary key (multi-pk filter not supported)
        primary_keys = cli_tools.get_arg_list(self.args.primary_keys)
        primary_key = primary_keys[0]

        for config_manager in self.config_managers:

            validation_builder = ValidationBuilder(config_manager)

            source_partition_row_builder = PartitionRowBuilder(
                primary_key,
                config_manager.source_client,
                config_manager.source_schema,
                config_manager.source_table,
                validation_builder.source_builder,
            )

            target_partition_row_builder = PartitionRowBuilder(
                primary_key,
                config_manager.target_client,
                config_manager.target_schema,
                config_manager.target_table,
                validation_builder.target_builder,
            )

            # accepted_datatypes = ("int64", "int32")
            # if (
            #     source_datatype not in accepted_datatypes
            #     and target_datatype not in accepted_datatypes
            # ):
            #     raise ValueError(
            #         "Expected primary_key for partition to be of type int."
            #         f"Got {source_df[source_df.columns[-1]].dtype.name}"
            #     )

            # Get Source and Target row Count
            source_count_query = source_partition_row_builder.get_count_query()
            source_count = source_count_query.execute()

            target_count_query = target_partition_row_builder.get_count_query()
            target_count = target_count_query.execute()

            row_count = max(source_count, target_count)

            # If supplied partition_num is greater than count(*) coalesce it
            if self.args.partition_num > row_count:
                partition_count = row_count
            else:
                partition_count = self.args.partition_num

            # Get Source and Target Primary key Min
            source_min_query = source_partition_row_builder.get_min_query()
            source_min = source_min_query.execute()

            target_min_query = target_partition_row_builder.get_min_query()
            target_min = target_min_query.execute()

            lower_bound = min(source_min, target_min)

            # Get Source and Target Primary key Max
            source_max_query = source_partition_row_builder.get_max_query()
            source_max = source_max_query.execute()

            target_max_query = target_partition_row_builder.get_max_query()
            target_max = target_max_query.execute()

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
                    f"{primary_key} >= {lower_val} and {primary_key} < {upper_val}"
                )
                filter_list.append(partition_filter)

                i += 1
                marker += partition_step

            master_filter_list.append(filter_list)

        return master_filter_list

    def _add_partition_filters_and_store(
        self,
        partition_filters: List[List[str]],
    ):
        """Add Partition Filters to ConfigManager and return a list of ConfigManager objects.

        Args:
            config_manager (List[ConfigManager]): List of Config manager instances.
            partition_filters (List[List[str]]): List of List of Partition filters
            for all Table/ConfigManager objects

        Returns:
            None
        """

        table_count = len(self.config_managers)
        for ind in range(table_count):
            config_manager = self.config_managers[ind]
            filter_list = partition_filters[ind]

            target_folder_name = config_manager.full_source_table
            target_folder_path = cli_tools.get_target_table_folder_path(
                self.config_dir, target_folder_name
            )

            for pos in range(len(filter_list)):
                filter_dict = {
                    "type": "custom",
                    "source": filter_list[pos],
                    "target": filter_list[pos],
                }
                # Append partition new filter
                config_manager.filters.append(filter_dict)

                # Save partition
                yaml_configs = self._get_yaml_from_config(config_manager)
                target_file_name = "0" * (4 - len(str(pos))) + str(pos) + ".yaml"
                config_file_path = os.path.join(target_folder_path, target_file_name)
                cli_tools.store_validation(config_file_path, yaml_configs)

                # Pop last partition filter
                config_manager.filters.pop()