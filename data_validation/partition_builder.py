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

import os, ibis
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

    @staticmethod
    def _leq_value(keys, values) -> str:
        """ Takes the list of primary keys in keys and the value of the biggest element
        and generates an expression which can be used in a where clause to filter all rows with primary keys equal or 
        smaller than the biggest element.

        Returns:
            String containing the expression - e.g. (birth_month < 5 OR (birth_month = 5 AND (birth_day <= 2)))
        """
        value0 = "'" + values[0] + "'" if type(values[0]) == type("A") else str(values[0])
        if len(keys) == 1:
            return keys[0] + " <= " + value0
        else:
            return (
                keys[0]
                + " < "
                + value0
                + " OR "
                + keys[0]
                + " = "
                + value0
                + " AND ("
                + PartitionBuilder._leq_value(keys[1:], values[1:])
                + ")"
            )

    @staticmethod
    def _geq_value(keys, values) -> str:
        """ Takes the list of primary keys in keys and the value of the smallest element
        and generates an expression which can be used in a where clause to filter all rows with primary keys equal or 
        bigger than the smallest element.

        Returns:
            String containing the expression - e.g. (birth_month > 5 OR (birth_month = 5 AND (birth_day >= 2)))
        """
        value0 = "'" + values[0] + "'" if type(values[0]) == type("A") else str(values[0])
        if len(keys) == 1:
            return keys[0] + " >= " + value0
        else:
            return (
                keys[0]
                + " > "
                + value0
                + " OR "
                + keys[0]
                + " = "
                + value0
                + " AND ("
                + PartitionBuilder._geq_value(keys[1:], values[1:])
                + ")"
        )
    
    def _get_partition_key_filters(self) -> List[List[str]]:
        """Generate where clauses for each partition for each table pair. We are partitioning the tables based on keys, so that
            we get equivalent sized partitions that can be compared against each other. With this approach, we can validate the
            partitions in parallel leading to horizontal scaling of DVT.

        Returns:
            A list of where clauses for the source tables for each table pair
            Therefore you get a list of lists.
        """
        master_filter_list = []
        for config_manager in self.config_managers: # For each pair of tables
            validation_builder = ValidationBuilder(config_manager)

            # extract primary keys
            primary_keys=config_manager.get_primary_keys_list()

            source_partition_row_builder = PartitionRowBuilder(
                primary_keys,
                config_manager.source_client,
                config_manager.source_schema,
                config_manager.source_table,
                validation_builder.source_builder,
            )
            source_table=source_partition_row_builder.query
            target_partition_row_builder = PartitionRowBuilder(
                primary_keys,
                config_manager.target_client,
                config_manager.target_schema,
                config_manager.target_table,
                validation_builder.target_builder,
            )
            target_table=target_partition_row_builder.query

            # Get Source and Target row Count
            source_count = source_partition_row_builder.get_count()
            target_count = target_partition_row_builder.get_count()

            if abs(source_count - target_count) > source_count*0.1 :
                            logging.warning(
                                "Source and Target table row counts vary by more than 10%,"
                                "partitioning may result in partitions with very different sizes"
                            )

            # Decide on number of partitions after checking number requested is not > number of rows in source
            number_of_part = self.args.partition_num if self.args.partition_num < source_count else source_count
            
            # First we use the ntile aggregate function and divide assign a partition
            # number to each row in the source table
            window1 = ibis.window(order_by=primary_keys)
            nt = (
                    source_table.get_column(primary_keys[0])
                        .ntile(buckets=number_of_part)
                        .over(window1)
                        .name(consts.DVT_NTILE_COL)
                )
            dvt_nt = primary_keys.copy()
            dvt_nt.append(nt)
            partitioned_table = source_table.select(dvt_nt)
            # Partitioned table is just the primary key columns in the source table along with
            # an additional column with the partition number associated with each row.

            # We are interested in only the primary key values at the begining and end of
            # each partitition - the following window groups by partition number
            window2 = ibis.window(order_by=primary_keys, group_by=[consts.DVT_NTILE_COL])
            first_pkys = [partitioned_table.get_column(primary_key)
                            .first()
                            .over(window2)
                            .name(consts.DVT_FIRST_PRE + primary_key)
                            for primary_key in primary_keys
                        ]
            last_pkys = [partitioned_table.get_column(primary_key)
                            .last()
                            .over(window2)
                            .name(consts.DVT_LAST_PRE + primary_key)
                            for primary_key in primary_keys
                        ]
            partition_no = partitioned_table[consts.DVT_NTILE_COL].first().over(window2).name(consts.DVT_PART_NO)
            column_list = [partition_no] + first_pkys + last_pkys
            partition_boundary = partitioned_table.select(column_list).sort_by([consts.DVT_PART_NO]).distinct()
        
            # Up until this point, we have built the table expression, have not executed the query yet.
            # The query is now executed to find the first and last element of each partition
            first_last_elements = partition_boundary.execute().to_numpy()
            # Once we have the first and last elements of each partition, we can generate the where clause
            # i.e. greater than or equal to first element and less than or equal to last element
            filter_clause_list = [ '(' + self._geq_value(primary_keys, part_bounds[1:1+len(primary_keys)]) +
                                        ') AND (' + self._leq_value(primary_keys, part_bounds[1+len(primary_keys):])+ ')' 
                                    for part_bounds in first_last_elements]

            # the first filter clause just needs to be less than the upper bound
            # the last filter clause just needs to be greater than the lower bound
            # this is in case there are rows in the source or target table at the beginning and end that don't have
            # corresponding rows in the other table
            filter_clause_list[0] = self._leq_value(primary_keys, first_last_elements[0][1+len(primary_keys):])
            filter_clause_list[number_of_part-1] = self._geq_value(primary_keys, first_last_elements[number_of_part-1][1:1+len(primary_keys)])

            master_filter_list.append(filter_clause_list)
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
