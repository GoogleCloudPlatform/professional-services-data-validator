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
import ibis
import pandas
import logging
import re
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
        self.primary_keys = self._get_primary_keys()

    def _get_arg_config_dir(self) -> str:
        """Return String yaml config folder path."""
        if not self.args.config_dir:
            raise ValueError("YAML Config Dir Path was not supplied.")

        return self.args.config_dir

    def _get_primary_keys(self) -> str:
        """Return the Primary Keys"""
        return cli_tools.get_arg_list(self.args.primary_keys)

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

        # Default partition logic: Use NTILE function to create partitions, ordering by primary keys.
        partition_filters = self._get_partition_key_filters()
        yaml_configs_list = self._add_partition_filters(partition_filters)
        self._store_partitions(yaml_configs_list)

    @staticmethod
    def _extract_where(x) -> str:
        """Given a ibis table expression with a filter (i.e. WHERE) clause, this function extracts the where clause
        in plain text

        Returns:
            String with the where condition
        """
        return re.sub(r"\s\s+", " ", ibis.to_sql(x).sql.split("WHERE")[1]).replace(
            "t0.", ""
        )

    def _get_partition_key_filters(self) -> List[List[List[str]]]:
        """Generate where clauses for each partition for each table pair. We have to create separate where for the source and
            target as they may each have a different filter applied to them. We are partitioning the tables based on keys, so that
            we get equivalent sized partitions that can be compared against each other. With this approach, we can validate the
            partitions in parallel leading to horizontal scaling of DVT. The design doc for this section is available in
            docs/internal/partition_table_prd.md

        Returns:
            A list of where clauses for the source tables for each table pair
            Therefore you get a list of lists.
        """
        master_filter_list = []
        for config_manager in self.config_managers:  # For each pair of tables
            validation_builder = ValidationBuilder(config_manager)

            source_partition_row_builder = PartitionRowBuilder(
                self.primary_keys,
                config_manager.source_client,
                config_manager.source_schema,
                config_manager.source_table,
                validation_builder.source_builder,
            )
            source_table = source_partition_row_builder.query
            target_partition_row_builder = PartitionRowBuilder(
                self.primary_keys,
                config_manager.target_client,
                config_manager.target_schema,
                config_manager.target_table,
                validation_builder.target_builder,
            )
            target_table = target_partition_row_builder.query

            # Get Source and Target row Count
            source_count = source_partition_row_builder.get_count()
            target_count = target_partition_row_builder.get_count()

            # For some reason Teradata connector returns a dataframe with the count element,
            # while the other connectors return a numpy.int64 value
            if isinstance(source_count, pandas.DataFrame):
                source_count = source_count.values[0][0]
            if isinstance(target_count, pandas.DataFrame):
                target_count = target_count.values[0][0]

            if abs(source_count - target_count) > source_count * 0.1:
                logging.warning(
                    "Source and Target table row counts vary by more than 10%,"
                    "partitioning may result in partitions with very different sizes"
                )

            # Decide on number of partitions after checking number requested is not > number of rows in source
            number_of_part = (
                self.args.partition_num
                if self.args.partition_num < source_count
                else source_count
            )

            # First we number each row in the source table. Using row_number instead of ntile since it is
            # available on all platforms (Teradata does not support NTILE). For our purposes, it is likely
            # more efficient
            window1 = ibis.window(order_by=self.primary_keys)
            row_number = (ibis.row_number().over(window1) + 1).name(consts.DVT_POS_COL)
            dvt_keys = self.primary_keys.copy()
            dvt_keys.append(row_number)
            rownum_table = source_table.select(dvt_keys)
            # Rownum table is just the primary key columns in the source table along with
            # an additional column with the row number associated with each row.

            # This rather complicated expression below is a filter (where) clause condition that filters the row numbers
            # that correspond to the first element of the partition. The number of a partition is
            # ceiling(row number * # of partitions / total number of rows). The first element of the partition is where
            # the remainder, i.e. row number * # of partitions % total number of rows is > 0 and <= number of partitions.
            # The remainder function does not work well with Teradata, hence writing that out explicitly.
            cond = (
                (
                    rownum_table[consts.DVT_POS_COL] * number_of_part
                    - (
                        rownum_table[consts.DVT_POS_COL] * number_of_part / source_count
                    ).floor()
                    * source_count
                )
                <= number_of_part
            ) & (
                (
                    rownum_table[consts.DVT_POS_COL] * number_of_part
                    - (
                        rownum_table[consts.DVT_POS_COL] * number_of_part / source_count
                    ).floor()
                    * source_count
                )
                > 0
            )
            first_keys_table = rownum_table[cond].order_by(self.primary_keys)

            # Up until this point, we have built the table expression, have not executed the query yet.
            # The query is now executed to find the first element of each partition
            first_elements = first_keys_table.execute().to_numpy()

            # Once we have the first element of each partition, we can generate the where clause
            # i.e. greater than or equal to first element and less than first element of next partition
            # The first and the last partitions have special where clauses - less than first element of second
            # partition and greater than or equal to the first element of the last partition respectively
            source_where_list = []
            target_where_list = []

            # Given a list of primary keys and corresponding values, the following lambda function builds the filter expression
            # to find all rows before the row containing the values in the sort order. The next function geq_value, finds all
            # rows after the row containing the values in the sort order, including the row specified by values.
            less_than_value = (
                lambda table, keys, values: table.__getattr__(keys[0]) < values[0]
                if len(keys) == 1
                else (table.__getattr__(keys[0]) < values[0])
                | (
                    (table.__getattr__(keys[0]) == values[0])
                    & less_than_value(table, keys[1:], values[1:])
                )
            )
            geq_value = (
                lambda table, keys, values: table.__getattr__(keys[0]) >= values[0]
                if len(keys) == 1
                else (table.__getattr__(keys[0]) > values[0])
                | (
                    (table.__getattr__(keys[0]) == values[0])
                    & geq_value(table, keys[1:], values[1:])
                )
            )

            filter_source_clause = less_than_value(
                source_table,
                self.primary_keys,
                first_elements[1, : len(self.primary_keys)],
            )
            filter_target_clause = less_than_value(
                target_table,
                self.primary_keys,
                first_elements[1, : len(self.primary_keys)],
            )
            source_where_list.append(
                self._extract_where(source_table.filter(filter_source_clause))
            )
            target_where_list.append(
                self._extract_where(target_table.filter(filter_target_clause))
            )

            for i in range(1, first_elements.shape[0] - 1):
                filter_source_clause = geq_value(
                    source_table,
                    self.primary_keys,
                    first_elements[i, : len(self.primary_keys)],
                ) & less_than_value(
                    source_table,
                    self.primary_keys,
                    first_elements[i + 1, : len(self.primary_keys)],
                )
                filter_target_clause = geq_value(
                    target_table,
                    self.primary_keys,
                    first_elements[i, : len(self.primary_keys)],
                ) & less_than_value(
                    target_table,
                    self.primary_keys,
                    first_elements[i + 1, : len(self.primary_keys)],
                )
                source_where_list.append(
                    self._extract_where(source_table.filter(filter_source_clause))
                )
                target_where_list.append(
                    self._extract_where(target_table.filter(filter_target_clause))
                )
            filter_source_clause = geq_value(
                source_table,
                self.primary_keys,
                first_elements[len(first_elements) - 1, : len(self.primary_keys)],
            )
            filter_target_clause = geq_value(
                target_table,
                self.primary_keys,
                first_elements[len(first_elements) - 1, : len(self.primary_keys)],
            )
            source_where_list.append(
                self._extract_where(source_table.filter(filter_source_clause))
            )
            target_where_list.append(
                self._extract_where(target_table.filter(filter_target_clause))
            )
            master_filter_list.append([source_where_list, target_where_list])
        return master_filter_list

    def _add_partition_filters(
        self,
        partition_filters: List[List[List[str]]],
    ) -> List[Dict]:
        """Add Partition Filters to ConfigManager and return a list of dict
        ConfigManager objects.

        Args:
            partition_filters (List[List[List[str]]]): List of List of Partition filters
            for all Table/ConfigManager objects, two list of filters for each configManager object,
            one for the source and one for the target

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
            for pos in range(len(filter_list[0])):
                filter_dict = {
                    "type": "custom",
                    "source": filter_list[0][pos],
                    "target": filter_list[1][pos],
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
