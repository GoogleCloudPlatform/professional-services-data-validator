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
from data_validation.validation_builder import list_to_sublists


class PartitionBuilder:
    def __init__(self, config_managers: List[ConfigManager], args: Namespace) -> None:
        self.config_managers = config_managers
        self.table_count = len(config_managers)
        self.args = args
        self.config_dir = self._get_arg_config_dir()

    def _get_arg_config_dir(self) -> str:
        """Return String yaml config folder path."""
        if not self.args.config_dir:
            raise ValueError("YAML Config Dir Path was not supplied.")

        return self.args.config_dir

    def _add_filters_get_yaml_file(
        self,
        config_manager: ConfigManager,
        source_filters: List[str],
        target_filters: List[str],
    ) -> Dict:
        """Given a ConfigManager object (from the input args to generate-partitions), add a source
            and target filter, one at a time to create a validation block (one partition of the table).
            The returned dict contains the config manager with multiple validation blocks,
            which can be written to a yaml file. The number of validation blocks = length of the
            filter lists = parts-per-file parameter (except for the last file because
            parts-per-file may not divide partition-num evenly).

        Args:
            config_manager ConfigManager: Config manager instance.
            source_filters, target_filters: list of filters - for splitting the table into partitions.
        Returns:
            A dict which represents a yaml file.
        """
        # Create multiple yaml validation blocks corresponding to the filters provided
        yaml_validations = []
        for (source_filter, target_filter) in zip(source_filters, target_filters):
            filter_dict = {
                "type": "custom",
                "source": source_filter,
                "target": target_filter,
            }
            # Append partition new filter
            config_manager.filters.append(filter_dict)
            yaml_validations.append(config_manager.get_yaml_validation_block())
            config_manager.filters.pop()

        yaml_config = {
            consts.YAML_SOURCE: self.args.source_conn,
            consts.YAML_TARGET: self.args.target_conn,
            consts.YAML_RESULT_HANDLER: config_manager.result_handler_config,
            consts.YAML_VALIDATIONS: yaml_validations,
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
    def _extract_where(table_expr, client) -> str:
        """Given a ibis table expression with a filter (i.e. WHERE) clause, this function extracts the where clause
        in plain text

        Returns:
            String with the where condition
        """
        if client.name == "spanner":
            # As per Issue #1059, use the BQ dialect for Spanner as they have the same query syntax
            return re.sub(
                r"\s\s+",
                " ",
                ibis.to_sql(table_expr, dialect="bigquery").sql.split("WHERE")[1],
            ).replace("t0.", "")

        return re.sub(
            r"\s\s+", " ", ibis.to_sql(table_expr).sql.split("WHERE")[1]
        ).replace("t0.", "")

    def _get_partition_key_filters(self) -> List[List[List[str]]]:
        """The PartitionBuilder object contains the configuration of the table pairs (source and target)
           to be validated and the args (number of partitions). Generate the partitions for each table
           pair and return the partition filter list for all table pairs . A partition
           filter is the string that is used in the where clause - e.g. 'x >=25 and x <50'. The design
           doc for this section is available in docs/internal/partition_table_prd.md

        Returns:
            A list of list of list of strings for the source and target tables for each table pair
            i.e. (list of strings - 1 per partition) x (source and target) x (number of table pairs)
        """
        master_filter_list = []
        for config_manager in self.config_managers:  # For each pair of tables
            validation_builder = ValidationBuilder(config_manager)

            source_pks, target_pks = [], []
            for pk in config_manager.primary_keys:
                source_pks.append(pk["source_column"])
                target_pks.append(pk["target_column"])

            source_partition_row_builder = PartitionRowBuilder(
                source_pks,
                config_manager.source_client,
                config_manager.source_schema,
                config_manager.source_table,
                config_manager.source_query,
                validation_builder.source_builder,
            )
            source_table = source_partition_row_builder.query
            target_partition_row_builder = PartitionRowBuilder(
                target_pks,
                config_manager.target_client,
                config_manager.target_schema,
                config_manager.target_table,
                config_manager.target_query,
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
            window1 = ibis.window(order_by=source_pks)
            row_number = (ibis.row_number().over(window1) + 1).name(consts.DVT_POS_COL)

            if config_manager.trim_string_pks():
                dvt_keys = []
                for key in source_pks.copy():
                    if source_table[key].type().is_string():
                        rstrip_key = source_table[key].rstrip().name(key)
                        dvt_keys.append(rstrip_key)
                    else:
                        dvt_keys.append(key)
            else:
                dvt_keys = source_pks.copy()

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
                rownum_table
                if source_count == number_of_part
                else (
                    (
                        rownum_table[consts.DVT_POS_COL] * number_of_part
                        - (
                            rownum_table[consts.DVT_POS_COL]
                            * number_of_part
                            / source_count
                        ).floor()
                        * source_count
                    )
                    <= number_of_part
                )
                & (
                    (
                        rownum_table[consts.DVT_POS_COL] * number_of_part
                        - (
                            rownum_table[consts.DVT_POS_COL]
                            * number_of_part
                            / source_count
                        ).floor()
                        * source_count
                    )
                    > 0
                )
            )
            first_keys_table = rownum_table[cond].order_by(source_pks)

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

            def less_than_value(table, keys, values):
                key_column = table.__getattr__(keys[0])
                if key_column.type().is_date():
                    # Ensure date PKs are treated as date literals as per #1191
                    value = values[0].date()
                else:
                    value = values[0]

                if len(keys) == 1:
                    return key_column < value
                else:
                    return (key_column < value) | (
                        (key_column == value)
                        & less_than_value(table, keys[1:], values[1:])
                    )

            def geq_value(table, keys, values):
                key_column = table.__getattr__(keys[0])
                if key_column.type().is_date():
                    value = values[0].date()
                else:
                    value = values[0]

                if len(keys) == 1:
                    return key_column >= value
                else:
                    return (key_column > value) | (
                        (key_column == value) & geq_value(table, keys[1:], values[1:])
                    )

            filter_source_clause = less_than_value(
                source_table,
                source_pks,
                first_elements[1, : len(source_pks)],
            )
            filter_target_clause = less_than_value(
                target_table,
                target_pks,
                first_elements[1, : len(target_pks)],
            )
            source_where_list.append(
                self._extract_where(
                    source_table.filter(filter_source_clause),
                    config_manager.source_client,
                )
            )
            target_where_list.append(
                self._extract_where(
                    target_table.filter(filter_target_clause),
                    config_manager.target_client,
                )
            )

            for i in range(1, first_elements.shape[0] - 1):
                filter_source_clause = geq_value(
                    source_table,
                    source_pks,
                    first_elements[i, : len(source_pks)],
                ) & less_than_value(
                    source_table,
                    source_pks,
                    first_elements[i + 1, : len(source_pks)],
                )
                filter_target_clause = geq_value(
                    target_table,
                    target_pks,
                    first_elements[i, : len(target_pks)],
                ) & less_than_value(
                    target_table,
                    target_pks,
                    first_elements[i + 1, : len(target_pks)],
                )
                source_where_list.append(
                    self._extract_where(
                        source_table.filter(filter_source_clause),
                        config_manager.source_client,
                    )
                )
                target_where_list.append(
                    self._extract_where(
                        target_table.filter(filter_target_clause),
                        config_manager.target_client,
                    )
                )
            filter_source_clause = geq_value(
                source_table,
                source_pks,
                first_elements[len(first_elements) - 1, : len(source_pks)],
            )
            filter_target_clause = geq_value(
                target_table,
                target_pks,
                first_elements[len(first_elements) - 1, : len(target_pks)],
            )
            source_where_list.append(
                self._extract_where(
                    source_table.filter(filter_source_clause),
                    config_manager.source_client,
                )
            )
            target_where_list.append(
                self._extract_where(
                    target_table.filter(filter_target_clause),
                    config_manager.target_client,
                )
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
            self.config_managers is a list of config_manager objects. Each config_manager object in
            list refers to a table pair (source and target) that are validated against each other.
            In most cases, this list is of length 1 because it was invoked as -tbls src=targ. If it
            was invoked with -tbls src1=targ1,src2=targ2 the self.config_managers will be of length
            2. Partition_filters is a list of list of lists of Partition filters
            - which is (list of filter strings, one per partition) x 2 (source & target) x number of table pairs
        Returns:
            yaml_configs_list (List[Dict]): List of YAML dicts (folder), one folder for each table pair being validated.
        """
        yaml_configs_list = [None] * len(self.config_managers)
        for ind, config_manager in enumerate(self.config_managers):
            filter_list = partition_filters[ind]

            yaml_configs_list[ind] = {
                "target_folder_name": config_manager.full_source_table,
                "yaml_files": [],
            }

            # Create a list of lists chunked by partitions per file
            # Both source and target table are divided into the same number of partitions, so we are
            # guaranteed filter_list[0] (source) and filter_list[1] (target) are of the same length
            source_filters_list = list_to_sublists(
                filter_list[0], self.args.parts_per_file
            )
            target_filters_list = list_to_sublists(
                filter_list[1], self.args.parts_per_file
            )
            for i in range(len(source_filters_list)):
                # Build and append partition YAML
                yaml_config = self._add_filters_get_yaml_file(
                    config_manager, source_filters_list[i], target_filters_list[i]
                )
                yaml_configs_list[ind]["yaml_files"].append(
                    {"target_file_name": f"{i:04}.yaml", "yaml_config": yaml_config}
                )
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
            for yaml_file in table["yaml_files"]:
                yaml_config = yaml_file["yaml_config"]
                target_file_name = yaml_file["target_file_name"]
                target_file_path = os.path.join(target_folder_path, target_file_name)
                cli_tools.store_validation(
                    target_file_path, yaml_config, include_log=False
                )

        logging.info(
            f"Success! Table partition configs written to directory: {self.config_dir}"
        )
