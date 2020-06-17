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

import copy
import logging

from data_validation import consts


class ConfigManager(object):

    _config: dict = None
    source_client = None
    target_client = None

    def __init__(self, config, source_client, target_client, verbose=False):
        """ Initialize a ValidationBuilder client which supplies the
            source and target queries tto run.

        Args:
            config (Dict): The Validation config supplied
            source_client (IbisClient): The Ibis client for the source DB
            target_client (IbisClient): The Ibis client for the target DB
            verbose (Bool): If verbose, the Data Validation client will print queries run
        """
        self._config = config

        self.source_client = source_client
        self.target_client = target_client

        self.verbose = verbose

    @property
    def config(self):
        """Return config object."""
        return copy.deepcopy(self._config)

    @property
    def source_connection(self):
        """return source connection object."""
        return self._config.get(consts.CONFIG_SOURCE_CONN)

    @property
    def target_connection(self):
        """return source connection object."""
        return self._config.get(consts.CONFIG_TARGET_CONN)

    @property
    def validation_type(self):
        """Return string validation type (Column|GroupedColumn)."""
        return self._config[consts.CONFIG_TYPE]

    @property
    def aggregates(self):
        """Return Aggregates from Config """
        return self._config.get(consts.CONFIG_AGGREGATES, [])

    def append_aggregates(self, aggregate_configs):
        """Append aggregate configs to existing config."""
        self._config[consts.CONFIG_AGGREGATES] = self.aggregates + aggregate_configs

    @property
    def query_groups(self):
        """ Return Query Groups from Config """
        return self._config.get(consts.CONFIG_GROUPED_COLUMNS, [])

    def append_query_groups(self, grouped_column_configs):
        """Append aggregate configs to existing config."""
        self._config[consts.CONFIG_GROUPED_COLUMNS] = (
            self.query_groups + grouped_column_configs
        )

    @property
    def source_schema(self):
        """Return string value of source schema."""
        return self._config[consts.CONFIG_SCHEMA_NAME]

    @property
    def source_table(self):
        """Return string value of source table."""
        return self._config[consts.CONFIG_TABLE_NAME]

    @property
    def target_schema(self):
        """Return string value of target schema."""
        return self._config.get(
            consts.CONFIG_TARGET_SCHEMA_NAME, self._config[consts.CONFIG_SCHEMA_NAME]
        )

    @property
    def target_table(self):
        """Return string value of target table."""
        return self._config.get(
            consts.CONFIG_TARGET_TABLE_NAME, self._config[consts.CONFIG_TABLE_NAME]
        )

    @property
    def query_limit(self):
        """Return int limit for query executions."""
        return self._config.get(consts.CONFIG_LIMIT)

    def get_source_ibis_table(self):
        """Return IbisTable from source."""
        if not hasattr(self, "_source_ibis_table"):
            self._source_ibis_table = self.source_client.table(
                self.source_table, database=self.source_schema
            )

        return self._source_ibis_table

    def get_target_ibis_table(self):
        """Return IbisTable from target."""
        if not hasattr(self, "_target_ibis_table"):
            self._target_ibis_table = self.target_client.table(
                self.target_table, database=self.target_schema
            )

        return self._target_ibis_table

    def get_yaml_validation_block(self):
        """Return Dict object formatted for a Yaml file."""
        config = self.config

        config.pop(consts.CONFIG_SOURCE_CONN)
        config.pop(consts.CONFIG_TARGET_CONN)

        return config

    @staticmethod
    def build_config_manager(
        config_type,
        source_conn,
        target_conn,
        source_client,
        target_client,
        table_obj,
        verbose=False,
    ):
        """Return a ConfigManager instance with available config."""
        config = {
            consts.CONFIG_TYPE: config_type,
            consts.CONFIG_SOURCE_CONN: source_conn,
            consts.CONFIG_TARGET_CONN: target_conn,
            consts.CONFIG_SCHEMA_NAME: table_obj[consts.CONFIG_SCHEMA_NAME],
            consts.CONFIG_TABLE_NAME: table_obj[consts.CONFIG_TABLE_NAME],
            consts.CONFIG_TARGET_SCHEMA_NAME: table_obj.get(
                consts.CONFIG_TARGET_SCHEMA_NAME, table_obj[consts.CONFIG_SCHEMA_NAME]
            ),
            consts.CONFIG_TARGET_TABLE_NAME: table_obj.get(
                consts.CONFIG_TARGET_TABLE_NAME, table_obj[consts.CONFIG_TABLE_NAME]
            ),
        }

        return ConfigManager(config, source_client, target_client, verbose=verbose)

    def build_config_grouped_columns(self, grouped_columns):
        """Return list of grouped column config objects."""
        grouped_column_configs = []
        source_table = self.get_source_ibis_table()
        for column in grouped_columns:
            if column not in source_table.columns:
                raise ValueError(
                    f"GroupedColumn DNE: {source_table.op().name}.{column}"
                )
            column_config = {
                consts.CONFIG_SOURCE_COLUMN: column,
                consts.CONFIG_TARGET_COLUMN: column,
                consts.CONFIG_FIELD_ALIAS: column,
                consts.CONFIG_CAST: None,
            }
            grouped_column_configs.append(column_config)

        return grouped_column_configs

    def build_config_aggregates(self, agg_type, arg_value, supported_types):
        """Return list of aggregate objects of given agg_type."""
        aggregate_configs = []
        source_table = self.get_source_ibis_table()
        target_table = self.get_target_ibis_table()
        allowlist_columns = arg_value or source_table.columns
        for column in source_table.columns:
            if column not in allowlist_columns:
                continue
            elif column not in target_table.columns:
                logging.info(
                    f"Skipping Agg {agg_type}: {source_table.op().name}.{column}"
                )
                continue
            elif (
                supported_types
                and str(source_table[column].type()) not in supported_types
            ):
                continue

            aggregate_config = {
                consts.CONFIG_SOURCE_COLUMN: column,
                consts.CONFIG_TARGET_COLUMN: column,
                consts.CONFIG_FIELD_ALIAS: f"{agg_type}__{column}",
                consts.CONFIG_TYPE: agg_type,
            }
            aggregate_configs.append(aggregate_config)

        return aggregate_configs
