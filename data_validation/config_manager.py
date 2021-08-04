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

import google.oauth2.service_account

from data_validation import consts, clients
from data_validation.result_handlers.bigquery import BigQueryResultHandler
from data_validation.result_handlers.text import TextResultHandler
from data_validation.validation_builder import ValidationBuilder


class ConfigManager(object):

    _config: dict = None
    source_client = None
    target_client = None

    def __init__(self, config, source_client, target_client, verbose=False):
        """Initialize a ConfigManager client which supplies the
            source and target queries to run.

        Args:
            config (Dict): The Validation config supplied
            source_client (IbisClient): The Ibis client for the source DB
            target_client (IbisClient): The Ibis client for the target DB
            verbose (Bool): If verbose, the Data Validation client will print queries run
            google_credentials (google.auth.credentials.Credentials):
                Explicit credentials to use in case default credentials
                aren't working properly.
        """
        self._config = config

        self.source_client = source_client
        self.target_client = target_client
        if not self.process_in_memory():
            self.target_client = source_client

        self.verbose = verbose

    @property
    def config(self):
        """Return config object."""
        return self._config

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
        """Return string validation type (Column|GroupedColumn|Row)."""
        return self._config[consts.CONFIG_TYPE]

    def process_in_memory(self):
        if (
            self.validation_type == "Row"
            and self.source_connection == self.target_connection
        ):
            return False

        return True

    @property
    def max_recursive_query_size(self):
        """Return Aggregates from Config """
        return self._config.get(consts.CONFIG_MAX_RECURSIVE_QUERY_SIZE, 50000)

    @property
    def aggregates(self):
        """Return Aggregates from Config """
        return self._config.get(consts.CONFIG_AGGREGATES, [])

    def append_aggregates(self, aggregate_configs):
        """Append aggregate configs to existing config."""
        self._config[consts.CONFIG_AGGREGATES] = self.aggregates + aggregate_configs

    @property
    def calculated_fields(self):
        return self._config.get(consts.CONFIG_CALCULATED_FIELDS)

    def append_calculated_fields(self, calculated_configs):
        self._config[consts.CONFIG_CALCULATED_FIELDS] = (
            self.calculated_fields + calculated_configs
        )

    @property
    def query_groups(self):
        """ Return Query Groups from Config """
        return self._config.get(consts.CONFIG_GROUPED_COLUMNS, [])

    def append_query_groups(self, grouped_column_configs):
        """Append grouped configs to existing config."""
        self._config[consts.CONFIG_GROUPED_COLUMNS] = (
            self.query_groups + grouped_column_configs
        )

    @property
    def primary_keys(self):
        """ Return Query Groups from Config """
        return self._config.get(consts.CONFIG_PRIMARY_KEYS, [])

    def append_primary_keys(self, primary_key_configs):
        """Append primary key configs to existing config."""
        self._config[consts.CONFIG_PRIMARY_KEYS] = (
            self.primary_keys + primary_key_configs
        )

    @property
    def filters(self):
        """Return Filters from Config """
        return self._config.get(consts.CONFIG_FILTERS, [])

    @property
    def source_schema(self):
        """Return string value of source schema."""
        return self._config.get(consts.CONFIG_SCHEMA_NAME, None)

    @property
    def source_table(self):
        """Return string value of source table."""
        return self._config[consts.CONFIG_TABLE_NAME]

    @property
    def target_schema(self):
        """Return string value of target schema."""
        return self._config.get(consts.CONFIG_TARGET_SCHEMA_NAME, self.source_schema)

    @property
    def target_table(self):
        """Return string value of target table."""
        return self._config.get(
            consts.CONFIG_TARGET_TABLE_NAME, self._config[consts.CONFIG_TABLE_NAME]
        )

    @property
    def full_target_table(self):
        """Return string value of fully qualified target table."""
        if self.target_schema:
            return self.target_schema + "." + self.target_table
        else:
            return self.target_table

    @property
    def full_source_table(self):
        """Return string value of target table."""
        if self.source_schema:
            return self.source_schema + "." + self.source_table
        else:
            return self.source_table

    @property
    def labels(self):
        """Return labels."""
        return self._config.get(consts.CONFIG_LABELS, [])

    @property
    def result_handler_config(self):
        """Return int limit for query executions."""
        return self._config.get(consts.CONFIG_RESULT_HANDLER) or {}

    @property
    def query_limit(self):
        """Return int limit for query executions."""
        return self._config.get(consts.CONFIG_LIMIT)

    @property
    def threshold(self):
        """Return threshold from Config """
        return self._config.get(consts.CONFIG_THRESHOLD, 0.0)

    def get_source_ibis_table(self):
        """Return IbisTable from source."""
        if not hasattr(self, "_source_ibis_table"):
            self._source_ibis_table = clients.get_ibis_table(
                self.source_client, self.source_schema, self.source_table
            )
        return self._source_ibis_table

    def get_source_ibis_calculated_table(self):
        """Return mutated IbisTable from source"""
        table = self.get_source_ibis_table()
        vb = ValidationBuilder(self)
        calculated_table = table.mutate(
            vb.source_builder.compile_calculated_fields(table)
        )

        return calculated_table

    def get_target_ibis_table(self):
        """Return IbisTable from target."""
        if not hasattr(self, "_target_ibis_table"):
            self._target_ibis_table = clients.get_ibis_table(
                self.target_client, self.target_schema, self.target_table
            )
        return self._target_ibis_table

    def get_target_ibis_calculated_table(self):
        """Return mutated IbisTable from target"""
        table = self.get_target_ibis_table()
        vb = ValidationBuilder(self)
        calculated_table = table.mutate(
            vb.target_builder.compile_calculated_fields(table)
        )

        return calculated_table

    def get_yaml_validation_block(self):
        """Return Dict object formatted for a Yaml file."""
        config = copy.deepcopy(self.config)

        del config[consts.CONFIG_SOURCE_CONN]
        del config[consts.CONFIG_TARGET_CONN]
        if consts.CONFIG_RESULT_HANDLER in config:
            del config[consts.CONFIG_RESULT_HANDLER]

        return config

    def get_result_handler(self):
        """Return ResultHandler instance from supplied config."""
        if not self.result_handler_config:
            return TextResultHandler()

        result_type = self.result_handler_config[consts.CONFIG_TYPE]
        if result_type == "BigQuery":
            project_id = self.result_handler_config[consts.PROJECT_ID]
            table_id = self.result_handler_config[consts.TABLE_ID]
            key_path = self.result_handler_config.get(
                consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH
            )
            if key_path:
                credentials = google.oauth2.service_account.Credentials.from_service_account_file(
                    key_path
                )
            else:
                credentials = None
            return BigQueryResultHandler.get_handler_for_project(
                project_id, table_id=table_id, credentials=credentials
            )
        else:
            raise ValueError(f"Unknown ResultHandler Class: {result_type}")

    @staticmethod
    def build_config_manager(
        config_type,
        source_conn,
        target_conn,
        source_client,
        target_client,
        table_obj,
        labels,
        threshold,
        result_handler_config=None,
        filter_config=None,
        verbose=False,
    ):
        if isinstance(filter_config, dict):
            filter_config = [filter_config]

        """Return a ConfigManager instance with available config."""
        config = {
            consts.CONFIG_TYPE: config_type,
            consts.CONFIG_SOURCE_CONN: source_conn,
            consts.CONFIG_TARGET_CONN: target_conn,
            consts.CONFIG_TABLE_NAME: table_obj[consts.CONFIG_TABLE_NAME],
            consts.CONFIG_TARGET_TABLE_NAME: table_obj.get(
                consts.CONFIG_TARGET_TABLE_NAME, table_obj[consts.CONFIG_TABLE_NAME]
            ),
            consts.CONFIG_LABELS: labels,
            consts.CONFIG_THRESHOLD: threshold,
            consts.CONFIG_RESULT_HANDLER: result_handler_config,
            consts.CONFIG_FILTERS: filter_config,
        }

        # Only FileSystem connections do not require schemas
        if source_conn["source_type"] != "FileSystem":
            config[consts.CONFIG_SCHEMA_NAME] = table_obj[consts.CONFIG_SCHEMA_NAME]
            config[consts.CONFIG_TARGET_SCHEMA_NAME] = table_obj.get(
                consts.CONFIG_TARGET_SCHEMA_NAME, table_obj[consts.CONFIG_SCHEMA_NAME]
            )
        else:
            config[consts.CONFIG_SCHEMA_NAME] = None
            config[consts.CONFIG_TARGET_SCHEMA_NAME] = None

        return ConfigManager(config, source_client, target_client, verbose=verbose)

    def build_config_grouped_columns(self, grouped_columns):
        """Return list of grouped column config objects."""
        grouped_column_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()
        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        for column in grouped_columns:

            if column.casefold() not in casefold_source_columns:
                raise ValueError(
                    f"GroupedColumn DNE in source: {source_table.op().name}.{column}"
                )
            if column.casefold() not in casefold_target_columns:
                raise ValueError(
                    f"GroupedColumn DNE in target: {target_table.op().name}.{column}"
                )
            column_config = {
                consts.CONFIG_SOURCE_COLUMN: casefold_source_columns[column.casefold()],
                consts.CONFIG_TARGET_COLUMN: casefold_target_columns[column.casefold()],
                consts.CONFIG_FIELD_ALIAS: column,
                consts.CONFIG_CAST: None,
            }
            grouped_column_configs.append(column_config)

        return grouped_column_configs

    def build_config_count_aggregate(self):
        """Return dict aggregate for COUNT(*)."""
        aggregate_config = {
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
            consts.CONFIG_TYPE: "count",
        }

        return aggregate_config

    def build_config_column_aggregates(self, agg_type, arg_value, supported_types):
        """Return list of aggregate objects of given agg_type."""
        aggregate_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()

        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        allowlist_columns = arg_value or casefold_source_columns
        for column in casefold_source_columns:
            # Get column type and remove precision/scale attributes
            column_type_str = str(source_table[casefold_source_columns[column]].type())
            column_type = column_type_str.split("(")[0]

            if column not in allowlist_columns:
                continue
            elif column not in casefold_target_columns:
                logging.info(
                    f"Skipping Agg {agg_type}: {source_table.op().name}.{column}"
                )
                continue
            elif supported_types and column_type not in supported_types:
                if self.verbose:
                    msg = f"Skipping Agg {agg_type}: {source_table.op().name}.{column} {column_type}"
                    print(msg)
                continue

            aggregate_config = {
                consts.CONFIG_SOURCE_COLUMN: casefold_source_columns[column],
                consts.CONFIG_TARGET_COLUMN: casefold_target_columns[column],
                consts.CONFIG_FIELD_ALIAS: f"{agg_type}__{column}",
                consts.CONFIG_TYPE: agg_type,
            }
            aggregate_configs.append(aggregate_config)

        return aggregate_configs
