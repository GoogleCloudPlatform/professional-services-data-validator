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
from ibis_bigquery.client import BigQueryClient

from data_validation import clients, consts, state_manager
from data_validation.result_handlers.bigquery import BigQueryResultHandler
from data_validation.result_handlers.text import TextResultHandler
from data_validation.validation_builder import ValidationBuilder


class ConfigManager(object):
    _config: dict = None
    _source_conn = None
    _target_conn = None
    _state_manager = None
    source_client = None
    target_client = None

    def __init__(self, config, source_client=None, target_client=None, verbose=False):
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
        self._state_manager = state_manager.StateManager()
        self._config = config

        self.source_client = source_client or clients.get_data_client(
            self.get_source_connection()
        )
        self.target_client = target_client or clients.get_data_client(
            self.get_target_connection()
        )

        self.verbose = verbose
        if self.validation_type not in consts.CONFIG_TYPES:
            raise ValueError(f"Unknown Configuration Type: {self.validation_type}")

    @property
    def config(self):
        """Return config object."""
        return self._config

    def get_source_connection(self):
        """Return source connection object."""
        if not self._source_conn:
            if self._config.get(consts.CONFIG_SOURCE_CONN):
                self._source_conn = self._config.get(consts.CONFIG_SOURCE_CONN)
            else:
                conn_name = self._config.get(consts.CONFIG_SOURCE_CONN_NAME)
                self._source_conn = self._state_manager.get_connection_config(conn_name)

        return self._source_conn

    def get_target_connection(self):
        """Return target connection object."""
        if not self._target_conn:
            if self._config.get(consts.CONFIG_TARGET_CONN):
                self._target_conn = self._config.get(consts.CONFIG_TARGET_CONN)
            else:
                conn_name = self._config.get(consts.CONFIG_TARGET_CONN_NAME)
                self._target_conn = self._state_manager.get_connection_config(conn_name)

        return self._target_conn

    @property
    def validation_type(self):
        """Return string validation type (Column|Schema)."""
        return self._config[consts.CONFIG_TYPE]

    def use_random_rows(self):
        """Return if the validation should use a random row filter."""
        return self._config.get(consts.CONFIG_USE_RANDOM_ROWS) or False

    def random_row_batch_size(self):
        """Return if the validation should use a random row filter."""
        return (
            self._config.get(consts.CONFIG_RANDOM_ROW_BATCH_SIZE)
            or consts.DEFAULT_NUM_RANDOM_ROWS
        )

    def get_random_row_batch_size(self):
        """Return number of random rows or None."""
        return self.random_row_batch_size() if self.use_random_rows() else None

    def process_in_memory(self):
        """Return whether to process in memory or on a remote platform."""
        return True

    @property
    def max_recursive_query_size(self):
        """Return Aggregates from Config"""
        return self._config.get(consts.CONFIG_MAX_RECURSIVE_QUERY_SIZE, 50000)

    @property
    def aggregates(self):
        """Return Aggregates from Config"""
        return self._config.get(consts.CONFIG_AGGREGATES, [])

    def append_aggregates(self, aggregate_configs):
        """Append aggregate configs to existing config."""
        self._config[consts.CONFIG_AGGREGATES] = self.aggregates + aggregate_configs

    @property
    def calculated_fields(self):
        return self._config.get(consts.CONFIG_CALCULATED_FIELDS, [])

    def append_calculated_fields(self, calculated_configs):
        self._config[consts.CONFIG_CALCULATED_FIELDS] = (
            self.calculated_fields + calculated_configs
        )

    @property
    def dependent_aliases(self):
        """Return all columns that are needed in final dataframe for row validations."""
        return self._config.get(consts.CONFIG_DEPENDENT_ALIASES, [])

    def append_dependent_aliases(self, dependent_aliases):
        """Appends columns that are needed in final dataframe for row validations."""
        self._config[consts.CONFIG_DEPENDENT_ALIASES] = (
            self.dependent_aliases + dependent_aliases
        )

    @property
    def query_groups(self):
        """Return Query Groups from Config"""
        return self._config.get(consts.CONFIG_GROUPED_COLUMNS, [])

    def append_query_groups(self, grouped_column_configs):
        """Append grouped configs to existing config."""
        self._config[consts.CONFIG_GROUPED_COLUMNS] = (
            self.query_groups + grouped_column_configs
        )

    @property
    def custom_query_type(self):
        """Return custom query type from config"""
        return self._config.get(consts.CONFIG_CUSTOM_QUERY_TYPE, "")

    def append_custom_query_type(self, custom_query_type):
        """Append custom query type config to existing config."""
        self._config[consts.CONFIG_CUSTOM_QUERY_TYPE] = (
            self.custom_query_type + custom_query_type
        )

    @property
    def source_query_file(self):
        """Return SQL Query File from Config"""
        return self._config.get(consts.CONFIG_SOURCE_QUERY_FILE, [])

    def append_source_query_file(self, query_file_configs):
        """Append grouped configs to existing config."""
        self._config[consts.CONFIG_SOURCE_QUERY_FILE] = (
            self.source_query_file + query_file_configs
        )

    @property
    def target_query_file(self):
        """Return SQL Query File from Config"""
        return self._config.get(consts.CONFIG_TARGET_QUERY_FILE, [])

    def append_target_query_file(self, query_file_configs):
        """Append grouped configs to existing config."""
        self._config[consts.CONFIG_TARGET_QUERY_FILE] = (
            self.target_query_file + query_file_configs
        )

    @property
    def primary_keys(self):
        """Return Primary keys from Config"""
        return self._config.get(consts.CONFIG_PRIMARY_KEYS, [])

    def append_primary_keys(self, primary_key_configs):
        """Append primary key configs to existing config."""
        self._config[consts.CONFIG_PRIMARY_KEYS] = (
            self.primary_keys + primary_key_configs
        )

    def get_primary_keys_list(self):
        """Return list of primary key column names"""
        return [key[consts.CONFIG_SOURCE_COLUMN] for key in self.primary_keys]

    @property
    def comparison_fields(self):
        """Return fields from Config"""
        return self._config.get(consts.CONFIG_COMPARISON_FIELDS, [])

    def append_comparison_fields(self, field_configs):
        """Append field configs to existing config."""
        self._config[consts.CONFIG_COMPARISON_FIELDS] = (
            self.comparison_fields + field_configs
        )

    @property
    def filters(self):
        """Return Filters from Config"""
        return self._config.get(consts.CONFIG_FILTERS, [])

    @property
    def source_schema(self):
        """Return string value of source schema."""
        if self.source_client._source_type == "FileSystem":
            return None
        return self._config.get(consts.CONFIG_SCHEMA_NAME, None)

    @property
    def source_table(self):
        """Return string value of source table."""
        return self._config[consts.CONFIG_TABLE_NAME]

    @property
    def target_schema(self):
        """Return string value of target schema."""
        if self.target_client._source_type == "FileSystem":
            return None
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
        """Return threshold from Config"""
        return self._config.get(consts.CONFIG_THRESHOLD, 0.0)

    def get_source_ibis_table(self):
        """Return IbisTable from source."""
        if not hasattr(self, "_source_ibis_table"):
            self._source_ibis_table = clients.get_ibis_table(
                self.source_client, self.source_schema, self.source_table
            )
        return self._source_ibis_table

    def get_source_ibis_calculated_table(self, depth=None):
        """Return mutated IbisTable from source
        n: Int the depth of subquery requested"""
        table = self.get_source_ibis_table()
        vb = ValidationBuilder(self)
        calculated_table = table.mutate(
            vb.source_builder.compile_calculated_fields(table, n=depth)
        )

        return calculated_table

    def get_target_ibis_table(self):
        """Return IbisTable from target."""
        if not hasattr(self, "_target_ibis_table"):
            self._target_ibis_table = clients.get_ibis_table(
                self.target_client, self.target_schema, self.target_table
            )
        return self._target_ibis_table

    def get_target_ibis_calculated_table(self, depth=None):
        """Return mutated IbisTable from target
        n: Int the depth of subquery requested"""
        table = self.get_target_ibis_table()
        vb = ValidationBuilder(self)
        calculated_table = table.mutate(
            vb.target_builder.compile_calculated_fields(table, n=depth)
        )

        return calculated_table

    def get_yaml_validation_block(self):
        """Return Dict object formatted for a Yaml file."""
        config = copy.deepcopy(self.config)

        config.pop(consts.CONFIG_SOURCE_CONN, None)
        config.pop(consts.CONFIG_TARGET_CONN, None)

        config.pop(consts.CONFIG_SOURCE_CONN_NAME, None)
        config.pop(consts.CONFIG_TARGET_CONN_NAME, None)

        config.pop(consts.CONFIG_RESULT_HANDLER, None)

        return config

    def get_result_handler(self):
        """Return ResultHandler instance from supplied config."""
        if not self.result_handler_config:
            if self.config[consts.CONFIG_TYPE] == consts.SCHEMA_VALIDATION:
                cols_filter_list = consts.SCHEMA_VALIDATION_COLUMN_FILTER_LIST
            else:
                cols_filter_list = consts.COLUMN_FILTER_LIST
            return TextResultHandler(
                self._config.get(consts.CONFIG_FORMAT, "table"), cols_filter_list
            )

        result_type = self.result_handler_config[consts.CONFIG_TYPE]
        if result_type == "BigQuery":
            project_id = self.result_handler_config[consts.PROJECT_ID]
            table_id = self.result_handler_config[consts.TABLE_ID]
            key_path = self.result_handler_config.get(
                consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH
            )
            if key_path:
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_file(
                        key_path
                    )
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
        source_conn_name,
        target_conn_name,
        table_obj,
        labels,
        threshold,
        format,
        use_random_rows=None,
        random_row_batch_size=None,
        source_client=None,
        target_client=None,
        result_handler_config=None,
        filter_config=None,
        verbose=False,
    ):
        if isinstance(filter_config, dict):
            filter_config = [filter_config]

        """Return a ConfigManager instance with available config."""
        config = {
            consts.CONFIG_TYPE: config_type,
            consts.CONFIG_SOURCE_CONN_NAME: source_conn_name,
            consts.CONFIG_TARGET_CONN_NAME: target_conn_name,
            consts.CONFIG_TABLE_NAME: table_obj[consts.CONFIG_TABLE_NAME],
            consts.CONFIG_SCHEMA_NAME: table_obj[consts.CONFIG_SCHEMA_NAME],
            consts.CONFIG_TARGET_SCHEMA_NAME: table_obj.get(
                consts.CONFIG_TARGET_SCHEMA_NAME, table_obj[consts.CONFIG_SCHEMA_NAME]
            ),
            consts.CONFIG_TARGET_TABLE_NAME: table_obj.get(
                consts.CONFIG_TARGET_TABLE_NAME, table_obj[consts.CONFIG_TABLE_NAME]
            ),
            consts.CONFIG_LABELS: labels,
            consts.CONFIG_THRESHOLD: threshold,
            consts.CONFIG_FORMAT: format,
            consts.CONFIG_RESULT_HANDLER: result_handler_config,
            consts.CONFIG_FILTERS: filter_config,
            consts.CONFIG_USE_RANDOM_ROWS: use_random_rows,
            consts.CONFIG_RANDOM_ROW_BATCH_SIZE: random_row_batch_size,
        }

        return ConfigManager(
            config,
            source_client=source_client,
            target_client=target_client,
            verbose=verbose,
        )

    def build_config_comparison_fields(self, fields, depth=None):
        """Return list of field config objects."""
        field_configs = []
        for field in fields:
            column_config = {
                consts.CONFIG_SOURCE_COLUMN: field.casefold(),
                consts.CONFIG_TARGET_COLUMN: field.casefold(),
                consts.CONFIG_FIELD_ALIAS: field,
                consts.CONFIG_CAST: None,
            }
            field_configs.append(column_config)
        return field_configs

    def build_config_grouped_columns(self, grouped_columns):
        """Return list of grouped column config objects."""
        grouped_column_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()
        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        for column in grouped_columns:

            if column.casefold() not in casefold_source_columns:
                raise ValueError(f"Grouped Column DNE in source: {column}")
            if column.casefold() not in casefold_target_columns:
                raise ValueError(f"Grouped Column DNE in target: {column}")
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

    def build_and_append_pre_agg_calc_config(
        self, column, calc_func, cast_type=None, depth=0
    ):
        """Create calculated field config used as a pre-aggregation step. Appends to calulated fields if does not already exist and returns created config."""
        calculated_config = {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [column],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [column],
            consts.CONFIG_FIELD_ALIAS: f"{calc_func}__{column}",
            consts.CONFIG_TYPE: calc_func,
            consts.CONFIG_DEPTH: depth,
        }

        if calc_func == "cast" and cast_type is not None:
            calculated_config[consts.CONFIG_DEFAULT_CAST] = cast_type
            calculated_config[
                consts.CONFIG_FIELD_ALIAS
            ] = f"{calc_func}_{cast_type}__{column}"

        existing_calc_fields = [
            config[consts.CONFIG_FIELD_ALIAS] for config in self.calculated_fields
        ]

        if calculated_config[consts.CONFIG_FIELD_ALIAS] not in existing_calc_fields:
            self.append_calculated_fields([calculated_config])
        return calculated_config

    def append_pre_agg_calc_field(self, column, agg_type, column_type):
        """Append calculated field for length(string) or epoch_seconds(timestamp) for preprocessing before column validation aggregation."""
        depth, cast_type = 0, None

        if column_type == "string":
            calc_func = "length"

        elif column_type == "timestamp":
            if isinstance(self.source_client, BigQueryClient) or isinstance(
                self.target_client, BigQueryClient
            ):
                calc_func = "cast"
                cast_type = "timestamp"
                pre_calculated_config = self.build_and_append_pre_agg_calc_config(
                    column, calc_func, cast_type, depth
                )
                column = pre_calculated_config[consts.CONFIG_FIELD_ALIAS]
                depth = 1

            calc_func = "epoch_seconds"

        elif column_type == "int32":
            calc_func = "cast"
            cast_type = "int64"

        else:
            raise ValueError(f"Unsupported column type: {column_type}")

        calculated_config = self.build_and_append_pre_agg_calc_config(
            column, calc_func, cast_type, depth
        )

        aggregate_config = {
            consts.CONFIG_SOURCE_COLUMN: f"{calculated_config[consts.CONFIG_FIELD_ALIAS]}",
            consts.CONFIG_TARGET_COLUMN: f"{calculated_config[consts.CONFIG_FIELD_ALIAS]}",
            consts.CONFIG_FIELD_ALIAS: f"{agg_type}__{calculated_config[consts.CONFIG_FIELD_ALIAS]}",
            consts.CONFIG_TYPE: agg_type,
        }

        return aggregate_config

    def build_config_column_aggregates(
        self, agg_type, arg_value, supported_types, cast_to_bigint=False
    ):
        """Return list of aggregate objects of given agg_type."""
        aggregate_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()

        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        if arg_value and supported_types:
            supported_types.append("string")

        allowlist_columns = arg_value or casefold_source_columns
        for column in casefold_source_columns:
            # Get column type and remove precision/scale attributes
            column_type_str = str(source_table[casefold_source_columns[column]].type())
            column_type = column_type_str.split("(")[0]

            if column not in allowlist_columns:
                continue
            elif column not in casefold_target_columns:
                print(
                    f"Skipping {agg_type} on {column} as column is not present in target table"
                )
                continue
            elif supported_types and column_type not in supported_types:
                if self.verbose:
                    print(
                        f"Skipping {agg_type} on {column} due to data type: {column_type}"
                    )
                continue

            if (
                column_type == "string"
                or (cast_to_bigint and column_type == "int32")
                or (
                    column_type == "timestamp"
                    and agg_type
                    in (
                        "sum",
                        "avg",
                        "bit_xor",
                    )  # For timestamps: do not convert to epoch seconds for min/max
                )
            ):
                aggregate_config = self.append_pre_agg_calc_field(
                    column, agg_type, column_type
                )

            else:
                aggregate_config = {
                    consts.CONFIG_SOURCE_COLUMN: casefold_source_columns[column],
                    consts.CONFIG_TARGET_COLUMN: casefold_target_columns[column],
                    consts.CONFIG_FIELD_ALIAS: f"{agg_type}__{column}",
                    consts.CONFIG_TYPE: agg_type,
                }

            aggregate_configs.append(aggregate_config)

        return aggregate_configs

    def build_config_calculated_fields(
        self, reference, calc_type, alias, depth, supported_types, arg_value=None
    ):
        """Returns list of calculated fields"""
        source_table = self.get_source_ibis_calculated_table(depth=depth)
        target_table = self.get_target_ibis_calculated_table(depth=depth)

        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        allowlist_columns = arg_value or casefold_source_columns
        for column in casefold_source_columns:
            column_type_str = str(source_table[casefold_source_columns[column]].type())
            column_type = column_type_str.split("(")[0]
            if column not in allowlist_columns:
                continue
            elif column not in casefold_target_columns:
                logging.info(
                    f"Skipping {calc_type} on {column} as column is not present in target table"
                )
                continue
            elif supported_types and column_type not in supported_types:
                if self.verbose:
                    msg = f"Skipping {calc_type} on {column} due to data type: {column_type}"
                    print(msg)
                continue

        calculated_config = {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: reference,
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: reference,
            consts.CONFIG_FIELD_ALIAS: alias,
            consts.CONFIG_TYPE: calc_type,
            consts.CONFIG_DEPTH: depth,
        }
        return calculated_config

    def _build_dependent_aliases(self, calc_type, col_list=None):
        """This is a utility function for determining the required depth of all fields"""
        order_of_operations = []
        if col_list is None:
            source_table = self.get_source_ibis_calculated_table()
            casefold_source_columns = {
                x.casefold(): str(x) for x in source_table.columns
            }
        else:
            casefold_source_columns = {x.casefold(): str(x) for x in col_list}
        if calc_type == "hash":
            order_of_operations = [
                "cast",
                "ifnull",
                "rstrip",
                "upper",
                "concat",
                "hash",
            ]
        column_aliases = {}
        col_names = []
        for i, calc in enumerate(order_of_operations):
            if i == 0:
                previous_level = [x for x in casefold_source_columns.values()]
            else:
                previous_level = [k for k, v in column_aliases.items() if v == i - 1]
            if calc in ["concat", "hash"]:
                col = {}
                col["reference"] = previous_level
                col["name"] = f"{calc}__all"
                col["calc_type"] = calc
                col["depth"] = i
                name = col["name"]
                # need to capture all aliases at the previous level. probably name concat__all
                column_aliases[name] = i
                col_names.append(col)
            else:
                for (
                    column
                ) in (
                    previous_level
                ):  # this needs to be the previous manifest of columns
                    col = {}
                    col["reference"] = [column]
                    col["name"] = f"{calc}__" + column
                    col["calc_type"] = calc
                    col["depth"] = i
                    name = col["name"]
                    column_aliases[name] = i
                    col_names.append(col)
        return col_names
