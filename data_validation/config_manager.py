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
import string
import random
from typing import TYPE_CHECKING, Optional, Union, List, Dict

import google.oauth2.service_account
import ibis.expr.datatypes as dt
import yaml

from data_validation import clients, consts, gcs_helper, state_manager
from data_validation.result_handlers.bigquery import BigQueryResultHandler
from data_validation.result_handlers.text import TextResultHandler
from data_validation.validation_builder import ValidationBuilder

if TYPE_CHECKING:
    import ibis.expr.types.TableExpr


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
        self._comparison_max_col_length = None

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

    def close_client_connections(self):
        """Attempt to clean up any source/target connections, based on the client types.

        Not all clients are covered here, we at least have Oracle and PostgreSQL for which we
        have seen connections being accumulated.
        https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1195
        """
        try:
            if self.source_client and self.source_client.name in ("oracle", "postgres"):
                self.source_client.con.dispose()
            if self.target_client and self.target_client.name in ("oracle", "postgres"):
                self.target_client.con.dispose()
        except Exception as exc:
            # No need to reraise, we can silently fail if exiting throws up an issue.
            logging.warning("Exception closing connections: %s", str(exc))

    @property
    def validation_type(self):
        """Return string validation type (Column|Schema)."""
        return self._config[consts.CONFIG_TYPE]

    def use_random_rows(self):
        """Return if the validation should use a random row filter."""
        return self._config.get(consts.CONFIG_USE_RANDOM_ROWS) or False

    def random_row_batch_size(self):
        """Return batch size for random row filter."""
        return int(
            self._config.get(consts.CONFIG_RANDOM_ROW_BATCH_SIZE)
            or consts.DEFAULT_NUM_RANDOM_ROWS
        )

    def get_random_row_batch_size(self):
        """Return number of random rows or None."""
        return self.random_row_batch_size() if self.use_random_rows() else None

    def trim_string_pks(self):
        """Return if the validation should trim string primary keys."""
        return self._config.get(consts.CONFIG_TRIM_STRING_PKS) or False

    def case_insensitive_match(self):
        """Return if the validation should perform a case insensitive match."""
        return self._config.get(consts.CONFIG_CASE_INSENSITIVE_MATCH) or False

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
    def concat(self):
        """Return field from Config"""
        return self._config.get(consts.CONFIG_ROW_CONCAT, [])

    @property
    def hash(self):
        """Return field from Config"""
        return self._config.get(consts.CONFIG_ROW_HASH, [])

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
        if self.source_table and self.source_schema:
            return self.source_schema + "." + self.source_table
        elif self.source_table:
            return self.source_table
        else:
            return f"custom.{''.join(random.choice(string.ascii_lowercase) for _ in range(5))}"

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

    @property
    def source_query(self):
        return self._config.get(consts.CONFIG_SOURCE_QUERY, None)

    def append_source_query(self, source_query):
        self._config["source_query"] = source_query

    @property
    def target_query(self):
        return self._config.get(consts.CONFIG_TARGET_QUERY, None)

    def append_target_query(self, target_query):
        self._config["target_query"] = target_query

    @property
    def exclusion_columns(self):
        """Return the exclusion columns from Config"""
        return self._config.get(consts.CONFIG_EXCLUSION_COLUMNS, [])

    @property
    def allow_list(self):
        """Return the allow_list from Config"""
        return self._config.get(consts.CONFIG_ALLOW_LIST, "")

    @property
    def filter_status(self):
        """Return filter status list from Config"""
        return self._config.get(consts.CONFIG_FILTER_STATUS, None)

    def append_exclusion_columns(self, column_configs):
        """Append exclusion columns to existing config."""
        self._config[consts.CONFIG_EXCLUSION_COLUMNS] = (
            self.exclusion_columns + column_configs
        )

    def append_allow_list(
        self, allow_list: Union[str, None], allow_list_file: Union[str, None]
    ):
        """Append datatype allow_list to existing config."""
        full_allow_list = []
        if allow_list:
            allow_list = allow_list.replace(" ", "")
            full_allow_list.append(allow_list)
        if allow_list_file:
            try:
                allow_list_yaml = gcs_helper.read_file(allow_list_file)
            except FileNotFoundError as e:
                raise ValueError(
                    "Cannot locate --allow-list-file: {allow_list_file}"
                ) from e
            allow_list_dict = yaml.safe_load(allow_list_yaml)
            full_allow_list.append(
                ",".join([f"{_[0]}:{_[1]}" for _ in allow_list_dict.items()])
            )

        self._config[consts.CONFIG_ALLOW_LIST] = ",".join(full_allow_list)

    def get_source_ibis_table(self):
        """Return IbisTable from source."""
        if not hasattr(self, "_source_ibis_table"):
            self._source_ibis_table = clients.get_ibis_table(
                self.source_client, self.source_schema, self.source_table
            )
        return self._source_ibis_table

    def get_source_ibis_table_from_query(self):
        """Return IbisTable from source."""
        if not hasattr(self, "_source_ibis_table"):
            self._source_ibis_table = clients.get_ibis_query(
                self.source_client, self.source_query
            )
        return self._source_ibis_table

    def get_source_ibis_calculated_table(self, depth=None):
        """Return mutated IbisTable from source
        depth: Int the depth of subquery requested"""
        if self.validation_type == consts.CUSTOM_QUERY:
            table = self.get_source_ibis_table_from_query()
        else:
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

    def get_target_ibis_table_from_query(self):
        """Return IbisTable from source."""
        if not hasattr(self, "_target_ibis_table"):
            self._target_ibis_table = clients.get_ibis_query(
                self.target_client, self.target_query
            )
        return self._target_ibis_table

    def get_target_ibis_calculated_table(self, depth=None):
        """Return mutated IbisTable from target
        n: Int the depth of subquery requested"""
        if self.validation_type == consts.CUSTOM_QUERY:
            table = self.get_target_ibis_table_from_query()
        else:
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
            # handler that display results either to output or in a file
            return TextResultHandler(
                self._config.get(consts.CONFIG_FORMAT, "table"),
                self.filter_status,
                cols_filter_list,
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
                project_id,
                self.filter_status,
                table_id=table_id,
                credentials=credentials,
                text_format=self._config.get(consts.CONFIG_FORMAT, "table"),
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
        filter_status=None,
        trim_string_pks=None,
        case_insensitive_match=None,
        concat=None,
        hash=None,
        verbose=False,
    ):
        if isinstance(filter_config, dict):
            filter_config = [filter_config]

        """Return a ConfigManager instance with available config."""
        config = {
            consts.CONFIG_TYPE: config_type,
            consts.CONFIG_SOURCE_CONN_NAME: source_conn_name,
            consts.CONFIG_TARGET_CONN_NAME: target_conn_name,
            consts.CONFIG_TABLE_NAME: table_obj.get(consts.CONFIG_TABLE_NAME, None),
            consts.CONFIG_SCHEMA_NAME: table_obj.get(consts.CONFIG_SCHEMA_NAME, None),
            consts.CONFIG_TARGET_SCHEMA_NAME: table_obj.get(
                consts.CONFIG_TARGET_SCHEMA_NAME,
                table_obj.get(consts.CONFIG_SCHEMA_NAME, None),
            ),
            consts.CONFIG_TARGET_TABLE_NAME: table_obj.get(
                consts.CONFIG_TARGET_TABLE_NAME,
                table_obj.get(consts.CONFIG_TABLE_NAME, None),
            ),
            consts.CONFIG_LABELS: labels,
            consts.CONFIG_THRESHOLD: threshold,
            consts.CONFIG_FORMAT: format,
            consts.CONFIG_RESULT_HANDLER: result_handler_config,
            consts.CONFIG_FILTERS: filter_config,
            consts.CONFIG_USE_RANDOM_ROWS: use_random_rows,
            consts.CONFIG_RANDOM_ROW_BATCH_SIZE: random_row_batch_size,
            consts.CONFIG_FILTER_STATUS: filter_status,
            consts.CONFIG_TRIM_STRING_PKS: trim_string_pks,
            consts.CONFIG_CASE_INSENSITIVE_MATCH: case_insensitive_match,
            consts.CONFIG_ROW_CONCAT: concat,
            consts.CONFIG_ROW_HASH: hash,
        }

        return ConfigManager(
            config,
            source_client=source_client,
            target_client=target_client,
            verbose=verbose,
        )

    def add_rstrip_to_comp_fields(self, comparison_fields: List[str]) -> List[str]:
        """As per #1190, add an rstrip calculated field for Teradata string comparison fields.

        Parameters:
            comparison_fields: List[str] of comparison field columns
        Returns:
            comp_fields_with_aliases: List[str] of comparison field columns with rstrip aliases
        """
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()
        source_table_schema = {k: v for k, v in source_table.schema().items()}
        target_table_schema = {k: v for k, v in target_table.schema().items()}
        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        comp_fields_with_aliases = []
        calculated_configs = []
        for field in comparison_fields:
            if field.casefold() not in casefold_source_columns:
                raise ValueError(f"Column DNE in source: {field}")
            if field.casefold() not in casefold_target_columns:
                raise ValueError(f"Column DNE in target: {field}")

            source_ibis_type = source_table[
                casefold_source_columns[field.casefold()]
            ].type()
            target_ibis_type = target_table[
                casefold_target_columns[field.casefold()]
            ].type()

            if (
                source_ibis_type.is_string() or target_ibis_type.is_string()
            ) and self._comp_field_cast(
                # Do not add rstrip if the column is a bool hiding in a string.
                source_table_schema,
                target_table_schema,
                field,
            ) != "bool":
                logging.info(
                    f"Adding rtrim() to string comparison field `{field.casefold()}` due to Teradata CHAR padding."
                )
                alias = f"rstrip__{field.casefold()}"
                calculated_configs.append(
                    self.build_config_calculated_fields(
                        [casefold_source_columns[field.casefold()]],
                        [casefold_target_columns[field.casefold()]],
                        "rstrip",
                        alias,
                        0,
                    )
                )
                comp_fields_with_aliases.append(alias)
            else:
                comp_fields_with_aliases.append(field)

        self.append_calculated_fields(calculated_configs)
        return comp_fields_with_aliases

    def _comp_field_cast(
        self, source_table_schema: dict, target_table_schema: dict, field: str
    ) -> str:
        # We check with .get() below because sometimes field is a computed name
        # like "concat__all" which is not in the real table.
        if (
            source_table_schema.get(field, None)
            and isinstance(source_table_schema[field], dt.Boolean)
        ) or (
            target_table_schema.get(field, None)
            and isinstance(target_table_schema[field], dt.Boolean)
        ):
            return "bool"
        return None

    def build_config_comparison_fields(self, fields, depth=None):
        """Return list of field config objects."""
        field_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()
        source_table_schema = {k: v for k, v in source_table.schema().items()}
        target_table_schema = {k: v for k, v in target_table.schema().items()}
        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        for field in fields:
            cast_type = self._comp_field_cast(
                source_table_schema, target_table_schema, field
            )
            column_config = {
                consts.CONFIG_SOURCE_COLUMN: casefold_source_columns.get(
                    field.casefold(), field
                ),
                consts.CONFIG_TARGET_COLUMN: casefold_target_columns.get(
                    field.casefold(), field
                ),
                consts.CONFIG_FIELD_ALIAS: field,
                consts.CONFIG_CAST: cast_type,
            }
            field_configs.append(column_config)
        return field_configs

    def build_column_configs(self, columns):
        """Return list of column config objects."""
        column_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()
        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        for column in columns:
            if column.casefold() not in casefold_source_columns:
                raise ValueError(f"Column DNE in source: {column}")
            if column.casefold() not in casefold_target_columns:
                raise ValueError(f"Column DNE in target: {column}")

            source_ibis_type = source_table[
                casefold_source_columns[column.casefold()]
            ].type()
            target_ibis_type = target_table[
                casefold_target_columns[column.casefold()]
            ].type()
            cast_type = (
                "string"
                if self._key_column_needs_casting_to_string(
                    source_ibis_type, target_ibis_type
                )
                else None
            )

            column_config = {
                consts.CONFIG_SOURCE_COLUMN: casefold_source_columns[column.casefold()],
                consts.CONFIG_TARGET_COLUMN: casefold_target_columns[column.casefold()],
                consts.CONFIG_FIELD_ALIAS: column,
                consts.CONFIG_CAST: cast_type,
            }
            column_configs.append(column_config)

        return column_configs

    def build_config_count_aggregate(self):
        """Return dict aggregate for COUNT(*)."""
        aggregate_config = {
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
            consts.CONFIG_TYPE: "count",
        }

        return aggregate_config

    def _prefix_calc_col_name(
        self, column_name: str, prefix: str, column_number: int
    ) -> str:
        """Prefix a column name but protect final string from overflowing SQL engine identifier length limit."""
        new_name = f"{prefix}__{column_name}"
        if len(new_name) > self._get_comparison_max_col_length():
            # Use an abstract name for the calculated column to avoid composing invalid SQL.
            new_name = f"{prefix}__dvt_calc_col_{column_number}"
        return new_name

    def build_and_append_pre_agg_calc_config(
        self,
        source_column,
        target_column,
        calc_func,
        column_position,
        cast_type=None,
        depth=0,
    ):
        """Create calculated field config used as a pre-aggregation step. Appends to calculated fields if does not already exist and returns created config."""
        calculated_config = {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [source_column],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [target_column],
            consts.CONFIG_FIELD_ALIAS: self._prefix_calc_col_name(
                source_column, calc_func, column_position
            ),
            consts.CONFIG_TYPE: calc_func,
            consts.CONFIG_DEPTH: depth,
        }

        if calc_func == "cast" and cast_type is not None:
            calculated_config[consts.CONFIG_DEFAULT_CAST] = cast_type
            calculated_config[consts.CONFIG_FIELD_ALIAS] = self._prefix_calc_col_name(
                source_column, f"{calc_func}_{cast_type}", column_position
            )

        existing_calc_fields = [
            config[consts.CONFIG_FIELD_ALIAS] for config in self.calculated_fields
        ]

        if calculated_config[consts.CONFIG_FIELD_ALIAS] not in existing_calc_fields:
            self.append_calculated_fields([calculated_config])
        return calculated_config

    def append_pre_agg_calc_field(
        self, source_column, target_column, agg_type, column_type, column_position
    ) -> dict:
        """Append calculated field for length(string | binary) or epoch_seconds(timestamp) for preprocessing before column validation aggregation."""
        depth, cast_type = 0, None
        if column_type in ["string", "!string"]:
            calc_func = "length"

        elif column_type in ["binary", "!binary"]:
            calc_func = "byte_length"

        elif column_type in ["timestamp", "!timestamp", "date", "!date"]:
            if (
                self.source_client.name == "bigquery"
                or self.target_client.name == "bigquery"
            ):
                calc_func = "cast"
                cast_type = "timestamp"
                pre_calculated_config = self.build_and_append_pre_agg_calc_config(
                    source_column,
                    target_column,
                    calc_func,
                    column_position,
                    cast_type,
                    depth,
                )
                source_column = target_column = pre_calculated_config[
                    consts.CONFIG_FIELD_ALIAS
                ]
                depth = 1

            calc_func = "epoch_seconds"

        elif column_type == "int32" or column_type == "!int32":
            calc_func = "cast"
            cast_type = "int64"

        else:
            raise ValueError(f"Unsupported column type: {column_type}")

        calculated_config = self.build_and_append_pre_agg_calc_config(
            source_column, target_column, calc_func, column_position, cast_type, depth
        )

        aggregate_config = {
            consts.CONFIG_SOURCE_COLUMN: f"{calculated_config[consts.CONFIG_FIELD_ALIAS]}",
            consts.CONFIG_TARGET_COLUMN: f"{calculated_config[consts.CONFIG_FIELD_ALIAS]}",
            consts.CONFIG_FIELD_ALIAS: self._prefix_calc_col_name(
                calculated_config[consts.CONFIG_FIELD_ALIAS],
                f"{agg_type}",
                column_position,
            ),
            consts.CONFIG_TYPE: agg_type,
        }

        return aggregate_config

    def _decimal_column_too_big_for_pandas(
        self,
        source_column_ibis_type: dt.DataType,
        target_column_ibis_type: dt.DataType,
    ) -> bool:
        """Identifies Decimal columns that will cause problems in a Pandas Dataframe, i.e.
        is of greater precision than a 64bit int/real can hold."""
        return bool(
            (
                isinstance(source_column_ibis_type, dt.Decimal)
                and (
                    source_column_ibis_type.precision is None
                    or source_column_ibis_type.precision > 18
                )
            )
            and (
                isinstance(target_column_ibis_type, dt.Decimal)
                and (
                    target_column_ibis_type.precision is None
                    or target_column_ibis_type.precision > 18
                )
            )
        )

    def _key_column_needs_casting_to_string(
        self,
        source_column_ibis_type: dt.DataType,
        target_column_ibis_type: dt.DataType,
    ) -> bool:
        return bool(
            self._decimal_column_too_big_for_pandas(
                source_column_ibis_type, target_column_ibis_type
            )
            or isinstance(source_column_ibis_type, dt.Binary)
            or isinstance(target_column_ibis_type, dt.Binary)
        )

    def build_config_column_aggregates(
        self, agg_type, arg_value, exclude_cols, supported_types, cast_to_bigint=False
    ):
        """Return list of aggregate objects of given agg_type."""

        def require_pre_agg_calc_field(
            column_type: str,
            target_column_type: str,
            agg_type: str,
            cast_to_bigint: bool,
        ) -> bool:
            if column_type in ["string", "!string"] and target_column_type in [
                "string",
                "!string",
            ]:
                return True
            elif column_type in ["binary", "!binary"]:
                if agg_type == "count":
                    # Oracle BLOB is invalid for use with SQL COUNT function.
                    return bool(
                        self.source_client.name == "oracle"
                        or self.target_client.name == "oracle"
                    )
                else:
                    # Convert to length for any min/max/sum on binary columns.
                    return True
            elif cast_to_bigint and column_type in ["int32", "!int32"]:
                return True
            elif column_type in [
                "timestamp",
                "!timestamp",
                "date",
                "!date",
            ] and agg_type in (
                "sum",
                "avg",
                "bit_xor",
            ):
                # For timestamps: do not convert to epoch seconds for min/max
                return True
            return False

        aggregate_configs = []
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()

        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        if arg_value:
            arg_value = [x.casefold() for x in arg_value]
            if exclude_cols:
                included_cols = [
                    column
                    for column in casefold_source_columns
                    if column not in arg_value
                ]
                arg_value = included_cols

            if supported_types:
                # This mutates external supported_types, making it local as part of adding more values.
                supported_types = supported_types + [
                    "string",
                    "!string",
                    "timestamp",
                    "!timestamp",
                    "date",
                    "!date",
                    "binary",
                    "!binary",
                ]
        else:
            if exclude_cols:
                raise ValueError(
                    "Exclude columns flag cannot be present with '*' column aggregation"
                )

        allowlist_columns = arg_value or casefold_source_columns
        for column_position, column in enumerate(casefold_source_columns):
            # Get column type and remove precision/scale attributes
            source_column_ibis_type = source_table[
                casefold_source_columns[column]
            ].type()
            column_type = str(source_column_ibis_type).split("(")[0]
            target_column_ibis_type = target_table[
                casefold_target_columns[column]
            ].type()
            target_column_type = str(target_column_ibis_type).split("(")[0]

            if column not in allowlist_columns:
                continue
            elif column not in casefold_target_columns:
                logging.warning(
                    f"Skipping {agg_type} on {column} as column is not present in target table"
                )
                continue
            elif supported_types and (
                column_type not in supported_types
                or target_column_type not in supported_types
            ):
                if self.verbose:
                    logging.info(
                        f"Skipping {agg_type} on {column} due to data type: {column_type}"
                    )
                continue

            if require_pre_agg_calc_field(
                column_type, target_column_type, agg_type, cast_to_bigint
            ):
                aggregate_config = self.append_pre_agg_calc_field(
                    casefold_source_columns[column],
                    casefold_target_columns[column],
                    agg_type,
                    column_type,
                    column_position,
                )
            else:
                aggregate_config = {
                    consts.CONFIG_SOURCE_COLUMN: casefold_source_columns[column],
                    consts.CONFIG_TARGET_COLUMN: casefold_target_columns[column],
                    consts.CONFIG_FIELD_ALIAS: self._prefix_calc_col_name(
                        column, f"{agg_type}", column_position
                    ),
                    consts.CONFIG_TYPE: agg_type,
                }
                if self._decimal_column_too_big_for_pandas(
                    source_column_ibis_type, target_column_ibis_type
                ):
                    aggregate_config[consts.CONFIG_CAST] = "string"

            aggregate_configs.append(aggregate_config)

        return aggregate_configs

    def build_config_calculated_fields(
        self,
        source_reference: list,
        target_reference: list,
        calc_type: str,
        alias: str,
        depth: int,
        custom_params: Optional[dict] = None,
    ) -> dict:
        """Returns list of calculated fields"""
        calculated_config = {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: source_reference,
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: target_reference,
            consts.CONFIG_FIELD_ALIAS: alias,
            consts.CONFIG_TYPE: calc_type,
            consts.CONFIG_DEPTH: depth,
        }

        if calc_type == consts.CONFIG_CUSTOM and custom_params:
            calculated_config.update(custom_params)
        elif calc_type == consts.CONFIG_CAST and custom_params:
            calculated_config[consts.CONFIG_DEFAULT_CAST] = custom_params

        return calculated_config

    def _get_comparison_max_col_length(self) -> int:
        if not self._comparison_max_col_length:
            self._comparison_max_col_length = min(
                [
                    clients.get_max_column_length(self.source_client),
                    clients.get_max_column_length(self.target_client),
                ]
            )
        return self._comparison_max_col_length

    def _strftime_format(
        self, column_type: Union[dt.Date, dt.Timestamp], client
    ) -> str:
        if column_type.is_timestamp():
            return "%Y-%m-%d %H:%M:%S"
        if clients.is_oracle_client(client):
            # Oracle DATE is a DateTime
            return "%Y-%m-%d %H:%M:%S"
        return "%Y-%m-%d"

    def _apply_base_cast_overrides(
        self,
        source_column: str,
        target_column: str,
        col_config: dict,
        source_table: "ibis.expr.types.TableExpr",
        target_table: "ibis.expr.types.TableExpr",
    ) -> dict:
        """Mutates col_config to contain any overrides. Also returns col_config for convenience."""
        if col_config["calc_type"] != "cast":
            return col_config

        source_table_schema = {k: v for k, v in source_table.schema().items()}
        target_table_schema = {k: v for k, v in target_table.schema().items()}

        if isinstance(
            source_table_schema[source_column], (dt.Date, dt.Timestamp)
        ) and isinstance(target_table_schema[target_column], (dt.Date, dt.Timestamp)):
            # Use strftime rather than cast for temporal comparisons.
            # Pick the most permissive format across the two engines.
            # For example Date -> Timestamp should format both source and target as Date.
            fmt = min(
                [
                    self._strftime_format(
                        source_table_schema[source_column], self.source_client
                    ),
                    self._strftime_format(
                        source_table_schema[source_column], self.target_client
                    ),
                ],
                key=len,
            )
            col_config["calc_type"] = consts.CONFIG_CUSTOM
            custom_params = {
                "calc_params": {
                    consts.CONFIG_CUSTOM_IBIS_EXPR: "ibis.expr.types.TemporalValue.strftime",
                    consts.CONFIG_CUSTOM_PARAMS: [
                        {consts.CONFIG_CUSTOM_PARAM_FORMAT_STR: fmt}
                    ],
                }
            }
            col_config.update(custom_params)
        elif isinstance(source_table_schema[source_column], dt.Boolean) or isinstance(
            target_table_schema[target_column], dt.Boolean
        ):
            custom_params = {"calc_params": consts.CONFIG_CAST_BOOL_STRING}
            col_config.update(custom_params)

        return col_config

    def _get_order_of_operations(self, calc_type: str) -> List[str]:
        """Return order of operations for row validation."""
        order_of_operations = ["cast", "ifnull", "rstrip"]
        if self.case_insensitive_match():
            order_of_operations.append("upper")
        if calc_type == "hash":
            order_of_operations.extend(["concat", "hash"])
        elif calc_type == "concat":
            order_of_operations.append("concat")

        return order_of_operations

    def _filter_columns_by_column_list(
        self, casefold_columns: list, col_list: list, exclude_cols: bool
    ) -> list:
        if col_list:
            filter_list = [_.casefold() for _ in col_list]
            if exclude_cols:
                # Exclude columns based on col_list if provided
                casefold_columns = {
                    k: v for (k, v) in casefold_columns.items() if k not in filter_list
                }
            else:
                # Include columns based on col_list if provided
                casefold_columns = {
                    k: v for (k, v) in casefold_columns.items() if k in filter_list
                }
        elif exclude_cols:
            raise ValueError(
                "Exclude columns flag cannot be present with column list '*'"
            )
        return casefold_columns

    def build_dependent_aliases(
        self, calc_type: str, col_list=None, exclude_cols=False
    ) -> List[Dict]:
        """This is a utility function for determining the required depth of all fields"""
        source_table = self.get_source_ibis_calculated_table()
        target_table = self.get_target_ibis_calculated_table()

        casefold_source_columns = {x.casefold(): str(x) for x in source_table.columns}
        casefold_target_columns = {x.casefold(): str(x) for x in target_table.columns}

        casefold_source_columns = self._filter_columns_by_column_list(
            casefold_source_columns, col_list, exclude_cols
        )
        casefold_target_columns = self._filter_columns_by_column_list(
            casefold_target_columns, col_list, exclude_cols
        )

        column_aliases = {}
        col_names = []
        for i, calc in enumerate(self._get_order_of_operations(calc_type)):
            if i == 0:
                previous_level = [x for x in casefold_source_columns.keys()]
            else:
                previous_level = [k for k, v in column_aliases.items() if v == i - 1]
            if calc in ["concat", "hash"]:
                col = {}
                col["source_reference"] = previous_level
                col["target_reference"] = previous_level
                col["name"] = f"{calc}__all"
                col["calc_type"] = calc
                col["depth"] = i
                name = col["name"]
                # need to capture all aliases at the previous level. probably name concat__all
                column_aliases[name] = i
                col_names.append(col)
            else:
                # This needs to be the previous manifest of columns
                for j, column in enumerate(previous_level):
                    col = {}
                    col["source_reference"] = [column]
                    col["target_reference"] = [column]
                    col["name"] = self._prefix_calc_col_name(column, calc, j)
                    col["calc_type"] = calc
                    col["depth"] = i

                    if i == 0:
                        # If depth 0, get raw column name with correct casing
                        source_column = casefold_source_columns[column]
                        target_column = casefold_target_columns[column]
                        col["source_reference"] = [source_column]
                        col["target_reference"] = [target_column]
                        # If we are casting the base column (i == 0) then apply any
                        # datatype specific overrides.
                        col = self._apply_base_cast_overrides(
                            source_column,
                            target_column,
                            col,
                            source_table,
                            target_table,
                        )

                    name = col["name"]
                    column_aliases[name] = i
                    col_names.append(col)
        return col_names

    def build_comp_fields(self, col_list: list, exclude_cols: bool = False) -> list:
        """This is a utility function processing comp-fields values like we do for hash/concat."""
        source_table = self.get_source_ibis_calculated_table()
        casefold_source_columns = {_.casefold(): str(_) for _ in source_table.columns}

        casefold_source_columns = self._filter_columns_by_column_list(
            casefold_source_columns, col_list, exclude_cols
        )

        return casefold_source_columns
