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


# Configuration Fields
SOURCE_TYPE = "source_type"
CONFIG = "config"
CONFIG_SOURCE_CONN_NAME = "source_conn_name"
CONFIG_TARGET_CONN_NAME = "target_conn_name"
CONFIG_SOURCE_CONN = "source_conn"
CONFIG_TARGET_CONN = "target_conn"
CONFIG_TYPE = "type"
CONFIG_SCHEMA_NAME = "schema_name"
CONFIG_TABLE_NAME = "table_name"
CONFIG_TARGET_SCHEMA_NAME = "target_schema_name"
CONFIG_TARGET_TABLE_NAME = "target_table_name"
CONFIG_LABELS = "labels"
CONFIG_FIELD_ALIAS = "field_alias"
CONFIG_AGGREGATES = "aggregates"
CONFIG_CALCULATED_FIELDS = "calculated_fields"
CONFIG_GROUPED_COLUMNS = "grouped_columns"
CONFIG_CALCULATED_SOURCE_COLUMNS = "source_calculated_columns"
CONFIG_CALCULATED_TARGET_COLUMNS = "target_calculated_columns"
CONFIG_PRIMARY_KEYS = "primary_keys"
CONFIG_SOURCE_COLUMN = "source_column"
CONFIG_TARGET_COLUMN = "target_column"
CONFIG_THRESHOLD = "threshold"
CONFIG_CAST = "cast"
CONFIG_FORMAT = "format"
CONFIG_LIMIT = "limit"
CONFIG_FILTERS = "filters"
CONFIG_FILTER_SOURCE = "source"
CONFIG_FILTER_TARGET = "target"
CONFIG_MAX_RECURSIVE_QUERY_SIZE = "max_recursive_query_size"

CONFIG_FILTER_SOURCE_COLUMN = "source_column"
CONFIG_FILTER_SOURCE_VALUE = "source_value"
CONFIG_FILTER_TARGET_COLUMN = "target_column"
CONFIG_FILTER_TARGET_VALUE = "target_value"

CONFIG_RESULT_HANDLER = "result_handler"

CONFIG_TYPE_COUNT = "count"
CONFIG_TYPE_SUM = "sum"

# Filter Type Options
FILTER_TYPE_CUSTOM = "custom"
FILTER_TYPE_EQUALS = "equals"

# Validation Types
COLUMN_VALIDATION = "Column"
GROUPED_COLUMN_VALIDATION = "GroupedColumn"
ROW_VALIDATION = "Row"
SCHEMA_VALIDATION = "Schema"

# State Manager Fields
DEFAULT_ENV_DIRECTORY = "~/.config/google-pso-data-validator/"
ENV_DIRECTORY_VAR = "PSO_DV_CONFIG_HOME"

# Yaml File Config Fields
YAML_RESULT_HANDLER = "result_handler"
YAML_SOURCE = "source"
YAML_TARGET = "target"
YAML_VALIDATIONS = "validations"

# BigQuery Result Handler Configs
PROJECT_ID = "project_id"
TABLE_ID = "table_id"
GOOGLE_SERVICE_ACCOUNT_KEY_PATH = "google_service_account_key_path"

# BigQuery Output Table Fields
VALIDATION_TYPE = "validation_type"
AGGREGATION_TYPE = "aggregation_type"
GROUP_BY_COLUMNS = "group_by_columns"

SOURCE_TABLE_NAME = "source_table_name"
SOURCE_COLUMN_NAME = "source_column_name"
SOURCE_AGG_VALUE = "source_agg_value"

TARGET_TABLE_NAME = "target_table_name"
TARGET_COLUMN_NAME = "target_column_name"
TARGET_AGG_VALUE = "target_agg_value"

VALIDATION_STATUS = "status"

# SQL Template Formatting
# TODO: should this be managed in query_builder if that is the only place its used?
COUNT_STAR = "{count_star}"

# Validation metadata
RESULT_TYPE_SOURCE = "source"
RESULT_TYPE_TARGET = "target"

# Ibis Object Info
NUMERIC_DATA_TYPES = ["float64", "int32", "int64", "decimal"]

FORMAT_TYPES = ["csv", "json", "table", "text"]

# Text Result Handler column filter list
COLUMN_FILTER_LIST = [
    "aggregation_type",
    "difference",
    "end_time",
    "labels",
    "pct_threshold",
    "run_id",
    "source_agg_value",
    "start_time",
    "target_agg_value",
]
