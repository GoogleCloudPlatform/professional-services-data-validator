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
CONFIG_SOURCE_CONN = "source_conn"
CONFIG_TARGET_CONN = "target_conn"
CONFIG_TYPE = "type"
CONFIG_SCHEMA_NAME = "schema_name"
CONFIG_TABLE_NAME = "table_name"
CONFIG_TARGET_SCHEMA_NAME = "target_schema_name"
CONFIG_TARGET_TABLE_NAME = "target_table_name"
CONFIG_FIELD_ALIAS = "field_alias"
CONFIG_AGGREGATES = "aggregates"
CONFIG_GROUPED_COLUMNS = "grouped_columns"
CONFIG_SOURCE_COLUMN = "source_column"
CONFIG_TARGET_COLUMN = "target_column"
CONFIG_CAST = "cast"
CONFIG_LIMIT = "limit"
CONFIG_FILTERS = "filters"
CONFIG_FILTER_SOURCE = "source"
CONFIG_FILTER_TARGET = "target"
CONFIG_RESULT_HANDLER = "result_handler"

# Filter Type Options
FILTER_TYPE_CUSTOM = "custom"

# Yaml File Config Fields
YAML_RESULT_HANDLER = "result_handler"
YAML_SOURCE = "source"
YAML_TARGET = "target"
YAML_VALIDATIONS = "validations"

# BigQuery Result Handler Configs
PROJECT_ID = "project_id"
TABLE_ID = "table_id"

# SQL Template Formatting
# TODO: should this be managed in query_builder if that is the only place its used?
COUNT_STAR = "{count_star}"

# Validation metadata
RESULT_TYPE_SOURCE = "source"
RESULT_TYPE_TARGET = "target"
