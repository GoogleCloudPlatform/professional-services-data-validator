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


# Generic Config Used in Submission
SOURCE_TYPE = "source_type"
CONFIG = "config"
SCHEMA_NAME = "schema_name"
TABLE_NAME = "table_name"
PARTITION_COLUMN = "partition_column"

# Configurations Used in SQL Generation
DEFAULT_PARTITION_KEY = "partition_key"
PARTITION_COLUMN_SQL = "partition_column_sql"
GROUP_COLUMN_SQL = "grouped_column_sql"

# SQL Template Formatting
# TODO: should this be managed in query_builder if that is the only place its used?
COUNT_STAR = "{count_star}"

# Data Cleanup Consts
INPUT_SUFFIX = "_inp"
OUTPUT_SUFFIX = "_out"