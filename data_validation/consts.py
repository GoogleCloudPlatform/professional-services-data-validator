
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