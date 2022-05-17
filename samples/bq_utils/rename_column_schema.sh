#!/bin/bash
# This script helps update the column name of the DVT results table from 'status' to 'validation_status' without losing existing data in the results table as per PR #420
# It makes a copy of the existing BigQuery results table and creates a new table with the updated schema. The new table keeps the original table name to avoid downstream errors.

DEPRECATED_TABLE=pso_data_validator.results_deprecated # Deprecated results table with column name 'status'
CURRENT_TABLE=pso_data_validator.results # Current results table
DEST_TABLE=pso_data_validator.results # Destination results table with new column name 'validation_status'

# To copy the existing table
bq cp $CURRENT_TABLE $DEPRECATED_TABLE

# To create a new table with column as Validation_status
bq query \
--destination_table $DEST_TABLE \
--clustering_fields validation_name,run_id \
--time_partitioning_field start_time \
--replace \
--use_legacy_sql=false \
"SELECT * EXCEPT(status), status as validation_status FROM $CURRENT_TABLE"