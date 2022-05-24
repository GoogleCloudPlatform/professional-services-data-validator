#!/bin/bash
# This script adds two columns 'primary_keys' and 'num_random_rows' to the schema as per PR #372. BigQuery natively supports adding columns to a schema.
# Reference: https://cloud.google.com/bigquery/docs/managing-table-schemas#adding_columns_to_a_tables_schema_definition 

DATASET=pso_data_validator
TABLE=results

# The JSON schema includes two additional columns for primary_keys and num_random_rows
bq update $PROJECT_ID:$DATASET.$TABLE ../../terraform/results_schema.json