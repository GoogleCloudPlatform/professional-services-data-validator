#!/bin/bash

#This script helps to perform an upgrade of column 'STATUS' to 'VALIDATION_STATUS' without losing existing data in Results table.
#It takes backup of the existing biqquery table 'RESULTS' and creates a new table with column name as 'VALIDATION_STATUS'

#PROJECT=pso-kokoro-resources
DEPRECATED_TABLE=pso_data_validator.results_deprecated #--Deprecated results table with column name 'status'
CURRENT_TABLE=pso_data_validator.results #--Current results table
DEST_TABLE=pso_data_validator.results #--Destination results table with new column name 'validation_status'

#To copy the existing table
bq cp $CURRENT_TABLE $DEPRECATED_TABLE

#To create a new table with column as Validation_status
bq query \
--destination_table $DEST_TABLE \
--replace \
--use_legacy_sql=false \
"SELECT * EXCEPT(status), status as validation_status FROM $CURRENT_TABLE"