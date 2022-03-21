# Data Validation Examples
This page describes some basic use cases of the tool.

**PLEASE NOTE:** In below commands, my_bq_conn refers to the connection name for your BigQuery project. We are validating BigQuery tables that are
available in BigQuery public datasets. These examples validate a table against itself for example purposes.  

Also, note that if no aggregation flag is provided, the tool will run a 'COUNT *' as the default aggregation.

#### Simple COUNT(*) on a table
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips
````

#### Run multiple tables
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips,bigquery-public-data.new_york_citibike.citibike_stations
````

#### Store validation config to the file
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips -c citibike.yaml
````
Above command creates a YAML file named citibike.yaml that can be used to run validations in the future. 

**Please note:** When the config-file (-c) option is provided, only the YAML file gets created. The validation doesn’t execute. 
 
 
#### Run validations from a configuration file
````shell script
data-validation run-config -c citibike.yaml
````
Above command executes validations stored in a config file named citibike.yaml. 
 
#### Run COUNT validations for all columns
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips --count '*'
````

#### Run COUNT validations for selected columns
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips --count bikeid,gender
````

#### Run a checksum validation for all rows
````shell script
data-validation validate row -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --primary-keys station_id --hash '*'
````

#### Store results in a BigQuery table
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips --count tripduration,start_station_name -bqrh $YOUR_PROJECT_ID.pso_data_validator.results
````
Please replace $YOUR_PROJECT_ID with the correct project-id where you created your results datasets as mentioned in the [installation](installation.md#setup) section.


#### Query results from a BigQuery results table

````shell script
SELECT
  run_id,
  validation_name,
  validation_type,
  group_by_columns,
  source_table_name,
  source_agg_value,
  target_agg_value,
  pct_difference,
  pct_threshold,
  difference,
  start_time
FROM
  `pso_data_validator.results`
ORDER BY
  start_time DESC
````

#### Run a single column GroupBy validation
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips --grouped-columns bikeid
````

#### Run a multi-column GroupBy validation
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips --grouped-columns bikeid,usertype
````

#### Apply single aggregation on a single field
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --sum num_bikes_available
````


#### Apply single aggregation on multiple fields
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --sum num_bikes_available,num_docks_available
````

#### Apply different aggregations on multiple fields
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --sum num_bikes_available,num_docks_available --avg num_bikes_disabled,num_docks_disabled
````

#### Apply different aggregations on multiple fields and apply GroupBy
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --grouped-columns region_id --sum num_bikes_available,num_docks_available --avg num_bikes_disabled,num_docks_disabled
````

#### Apply filters
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --grouped-columns region_id --sum num_bikes_available,num_docks_available --filters 'region_id=71' -bqrh $YOUR_PROJECT_ID.pso_data_validator.results
````

#### Apply labels
````shell script
data-validation validate column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips --count tripduration,start_station_name -l tag=test-run,owner=name
````

#### Run a schema validation
````shell script
data-validation validate schema -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips -bqrh $YOUR_PROJECT_ID.pso_data_validator.results
````

#### Run validation on a file
````shell script
# Additional dependencies needed for GCS files
pip install gcsfs
pip install fsspec

data-validation connections add --connection-name file_conn FileSystem --table-name $FILE_NAME --file-path gs://path/to/file --file-type csv
data-validation connections add --connection-name my_bq_conn BigQuery --project-id $YOUR_PROJECT_ID

# Validate GCS CSV file with BigQuery table
data-validation validate column -sc file_conn -tc my_bq_conn -tbls $FILE_NAME=$YOUR_PROJECT_ID.dataset.table --count $COLUMN
````

#### Run custom SQL 
````shell script
data-validation query
  --conn connection-name The named connection to be queried.
  --query, -q The Raw query to run against the supplied connection
````

#### Sample YAML file (Grouped Column validation)
```yaml
result_handler:
  project_id: my-project-id
  table_id: pso_data_validator.results
  type: BigQuery
  google_service_account_key_path: path/to/sa.json
source: my_bq_conn
target: my_bq_conn
validations:
- aggregates:
  - field_alias: count
    source_column: null
    target_column: null
    type: count
  - field_alias: sum__num_bikes_available
    source_column: num_bikes_available
    target_column: num_bikes_available
    type: sum
    cast: float64
  - field_alias: sum__num_docks_available
    source_column: num_docks_available
    target_column: num_docks_available
    type: sum
  filters:
  - source: region_id=71
    target: region_id=71
    type: custom
  grouped_columns:
  - cast: null
    field_alias: region_id
    source_column: region_id
    target_column: region_id
  labels:
  - !!python/tuple
    - description
    - test
  schema_name: bigquery-public-data.new_york_citibike
  table_name: citibike_stations
  target_schema_name: bigquery-public-data.new_york_citibike
  target_table_name: citibike_stations
  threshold: 0.0
  type: Column
  ```

#### Run a custom query validation
````shell script
data-validation validate custom-query --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations
````

#### Run a custom query validation with sum aggregation 
````shell script
data-validation validate custom-query --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --sum num_bikes_available
````

#### Run a custom query validation with max aggregation 
````shell script
data-validation validate custom-query --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --max num_bikes_available
````

Please replace source_query.sql and target_query.sql with the correct files containing sql query for source and target database respectively.