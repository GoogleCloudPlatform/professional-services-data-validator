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

#### Run a row hash validation for all rows
````shell script
data-validation validate row -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --primary-keys station_id --hash '*'
````

#### Run a row hash validation for all rows but filter only the failed records 
````shell script
data-validation validate row -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --filter-status fail --primary-keys station_id --hash '*'
````

#### Run a row level comparison field validation for 100 random rows
````shell script
data-validation validate row -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --primary-keys station_id -comp-fields name -rr -rbs 100
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

#### Sample YAML Config (Grouped Column validation)
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
  calculated_fields: []
  filters:
  - source: region_id=71
    target: region_id=71
    type: custom
  format: table
  grouped_columns:
  - cast: null
    field_alias: region_id
    source_column: region_id
    target_column: region_id
  labels:
  - !!python/tuple
    - user
    - test
  random_row_batch_size: null
  schema_name: bigquery-public-data.new_york_citibike
  table_name: citibike_stations
  target_schema_name: bigquery-public-data.new_york_citibike
  target_table_name: citibike_stations
  threshold: 0.0
  type: Column
  use_random_rows: false
```

#### Sample YAML with Calc Fields (Cast to NUMERIC before aggregation)

The NUMERIC data type in BigQuery is equivalent to DECIMAL(38,9). This configuration
will run a SUM(CAST(column to NUMERIC)) to avoid integer overflow.

```yaml
result_handler: {}
source: my_bq_conn
target: my_bq_conn
validations:
- aggregates:
  - field_alias: count
    source_column: null
    target_column: null
    type: count
  - field_alias: sum__int
    source_column: cast__int
    target_column: cast__int
    type: sum
  calculated_fields:
  - depth: 0
    field_alias: cast__int
    source_calculated_columns:
    - int
    target_calculated_columns:
    - int
    type: cast
    default_cast: decimal(38,9)
  filters: []
  format: table
  labels: []
  random_row_batch_size: null
  schema_name: project.dataset
  table_name: my_table
  target_schema_name: project.dataset
  target_table_name: my_table
  threshold: 0.0
  type: Column
  use_random_rows: false
```

#### Sample Row Validation YAML with Custom Calc Field

This is a comparison field validation where DVT will first apply the
calculated field and then value compare the result.

```yaml
result_handler: {}
source: my_bq_conn
target: my_bq_conn
validations:
- calculated_fields:
  - depth: 0
    field_alias: replace_name
    source_calculated_columns:
    - name
    target_calculated_columns:
    - name
    type: custom
    ibis_expr: ibis.expr.api.StringValue.replace
    params:
    - pattern: '/'
    - replacement: '-'
  comparison_fields:
  - cast: null
    field_alias: replace_name
    source_column: replace_name
    target_column: replace_name
  filter_status: null
  filters: []
  format: table
  labels: []
  primary_keys:
  - cast: null
    field_alias: station_id
    source_column: station_id
    target_column: station_id
  random_row_batch_size: '5'
  schema_name: bigquery-public-data.new_york_citibike
  table_name: citibike_stations
  target_schema_name: bigquery-public-data.new_york_citibike
  target_table_name: citibike_stations
  threshold: 0.0
  type: Row
  use_random_rows: true
```


#### Run a custom query column validation
````shell script
data-validation validate custom-query --custom-query-type column --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn
````

#### Run a custom query validation with sum aggregation 
````shell script
data-validation validate custom-query --custom-query-type column --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --sum num_bikes_available
````

#### Run a custom query validation with max aggregation 
````shell script
data-validation validate custom-query --custom-query-type column --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations --max num_bikes_available
````

#### Run a custom query row validation
````shell script
data-validation validate custom-query --custom-query-type row --source-query-file source_query.sql --target-query-file target_query.sql -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_stations  --hash \'*\' --primary-keys station_id
````

Please replace source_query.sql and target_query.sql with the correct files containing sql query for source and target database respectively. The primary key should be included
in the SELECT statement of both source_query.sql and target_query.sql