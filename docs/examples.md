# Data Validation Examples
This page describes some basic use cases of the tool.

**PLEASE NOTE:** In below commands, my_bq_conn refers to the connection name for your BigQuery project. We are validating BigQuery tables that are
available in BigQuery public datasets. 

Also, note that if no aggregation flag is provided, the tool will run a 'COUNT *' default aggregation.

#### Simple COUNT(*) on a table
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips
````

#### Run multiple tables
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips,bigquery-public-data.new_york_citibike:citibike_stations
````

#### Store validation config to the file
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips -c citibike.yaml
````
Above command creates a YAML file named citibike.yaml that can be used to run validations in the future. 

**Please note:** When the config-file (-c) option is provided, only the YAML file gets created. The validation doesn’t execute. 
 
 
#### Run validations from a configuration file
````shell script
data-validation run-config -c citibike.yaml
````
Above command executes validations stored in a config file named citibike.yaml. 

**Please note:** Currently config file is the way for users to easily validate tables and columns with different names. CLI doesn't currently support
the validation of tables with different names.
 

#### Run COUNT validations for all columns
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips --count '*'
````

#### Run COUNT validations for selected columns
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips --count bikeid,gender
````

#### Store results in a BigQuery table
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips --count tripduration,start_station_name -rc $YOUR_PROJECT_ID.pso_data_validator.results
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
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips --grouped-columns bikeid
````

#### Run a multi-column GroupBy validation
````shell script
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips --grouped-columns bikeid,usertype
````

#### Apply single aggregation on a single field
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_stations --sum num_bikes_available
````


#### Apply single aggregation on multiple fields
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_stations --sum num_bikes_available,num_docks_available
````

#### Apply different aggregations on multiple fields
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_stations --sum num_bikes_available,num_docks_available --avg num_bikes_disabled,num_docks_disabled
````

#### Apply different aggregations on multiple fields and apply GroupBy
````shell script
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_stations --grouped-columns region_id --sum num_bikes_available,num_docks_available --avg num_bikes_disabled,num_docks_disabled
````

#### Apply filters
````shell script
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_stations --grouped-columns region_id --sum num_bikes_available,num_docks_available --filters 'region_id=71' -rc $YOUR_PROJECT_ID.pso_data_validator.results
````

#### Apply labels
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls bigquery-public-data.new_york_citibike:citibike_trips --count tripduration,start_station_name -l tag=test-run,owner=name
````

#### Run custom SQL 
````shell script
data-validation query
  --conn connection-name The named connection to be queried.
  --query, -q The Raw query to run against the supplied connection
````
