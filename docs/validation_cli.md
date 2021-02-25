Data Validation CLI is a main interface to use this tool.

The CLI has several different commands which can be used to create and re-run validations.

Validation tool first expects connections to be created before running validations. To create connections please review [connections.md](connections.md) page.

Once you have connections setup, you are ready to run the validations:

### Validation command syntax and options

```
data-validation run
  --type TYPE, -t TYPE  Type of Data Validation (Column, GroupedColumn)
  --source-conn SOURCE_CONN, -sc SOURCE_CONN
                        Source connection details.
                        See: *Data Source Configurations* section for each data source
  --target-conn TARGET_CONN, -tc TARGET_CONN
                        Target connection details
                        See: *Data Source Configurations* section for each data source
  --tables-list TABLES, -tbls TABLES 
                        JSON List of tables 
                        '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips","target_table_name":"citibike_trips"}]'
  --grouped-columns GROUPED_COLUMNS, -gc GROUPED_COLUMNS
                        JSON List of columns to use in group by '["col_a"]'
                        (Optional) Only used in GroupedColumn type validations
  --count COUNT         JSON List of columns count '["col_a"]' or * for all columns
  --sum SUM             JSON List of columns sum '["col_a"]' or * for all numeric
  --min MIN             JSON List of columns min '[\"col_a\"]' or * for all numeric
  --max MAX             JSON List of columns max '[\"col_a\"]' or * for all numeric
  --avg AVG             JSON List of columns avg '[\"col_a\"]' or * for all numeric
  --result-handler-config RESULT_HANDLER_CONFIG, -rc RESULT_HANDLER_CONFIG
                        (Optional) JSON Result handler config details. Defaults to stdout
                        See: *Validation Reports* section
  --filters FILTER      JSON List of filters '[{"type":"custom","source":"Col > 100","target":"Col > 100"}]'
  --config-file CONFIG_FILE, -c CONFIG_FILE
                        YAML Config File Path to be used for storing validations.
  --labels KEY=VALUE, -l KEY1=VALUE1,KEY2=VALUE2
                        (Optional) Comma-separated key value pair labels for the run.
  --verbose, -v         Verbose logging will print queries executed
```


**Following commands walk you through some basic use cases of the commands:**

**PLEASE NOTE:** In below commands, my_bq_conn refers to the connection name for your BigQuery project. We are validating BigQuery tables that are
available in BigQuery public datasets.

#### Simple COUNT(*) on a table
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]'
````

#### Run multiple tables
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"},{"schema_name":"bigquery-public-data.new_york_citibike","table_name"
:"citibike_stations"}]'
````

#### Store validation config to the file
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' -c citibike.yaml
````
Above command creates a YAML file named citibike.yaml that can be used to run validations in the future. 

**Please note:** When config-file(-c) option is provided, only a file gets created. Validation doesn’t execute.
 
 
#### Run validations from a configuration file
````shell script
data-validation run-config -c citibike.yaml
````
Above command executes validations stored in a config file named citibike.yaml. 

**Please note:** Currently config file is the way for users to easily validate tables and columns with different names. CLI doesn't currently support
the validation of tables with different names.
 

#### Run COUNT validations for all columns
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' --count '*'
````

#### Run COUNT validations for selected columns
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' --count '["bikeid","gender"]'
````

#### Store results in a BigQuery table
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' --count '["tripduration","start_station_name"]' -rc '{"project_id":"$YOUR-PROJECT-ID","type":"BigQuery","table_id":"pso_data_validator.results"}'
````
Please replace $YOUR-PROJECT-ID with the correct project-id where you created your results datasets as mentioned in [Installation](Installation.md#setup) section.


#### Preview results from a results BigQuery Table

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
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' --grouped-columns '["bikeid"]'
````

#### Run Multi column GroupBy validation
````shell script
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' --grouped-columns '["bikeid","usertype"]'
````

#### Apply single aggregation on a single field
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' --sum '["num_bikes_available"]'
````


#### Apply single aggregation on multiple fields
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' --sum '["num_bikes_available","num_docks_available"]'
````

#### Apply different aggregations on multiple fields
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' --sum '["num_bikes_available","num_docks_available"]' --avg '["num_bikes_disabled","num_docks_disabled"]'
````

#### Apply different aggregations on multiple fields and apply GroupBy
````shell script
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' --grouped-columns '["region_id"]' --sum '["num_bikes_available","num_docks_available"]' --avg '["num_bikes_disabled","num_docks_disabled"]'
````

#### Apply filters
````shell script
data-validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' --grouped-columns '["region_id"]' --sum '["num_bikes_available","num_docks_available"]' --filters '[{"type":"custom","source":"region_id=71","target":"region_id=71"}]' -rc '{"project_id":"$YOUR-PROJECT-ID","type":"BigQuery","table_id":"pso_data_validator.results"}'
````

#### Apply labels
````shell script
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' --count '["tripduration","start_station_name"]' -l "run-tag=test-run,run-val=test"
````

#### Run custom SQL 
````shell script
data-validation query
  --conn connection-name The named connection to be queried.
  --query, -q The Raw query to run against the supplied connection
````
