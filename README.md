# Data Validation Tooling
The goal of Data Validation Tool is to allow easy comparison and validation between different tables.  This Python CLI tool supports several types of comparison:

    - Count Validation: Total counts and other aggregates match between source and destination
    - Partitioned Count: Partitioned counts and other aggregations between source and destination
        - Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date
    - Filters: Count or Partitioned Count WHERE FILTER_CONDITION

## Installation
The [installation](docs/installation.md) page describes the prerequisites and setup steps needed to install and use the data validation tool.

## Usage
Before using this tool, you will need to create connections to the source and target tables. Once the connections are created, you can run validations on those tables. Validation results can be printed to stdout (default) or outputted to BigQuery. The validation tool also allows you to save or edit validation configurations in a YAML file. This is useful for running common validations or updating the configuration.

### Connections
The [Connections](docs/connections.md) page provides details about how to create and list connections for the validation tool.

### Running CLI Validations

The data validation CLI is a main interface to use this tool.

The CLI has several different commands which can be used to create and re-run validations.

The validation tool first expects connections to be created before running validations. To create connections please review the [Connections](connections.md) page.

Once you have your connections set up, you are ready to run the validations.

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

The [Examples](docs/examples.md) page provides many examples of how a tool can used to run powerful validations without writing any queries.


### Running Custom SQL Exploration
There are many occasions where you need to explore a data source while running
validations.  To avoid the need to open and install a new client, the CLI allows
you to run custom queries.
```
data-validation query
  --conn connection-name The named connection to be queried.
  --query, -q The Raw query to run against the supplied connection
```

## Query Configurations

You can customize the configuration for any given validation by providing use case specific CLI arguments or editing the saved YAML configuration file.

For example, the following command creates a YAML file for the validation of  the `new_york_citibike` table.
```
data-validation run -t Column -sc bq -tc bq -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' -c citibike.yaml
```

Here is the generated YAML file named `citibike.yaml`:
```
result_handler: {}
source: bq
target: bq
validations:
- aggregates:
  - field_alias: count
    source_column: null
    target_column: null
    type: count
  filters: []
  labels: []
  schema_name: bigquery-public-data.new_york_citibike
  table_name: citibike_trips
  target_schema_name: bigquery-public-data.new_york_citibike
  target_table_name: citibike_trips
  type: Column

```

You can now edit the YAML file if, for example, the `new_york_citibike` table is stored in datasets that have different names in the source and target systems. Once the file is updated and saved, the following command runs the new validation:
```
data-validation run-config -c citibike.yaml
```


### Filters

Currently the only form of filter supported is a custom filter written by you in the syntax of the given source.  In future we will also release pre-built filters to cover certain usecases (ie. `SELECT * FROM table WHERE created_at > 30 days ago;`).

#### Custom Filters
```
{
    "type": "custom",
    "source": "created_at > '2020-01-01 00:00:00'::TIMESTAMP",
    "target": "created_at > '2020-01-01 00:00:00'",
}
```

Note that you are writing the query to execute, which does not have to match between source and target as long as the results can be expected to align.


## Validation Reports

The data validation tool can write the results of a validation run to Google
BigQuery or print to Std Out.

The output handlers tell the data validation where to store the results of each validation.
By default the handler will print to stdout.

### Configure tool to output to BigQuery
```
{
    # Configuration Required for All Data Soures
    "type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": "my-project-name",

    # (Optional) BigQuery JSON Config File for On-Prem usecases
    "google_service_account_key_path": "/path/to/key.json",

    # Configuration Required Depending on Validator Type
    "table_id": "dataset_name.table_name"
}
```

## Add Support for an existing Ibis Data Source

If you want to add an Ibis Data Source which exists, but was not yet supported in the Data Validation tool, it is a simple process.

1. In data_validation/data_validation.py
    - Import the extened Client for the given source (ie. from ibis.sql.mysql.client import MySQLClient).
    - Add the "<RefName>": Client to the global CLIENT_LOOKUP dictionary.

2. In third_party/ibis/ibis_addon/operations.py
    - Add the RawSQL operator to the data source registry (for custom filter support).

3. You are done, you can reference the data source via the config.
    - Config: {"source_type": "<RefName>", ...KV Values required in Client...}


## Deploy to Composer
```
#!/bin/bash

export COMPOSER_ENV=""
export LOCATION=""

echo "Creating Composer Env: $COMPOSER_ENV"
gcloud services enable composer.googleapis.com
gcloud composer environments create $COMPOSER_ENV --location=$LOCATION --python-version=3

echo "Updating Composer Env Reqs: $COMPOSER_ENV"
# Composer builds Pandas and BigQuery for you, these should be stripped out
cat requirements.txt | grep -v pandas | grep -v google-cloud-bigquery > temp_reqs.txt
gcloud composer environments update $COMPOSER_ENV --location=$LOCATION --update-pypi-packages-from-file=temp_reqs.txt
rm temp_reqs.txt

# Deploy Package to Composer (the hacky way)
echo "Rebuilding Data Validation Package in GCS"
export GCS_BUCKET_PATH=$(gcloud composer environments describe $COMPOSER_ENV --location=$LOCATION | grep dagGcsPrefix | awk '{print $2;}')
gsutil rm -r $GCS_BUCKET_PATH/data_validation
gsutil cp -r data_validation $GCS_BUCKET_PATH/data_validation

# Deploy Test DAG to Composer
echo "Pushing Data Validation Test Operator to GCS"
gsutil cp tests/test_data_validation_operators.py $GCS_BUCKET_PATH/
```

## Contributing

Contributions are welcome. See the [contributing guide](CONTRIBUTING.md) for
details.
