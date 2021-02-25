# Data Validation Tooling
The goal of this tool is to allow easy comparison and validation between different tables.  The repository supports several types of comparison:

    - Count Validation: Total counts and other aggregates match between source and destination
    - Partitioned Count: Partitioned counts and other aggregations between source and destination
        - Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date
    - Filters: Count or Partitioned Count WHERE FILTER_CONDITION

## Installation
The [Installation](docs/Installation.md) page describes the prerequisites and setup steps needed to install and use the data validation tool.

## Running validations
In order to run validations, Data Validation tool needs to create connections to the source and target. Once the connections are created, the tool can run validations on those connections.
the data validation tool can run validations on those connections.

### Connections
[Connections.md](docs/connections.md) provides details about how to create and review connections for the validation tool.

### Running CLI Validations

The CLI has several different commands which can be used to create and re-run validations.
[validation_cli.md](docs/validation_cli.md) provides many examples of how a tool can used to run powerful validations without writing any queries.


### Running Custom SQL Exploration
There are many occasions where you need to exmplore a data source while running
validations.  To avoid the need to open and install a new client, the CLI allows
you to run custom queries.
```
data-validation query
  --conn connection-name The named connection to be queried.
  --query, -q The Raw query to run against the supplied connection
```

## Query Configurations

It is possible to customize the configuration for a given validation.  The CLI expects that you are trying to compare two identical tables; however, you can customize each query (sour or target) either by running the validation with a custom configuration in Python or editing a saved YAML configuration file.

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

2. In third_partty/ibis/ibis_addon/operations.py
    - Add the RawSQL operator to the data source registry (for custom filter support).

3. You are done, you can reference the data source via the config.
    - Config: {"source_type": "<RefName>", ...KV Values required in Client...}

## Examples

### Example of Comparing BigQuery to BigQuery
```
# Store BigQuery Connection
data-validation connections add -c my_bq_conn BigQuery --project-id pso-kokoro-resources

# Run Validation (optionally use -c file_path.yaml to store the validation)
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn \
  -tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' \
  --sum '*' --count '*'

# Run a Stored Validations File
data-validation run-config -c ex_yaml.yaml
```

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
