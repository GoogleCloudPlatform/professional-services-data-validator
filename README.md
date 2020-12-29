# Data Validation Tooling
The goal of this tool is to allow easy comparison and validation between different tables.  The repository supports several types of comparison:

    - Count Validation: Total counts and other aggregates match between source and destination
    - Partitioned Count: Partitioned counts and other aggregations between source and destination
        - Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date

## Setup

To write results to BigQuery, you'll need to setup the required cloud
resources, local authentication, and configure the tool.

### Prerequisites

A Google Cloud Platform with the BigQuery API enabled is required.

Confirm which Google user account is used to execute the tool. If running in
production, it's recommended that you create a service account specifically
for running the tool.

#### Create cloud resources - Terraform

You can use Terraform to create the necessary BigQuery resources. (See next
section for manually creating resources with `gcloud`.)

```
cd terraform
terraform init
terraform apply
```

You should see a dataset named `pso_data_validator` and a table named
`results`.

#### Create cloud resources - Cloud SDK (gcloud)

Create a dataset for validation results.

```
bq mk pso_data_validator
```

Create a table.

```
bq mk --table \
  --time_partitioning_field start_time \
  --clustering_fields validation_name,run_id \
  pso_data_validator.results \
  terraform/results_schema.json
```

### Deploy Data Validation CLI

The Data Validation tooling requires Python 3.6+.

Create and activate a new virtual environment to sandbox the tool and its
dependencies from your system installation of Python.

```
python3.6 -m venv venv
source venv/bin/activate
```

Update pip and make sure gcc is installed in your environment.
```
apt-get update  && apt-get install gcc -y && apt-get clean
pip install --upgrade pip
```


You can [pip install this package directly from git](https://pip.pypa.io/en/stable/reference/pip_install/#git)
for any tag (or branch or commit). We suggest installing from latest [GitHub Release]([GitHub](https://github.com/GoogleCloudPlatform/professional-services-data-validator/releases).

install from tag 1.1.0
```bash
python -m pip install -e git+https://github.com/GoogleCloudPlatform/professional-services-data-validator.git@1.1.0#egg=google_pso_data_validator
```

install from HEAD of default branch
```bash
python -m pip install -e git+https://github.com/GoogleCloudPlatform/professional-services-data-validator.git#egg=google_pso_data_validator
```


Alternatively, you can install from [Python Wheel](https://pythonwheels.com/) on [GCS](
https://storage.googleapis.com/professional-services-data-validator/releases/1.1.0/google_pso_data_validator-1.1.0-py3-none-any.whl).

```
python -m pip install https://storage.googleapis.com/professional-services-data-validator/releases/1.1.0/google_pso_data_validator-1.1.0-py3-none-any.whl
```


If you require Teradata and have a license, install the `teradatasql` package.

```
python -m pip install teradatasql
```

After installing the Data Validation package you will
have access to the `data-validation -h` or `python -m data_validation -h`
tool on your CLI.

#### Running CLI Validations

The CLI has several different commands which can be used to create and re-run validations.

The first step is to establish connections to your source and target databases.  You can save
and list connections.  They will be saved either to `~/.config/google-pso-data-validator/` or 
a directory specified by the env variable `PSO_DV_CONFIG_HOME`.
You can manage connections via the CLI:
```
data-validation connections
  add --connection-name my-conn-name
  BigQuery
    --project-id my-gcp-project

data-validation connections list
```

To manually run a validation, use the `run` command and to save the validation to a yaml
file add the flag `--config-file`.  Below details the configurations you
will use to run and to store validations.

```
data-validation run
  --type TYPE, -t TYPE  Type of Data Validation (Column, GroupedColumn)
  --source-conn SOURCE_CONN
                        Source connection details.
                        See: *Data Source Configurations* section for each data source
  --target-conn TARGET_CONN
                        Target connection details
                        See: *Data Source Configurations* section for each data source

  --tables-list TABLES  JSON List of dict {schema:schema_name, table:table_name}
  --grouped-columns GROUPED_COLUMNS
                        JSON List of columns to use in group by '["col_a"]'
                        (Optional) Only used in GroupedColumn tyoe validagtions

  --count COUNT         JSON List of columns sum '["col_a"]' or * for all columns
  --sum SUM             JSON List of columns sum '["col_a"]' or * for all numeric
  --result-handler-config RESULT_HANDLER_CONFIG
                        Result handler config details.
                        See: *Output Handler Configurations* section
  --filters FILTER      JSON List of filters '[{"type":"custom","source":"Col > 100","target":"Col > 100"}]'
  --config-file CONFIG_FILE
                        YAML Config File Path to be used for storing validations.
  --verbose, -v         Verbose logging will print queries executed
```

Once a validation YAML file has been saved via `data-validation run ... --config-file my-validations.yaml`, the validations can be run
very easily via:
```
data-validation run-config
  --config-file CONFIG_FILE
                        YAML Config File Path to be used for executing validations.
  --verbose, -v         Verbose logging will print queries executed
```

## Data Source Configurations
Every source type requires its own configuration for connectivity.  Below is an example of the expected configuration for each source type.

### BigQuery
```
{
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": "my-project-name",

    # (Optional) BigQuery JSON Config File for On-Prem usecases
    "google_service_account_key_path": "/path/to/key.json"
}
```

#### User/Service account needs following BigQuery permissions to run this validator tool:
* bigquery.jobs.create (BigQuery JobUser role)
* bigquery.readsessions.create (BigQuery Read Session User)
* bigquery.tables.get (BigQuery Data Viewer)
* bigquery.tables.getData (BigQuery Data Viewer)

#### If you plan to store validation results in BigQuery:
* bigquery.tables.update (BigQuery Data Editor)
* bigquery.tables.updateData(BigQuery Data Editor)


### Teradata
Please note the Teradata is not-native to this package and must be installed
via `pip install teradatasql` if you have a license.
```
{
    # Configuration Required for All Data Soures
    "source_type": "Teradata",

    # Connection Details
    "host": "127.0.0.1",
    "port":1025,
    "user_name":"my-user",
    "password":"my-password"
}
```

### Oracle
Please note the Oracle package is not installed by default.  You will need to follow [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
Then `pip install cx_Oracle`.
```
{
    # Configuration Required for All Data Soures
    "source_type": "Oracle",

    # Connection Details
    "host": "127.0.0.1",
    "port":1521,
    "user_name":"my-user",
    "password":"my-password",
    "database": "XE",

}
```

### MSSQL Server
Please note the MSSQL Server package is not installed by default.  You will need to follow [SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) installation steps.
Then `pip install pyodbc`.
```
{
    # Configuration Required for All Data Soures
    "source_type": "MSSQL",

    # Connection Details
    "host": "127.0.0.1",
    "port":1521,
    "user_name":"my-user",
    "password":"my-password",
    "database": "my-db",

}
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
