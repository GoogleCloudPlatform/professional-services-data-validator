# Data Validation Tooling
The goal of this tool is to allow easy comparison and validation between different tables.  The repository supports several types of comparison
    - Count Validation: Total counts and other aggregates match between source and destination
    - Partitioned Count: Partitioned counts and other aggregations between source and destination
        - Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date
    - Stretch: Custom queries of any kind match between source and destination
    - Stretch: Random sampling of data matches between source and destination

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

The Data Validation tooling requires Python 3.6+ and Terraform installed.

```
# Clone repo locally or install via pip
git clone git@github.com:GoogleCloudPlatform/professional-services-data-validator.git
cd professional-services-data-validator/
python -m pip install --upgrade pip
python -m pip install .

# NOTE: If you require Teradata and have a license.
python -m pip install teradatasql
```

After installing the Data Validation package you will
have access to the `data-validation -h` tool on your CLI.

#### Running CLI Validations

The CLI has several different commands which can be used to create and re-run validations.

To manually run a validation, use the `run` command and to save the validation to a yaml
file switch to `store` and add a config file path.  Below details the configurations you
will use for both `run` and `store`.

```
data-validation {run|store}
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
  --config-file CONFIG_FILE
                        YAML Config File Path to be used for building or running validations.
  --verbose, -v         Verbose logging will print queries executed
```

Once a validation YAML file has been saved via `data-validation store ...`, the validations can be run
very easily via:
```
data-validation run-config
  --config-file CONFIG_FILE
                        YAML Config File Path to be used for building or running validations.
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
    "user_name":"my_user",
    "password":"my_password"
}
```

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

## Examples

### Example of Comparing BigQuery to BigQuery
```
from data_validation import data_validation, consts
from data_validation.data_sources import data_client, example_client, bigquery
from data_validation.query_builder import query_builder

inp_config = {
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": os.environ["PROJECT_ID"],

    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    consts.PARTITION_COLUMN: "starttime",
    consts.PARTITION_COLUMN_TYPE: "DATE",
}
out_config = {
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": os.environ["PROJECT_ID"],

    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    consts.PARTITION_COLUMN: "starttime",
    consts.PARTITION_COLUMN_TYPE: "DATE",
}

builder = query_builder.QueryBuilder.build_partition_count_validator()
data_validation.process_data(builder, inp_config, out_config)
```

## Contributing

Contributions are welcome. See the [contributing guide](CONTRIBUTING.md) for
details.