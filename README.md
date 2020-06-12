# Data Validation Tooling
The goal of this tool is to allow easy comparison and validation between different tables.  The repository supports several types of comparison
    - Count Validation: Total counts and other aggregates match between source and destination
    - Partitioned Count: Partitioned counts and other aggregations between source and destination
        - Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date
    - Stretch: Custom queries of any kind match between source and destination
    - Stretch: Random sampling of data matches between source and destination

## Deploy Data Validation Tooling
TODO: This repository will eventually be made open source and pushed to PyPi

### Deploy to Composer
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

## Source Configuration
Every source type requires its own configuration for connectivity.  Below is an example of the expected configuration for each source type.

### BigQuery
```
{
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": "my-project-name",

    # Configuration Required Depending on Validator Type
    "schema_name": "dataset_name",
    "table_name": "table_name",
    consts.PARTITION_COLUMN: "updated_at",
    consts.PARTITION_COLUMN_TYPE: "DATE",
}
```

## Example of Comparing BigQuery to BigQuery
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

## Validation Reports

The data validation tool can write the results of a validation run to Google
BigQuery.

### Setup

To write results to BigQuery, you'll need to setup the required cloud
resources, local authentication, and configure the tool.

#### Prerequisites

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

#### Configure tool to output to BigQuery

TODO([GH#24](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/24)):
Add support for BigQuery output to CLI

## Contributing

Contributions are welcome. See the [contributing guide](CONTRIBUTING.md) for
details.
