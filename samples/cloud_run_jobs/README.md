# Distributed Data Validation with Cloud Run Jobs or GKE Jobs

This is an example of distributed DVT usage using Cloud Run Jobs or GKE Batch Jobs. This is useful when you are validating large tables and hitting memory constraints. This will require table partitions to be generated beforehand with the `generate-table-partitions` command. Once the YAMLs are generated, the jobs can distribute each YAML configuration file per node in parallel.

### Quick Steps


### Build Docker Image

You will need to build a Docker image to be used by Cloud Run Jobs.

```
export PROJECT_ID=<PROJECT-ID>
gcloud builds submit --tag gcr.io/${PROJECT_ID}/data-validation \
    --project=${PROJECT_ID}
```

### Create a Cloud Run Job
Ensure you have the [correct permissions](https://cloud.google.com/run/docs/create-jobs#iam_permissions_required_to_create_and_execute) to create and
execute Cloud Run Jobs.

```
gcloud run jobs create JOB_NAME --image gcr.io/${PROJECT_ID}/data-validation --tasks 25 --max-retries 2 --parallelism 30 --set-env-vars
```

### Store connections in GCS


### Generate Table Partition YAMLs in GCS

Generate table partitions for the large table you want to validate. In this example, we will use the public table `bigquery-public-data.new_york_trees.tree_census_2015`
and validate the table against itself.

First, create a GCS path to store the YAML configs for this table.
```
export BUCKET_NAME=<BUCKET_NAME>
export BUCKET_LOCATION=<BUCKET_LOCATION>
gcloud storage buckets create gs://BUCKET_NAME --location=BUCKET_LOCATION
```

Next, generate the table partitions and store the YAMLs in the GCS location specified.
```
data-validation generate-table-partitions \
  -sc bq \
  -tc bq \
  -tbls bigquery-public-data.new_york_trees.tree_census_2015 \
  --primary-keys tree_id \
  --hash '*' \
  --config-dir gs://${BUCKET_NAME}/data_validation_configs \
  --partition-num 50 \
  -bqrh PROJECT.DATASET.RESULTS_TABLE
```

### 

