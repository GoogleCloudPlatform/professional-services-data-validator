# Distributed Data Validation with Cloud Run Jobs

This is an example of distributed DVT usage using [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs). Distributed DVT usage alleviates memory constraints when running large table validations. In this sample, you will first generate table partitions with the `generate-table-partitions` command, which will partition your large table into smaller pieces with filters. Read more on generating partitions [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator?#generate-table-partitions-for-large-table-row-validations).The Cloud Run Job can then distribute each partition's YAML configuration as a Cloud Run Task in parallel.

## Quickstart

### Build a Docker Image

You will need to build a Docker image to be used by your Cloud Run Job with DVT installed.

```
export PROJECT_ID=<PROJECT-ID>
gcloud builds submit --tag gcr.io/${PROJECT_ID}/data-validation \
    --project=${PROJECT_ID}
```

### Store connections in Cloud Storage

Store your connections in GCS so they will be accessible to your Cloud Run Job.

```
export PSO_DV_CONN_HOME=<GCS_CONN_PATH>
data-validation connections add --connection-name bq BigQuery --project-id ${PROJECT_ID}
```

The `PSO_DV_CONN_HOME` environment variable will indicate that you want your connection files stored and retrieved from GCS automatically. Read more about it [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#gcs-connection-management-recommended).

### Generate Table Partition YAMLs in GCS

Generate table partitions for the large table you want to validate. In this example, we will use the public table `bigquery-public-data.new_york_trees.tree_census_2015`
and validate the table against itself.

First, create a GCS path to store the YAML configs for this table.

```
export CONFIG_GCS_BUCKET_NAME=<BUCKET_NAME>
export CONFIG_GCS_BUCKET_LOCATION=<BUCKET_LOCATION> # i.e. 'us-central1'
gcloud storage buckets create gs://${CONFIG_GCS_BUCKET_NAME}
  --location=${CONFIG_GCS_BUCKET_LOCATION}
```

Next, generate the table partitions and store the YAMLs in the GCS bucket you created.

```
export BQ_RESULT_TABLE=<BQ_RESULT_TABLE> # i.e. 'project.dataset.table'
data-validation generate-table-partitions \
  -sc bq \
  -tc bq \
  -tbls bigquery-public-data.new_york_trees.tree_census_2015 \
  --primary-keys tree_id \
  --hash '*' \
  --config-dir "gs://${CONFIG_GCS_BUCKET_NAME}/" \
  --partition-num 50 \
  -bqrh ${BQ_RESULT_TABLE}
```

The `generate-table-partitions` command will create a folder named `bigquery-public-data.new_york_trees.tree_census_2015` within your GCS bucket populated with the 50 YAML files.

### Create a Cloud Run Job

First, ensure you have the [correct permissions](https://cloud.google.com/run/docs/create-jobs#iam_permissions_required_to_create_and_execute) to create and
execute Cloud Run Jobs. Also, make sure that the Cloud Run service account has access to Google Cloud Storage (to read connection configuration and YAML files) and BigQuery (to publish results).

Set the number of tasks to the number of partitions you created. In this example, we created 50 partitions.

By default, each partition validation is retried up to 3 times if there is an error. Keep in mind that if you are validating 1000's of partitions in parallel, you may find that setting the parallelism too high (say 100) may result in timeouts and slow down the validation.

```
export JOB_NAME=<CLOUD_RUN_JOB_NAME>
export REGION=<REGION> # i.e us-central1
gcloud run jobs create ${JOB_NAME} \
  --image gcr.io/${PROJECT_ID}/data-validation \
  --tasks 50 --max-retries 2 --parallelism 15 \
  --region ${REGION} \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="-ll,WARNING,configs,run,-kc,-cdir,gs://${CONFIG_GCS_BUCKET_NAME}/bigquery-public-data.new_york_trees.tree_census_2015"
```

We set the `--log-level (-ll)` flag to 'WARNING' to prevent logging validation results to stdout as well as to BigQuery. The `--kube-completions (-kc)` flag indicates you are running in Kubernetes or Cloud Run and signals DVT to only run the validation corresponding to the `CLOUD_RUN_TASK_INDEX` environment variable that is set by Cloud Run.

See the full list of supported flags for the `gcloud run jobs create` command [here](https://cloud.google.com/sdk/gcloud/reference/run/jobs/create).

### Execute the Cloud Run Job

Finally, execute the Cloud Run Job and see your validation results in your BigQuery results table.

```
gcloud run jobs execute ${JOB_NAME} --region ${REGION}
```
