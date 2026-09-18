# Running Multiple Data Validations Concurrently with Cloud Run Jobs

This is an example of distributed DVT usage using [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs) to validate many tables concurrently. This example uses column validation which is the most likely validation type to run in this scenario.

In this sample, you will first generate DVT configuration files in a Cloud Storage directory. Because Cloud Run automatically sets `CLOUD_RUN_TASK_COUNT`, DVT deals the YAML files out to the tasks round-robin, so files can be named after their tables and the number of tasks does not need to equal the number of YAML files.

## Build a Docker Image

You will need to build a Docker image to be used by your Cloud Run Job with DVT installed. This example uses Artifact Registry.

```
PROJECT_ID=<PROJECT-ID>
REGION=<REGION> # e.g. us-central1
REPO=<ARTIFACT-REGISTRY-REPOSITORY-NAME>
SA=<SERVICE_ACCOUNT_NAME>@${PROJECT_ID}.iam.gserviceaccount.com
gcloud builds submit \
  --project=${PROJECT} \
  --service-account=projects/${PROJECT_ID}/serviceAccounts/${SA} \
  --tag=${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/dvt
```

## Store Connections in Cloud Storage

Store your connections in GCS so they will be accessible to your Cloud Run Job.

```
export PSO_DV_CONN_HOME=gs://<GCS-CONN-PATH>
data-validation connections add --connection-name bq BigQuery --project-id ${PROJECT_ID}
```

The `PSO_DV_CONN_HOME` environment variable indicates that you want your connection files stored and retrieved from GCS automatically. Read more about it [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#gcs-connection-management-recommended).

## Generate Table YAMLs in GCS

To run jobs via Cloud Run Jobs we first generate YAML files for each table in a GCS directory. Because Cloud Run automatically injects `CLOUD_RUN_TASK_COUNT`, DVT sorts all YAML files in the directory by name and distributes them across tasks round-robin.

### Static Table List

This example uses a static list of 4 tables from the integration test `pso_data_validator` schema and creates a single YAML file named after each table:

```shell
GCS_YAML_PATH=gs://<GCS-YAML-PATH>
SCHEMA=pso_data_validator
for TABLE in dvt_core_types dvt_large_decimals dvt_binary dvt_char_id; do
  data-validation validate column -sc ora -tc pg \
  --tables-list ${SCHEMA}.${TABLE} \
  --count="*" --min="*" --max="*" --sum="*" \
  --filter-status=fail \
  --config-file=${GCS_YAML_PATH}/${SCHEMA}/${TABLE}.yaml
done
```

Example output configuration files:
```shell
$ gcloud storage ls ${GCS_YAML_PATH}/${SCHEMA}
gs://example-dvt-bucket/dvt_configs/pso_data_validator/dvt_binary.yaml
gs://example-dvt-bucket/dvt_configs/pso_data_validator/dvt_char_id.yaml
gs://example-dvt-bucket/dvt_configs/pso_data_validator/dvt_core_types.yaml
gs://example-dvt-bucket/dvt_configs/pso_data_validator/dvt_large_decimals.yaml
```

### Dynamic Table List

This example uses the `data-validation find-tables` command to generate a dynamic list of all tables in a schema and batches the tables into 4 YAML files. The advantage of batching multiple tables per configuration file is reduced startup overhead per validation; alternatively, you can generate one YAML per table and let dynamic round-robin chunking assign multiple single-table YAMLs to each Cloud Run task.

```shell
GCS_YAML_PATH=gs://<GCS-YAML-PATH>
SCHEMA=pso_data_validator
NUM_CONFIG_FILES=4

data-validation find-tables -sc ora -tc pg \
 --allowed-schemas ${SCHEMA} \
 |jq "[_nwise((length/${NUM_CONFIG_FILES})|ceil)]" > ${SCHEMA}.json

for N in $(seq 0 $((NUM_CONFIG_FILES - 1))); do
  YAML_FILE="batch_${N}.yaml"
  INPUT_TABLES=$(cat ${SCHEMA}.json|jq ".[${N}]")
  data-validation validate column -sc ora -tc pg \
  --tables-list="${INPUT_TABLES}" \
  --count="*" --min="*" --max="*" --sum="*" \
  --filter-status=fail \
  --config-file=${GCS_YAML_PATH}/${SCHEMA}/${YAML_FILE}
done
```

## Run Concurrent Validation

The Cloud Run command below will work through the YAML files across 2 parallel tasks (`--tasks 2 --parallelism 2`). With dynamic round-robin chunking, `--tasks` does not need to equal the number of YAML files—each task will run its assigned subset of files sequentially. Change `--tasks` and `--parallelism` to reflect your desired concurrency.

```shell
PROJECT_ID=<PROJECT-ID>
REGION=<REGION>
REPO=<ARTIFACT-REGISTRY-REPOSITORY-NAME>
NETWORK=<SUBNET-NAME>
PSO_DV_CONN_HOME=gs://<GCS-CONN-PATH>
GCS_YAML_PATH=gs://<GCS-YAML-PATH>/pso_data_validator
JOB_NAME="dvt-$(date +'%Y%m%d%H%M%S')"
gcloud run jobs create ${JOB_NAME} \
  --project ${PROJECT} --region ${REGION} --network=${NETWORK} \
  --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/dvt \
  --tasks 2 --max-retries 2 --parallelism 2 \
  --task-timeout=900s --execute-now \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=${GCS_YAML_PATH}"
```
