# Running Multiple Data Validations Concurrently with Cloud Run Jobs

This is an example of distributed DVT usage using [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs) to validate many tables concurrently. This example uses column validation which is the most likely validation type to run in this scenario.

In this sample, you will first generate DVT configuration files. Cloud Run Jobs can then distribute each table's YAML configuration as a Cloud Run Task concurrently.

Note: This sample subverts logic used to [scale out row validation for a large table](../large_table/README.md). It can be streamlined once [issue 1205](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1205) has been actioned.

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

To run jobs via Cloud Run Jobs we first need to generate YAML files for each table. Unfortunately the YAML files (currently, see issue 1205 note above) have a strict naming convention of `nnnn.yaml`, starting from "0000".

### Static Table List

This example uses a static list of 4 tables from the integration test `pso_data_validator` schema and creates a single YAML file for each table:

```shell
GCS_YAML_PATH=gs://<GCS-YAML-PATH>
SCHEMA=pso_data_validator
N=0
for TABLE in dvt_core_types dvt_large_decimals dvt_binary dvt_char_id; do
  YAML_FILE="$(printf "%04d\n" ${N}).yaml"
  data-validation validate column -sc ora -tc pg \
  --tables-list ${SCHEMA}.${TABLE} \
  --count="*" --min="*" --max="*" --sum="*" \
  --filter-status=fail \
  --config-file=${GCS_YAML_PATH}/${SCHEMA}/${YAML_FILE}
  ((N++))
done
```

Example output configuration files:
```shell
$ gsutil ls ${GCS_YAML_PATH}/${SCHEMA}
gs://example-dvt-bucket/dvt_configs/pso_data_validator/0000.yaml
gs://example-dvt-bucket/dvt_configs/pso_data_validator/0001.yaml
gs://example-dvt-bucket/dvt_configs/pso_data_validator/0002.yaml
gs://example-dvt-bucket/dvt_configs/pso_data_validator/0003.yaml
```

### Dynamic Table List

This example uses the `data-validation find-tables` command to generate a dynamic list of all tables in a schema and batches the tables into 4 YAML files. The advantage of batching multiple tables per configuration file is reduced overhead from starting many Cloud Run Jobs, we can benefit from concurrent processing while also letting DVT process multiple tables serially.

```shell
GCS_YAML_PATH=gs://<GCS-YAML-PATH>
SCHEMA=pso_data_validator
NUM_CONFIG_FILES=4

data-validation find-tables -sc ora -tc pg \
 --allowed-schemas ${SCHEMA} \
 |jq "[_nwise((length/${NUM_CONFIG_FILES})|ceil)]" > ${SCHEMA}.json

for N in $(seq 0 $((NUM_CONFIG_FILES - 1))); do
  YAML_FILE="$(printf "%04d\n" ${N}).yaml"
  INPUT_TABLES=$(cat ${SCHEMA}.json|jq ".[${N}]")
  data-validation validate column -sc ora -tc pg \
  --tables-list="${INPUT_TABLES}" \
  --count="*" --min="*" --max="*" --sum="*" \
  --filter-status=fail \
  --config-file=${GCS_YAML_PATH}/${SCHEMA}/${YAML_FILE}
done
```

## Run Concurrent Validation

The Cloud Run command below will work through the 4 YAML files in 2 parallel streams. `--tasks 4` should be changed to match however many input YAML files you have. `--parallelism 2` should be changed to reflect the desired concurrency.

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
  --tasks 4 --max-retries 2 --parallelism 2 \
  --task-timeout=900s --execute-now \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=${GCS_YAML_PATH}"
```
