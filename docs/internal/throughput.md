# DVT Hash Validation Throughput

## TL;DR
Customers often ask for throughput numbers for DVT row hash validations. This document attempts to provide "best case" numbers we can talk about. By "best case" we mean that all systems tested are in GCP and therefore customers testing with on-premises databases will likely experience lower throughput numbers.

We only tested up to Cloud Run parallelism of 8. Customers could go higher but should keep in mind the overhead on source and target systems.

All tests were executed for tables containing 100 million rows each.

<table>
  <thead>
    <tr>
      <th rowspan="2">Engines <br>(source & target)</th>
      <th rowspan="2">Parallelism</th>
      <th colspan="2">No Result Handler</th>
      <th colspan="2">PostgreSQL Result Handler</th>
    </tr>
    <tr>
      <th>Elapsed time</th>
      <th>Throughput (rows/minute)</th>
      <th>Elapsed time</th>
      <th>Throughput (rows/minute)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="4">Oracle & BigQuery</td>
      <td>1</td>
      <td>62m32s</td>
      <td>1.5 million</td>
      <td>103m51s</td>
      <td>1 million</td>
    </tr>
    <tr>
      <td>2</td>
      <td>30m54s</td>
      <td>3.2 million</td>
      <td>52m27s</td>
      <td>1.9 million</td>
    </tr>
    <tr>
      <td>4</td>
      <td>15m53s</td>
      <td>6.2 million</td>
      <td>28m10s</td>
      <td>3.5 million</td>
    </tr>
    <tr>
      <td>8</td>
      <td>8m49s</td>
      <td>11.3 million</td>
      <td>15m5s</td>
      <td>6.6 million</td>
    </tr>
  </tbody>
</table>


## Assumptions
The actual data in source and target rows is irrelevant for these tests because we are only interested in DVT's ability to process the hashes and the hash values are the same size regardless of how many input columns there are. We are not trying to measure the speed of customers' SQL engines.

The source and target databases are in GCP in the same region as DVT itself. Again we are not trying to measure the speed of customers' connections to GCP, only DVT itself.

DVT utilizes threads when receiving results from source and target engines but is single threaded when doing its own CPU intensive work, therefore it will only fully consume a single vCPU.

Working assumption (based on internal testing) is that we need 2GB of RAM per million rows processed in a single pass. We've carefully sized DVT partitions to fit within the memory provisioned in Cloud Run for a single vCPU. For limits see https://cloud.google.com/run/docs/configuring/jobs/memory-limits.


## Helper environment variables
```
PROJECT=my-project
LOCATION=my-region
NETWORK=my-network
BUCKET=my-bucket
REPO=my-dvt-artifact-repo
SA=dvt-sa@${PROJECT}.iam.gserviceaccount.com
DVT_TAG=dvt:770
```


## Connections
We created three connections:
- An Oracle source connection
- A BigQuery target connection
- A PostgreSQL result handler connection

The connections were stored in Cloud Storage so they can be used from Cloud Run.

```
export PSO_DV_CONN_HOME=gs://${BUCKET}

# Oracle
data-validation connections add --secret-manager-type GCP \
--secret-manager-project-id ${PROJECT} \
-c ora Oracle \
--host=10.x.x.x --port=1521 --database=pdb1 \
--user=dvt_user --password=dvt_pass_secret

# BigQuery
data-validation connections add \
-c bq BigQuery \
--project=${PROJECT}

# PostgreSQL results handler
data-validation connections add --secret-manager-type GCP \
--secret-manager-project-id ${PROJECT} \
-c pg_rh Postgres \
--host=10.x.x.x --database=postgres \
--user=dvt_results_writer --password=dvt_results_writer_pass_secret
```

## Generate partitions

We have created plenty of partitions to keep the individual memory requirement below the memory provisioned in Cloud Run for a single vCPU. We only intend to test up to parallelism 8 and therefore use `--parts-per-file=4` to group partitions into fewer configuration files which reduces the overhead of creating/closing Cloud Run Jobs.

### With filter on status
This command uses `--filter-status=fail` to prevent any data being written to the result handler.
```
data-validation generate-table-partitions \
-sc=ora -tc=bq \
-tbls=dvt_test.tab_vol_100m \
--primary-keys=id \
--hash='*' \
--filter-status=fail \
-cdir=gs://${BUCKET}/partitions_dir/ora2bq_t16 \
--result-handler=pg_rh.pso_data_validator_results.results \
--partition-num=64 --parts-per-file=4
```

### Without filter on status
This command generates partitions that will write all validation results to the result handler, i.e. all 100 million rows will write their status.
```
data-validation generate-table-partitions \
-sc=ora -tc=pg \
-tbls=dvt_test.tab_vol_100m \
--primary-keys=id \
--hash='*' \
-cdir=gs://${BUCKET}/partitions_dir/ora2pg_t16_rh \
--result-handler=pg_rh.pso_data_validator_results.results \
--partition-num=64 --parts-per-file=4
```

## DVT container
First we need to define a container with DVT installed into a Python virtual environment and, if validating Oracle data, the cx_Oracle package and a supporting Oracle client.

Example `Dockerfile`:

```
FROM python:3.11-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

RUN apt-get update

# Oracle client
RUN apt-get -y install libaio1 libaio-dev unzip wget
ENV OTN_URL=https://download.oracle.com/otn_software/linux/instantclient
RUN wget -q ${OTN_URL}/218000/instantclient-sdk-linux.x64-21.8.0.0.0dbru.zip && \
    wget -q ${OTN_URL}/218000/instantclient-basic-linux.x64-21.8.0.0.0dbru.zip && \
    wget -q ${OTN_URL}/218000/instantclient-tools-linux.x64-21.8.0.0.0dbru.zip && \
    mkdir /opt/oracle && \
    unzip instantclient-sdk-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/ && \
    unzip instantclient-basic-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/ && \
    unzip instantclient-tools-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/
ENV ORACLE_HOME=/opt/oracle/instantclient_21_8
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_8

# Python pre-reqs (gcc for cx_Oracle)
RUN apt-get install gcc -y

# Install DVT
ENV DVT_HOME=/opt/dvt
ENV VIRTUAL_ENV=${DVT_HOME}/.venv
ENV PIPOPTS="--root-user-action=ignore"
RUN mkdir ${DVT_HOME}
RUN python3 -m venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
RUN . ${VIRTUAL_ENV}/bin/activate && pip install ${PIPOPTS} --upgrade pip
RUN . ${VIRTUAL_ENV}/bin/activate && pip install ${PIPOPTS} google_pso_data_validator
RUN . ${VIRTUAL_ENV}/bin/activate && pip install ${PIPOPTS} cx_Oracle

# Entrypoint
ENTRYPOINT ["python","-m","data_validation"]
```

Build image example:
```
gcloud builds submit \
 --project=${PROJECT} --region=${LOCATION} \
 --service-account=projects/${PROJECT}/serviceAccounts/${SA} \
 --gcs-log-dir=gs://${BUCKET}/cloud-run-logs/ \
 --tag=${LOCATION}-docker.pkg.dev/${PROJECT}/${REPO}/${DVT_TAG}
```

## Test commands

### Oracle & BigQuery validation without result handler writes
```
PSO_DV_CONN_HOME=gs://${BUCKET}
PART_DIR="partitions_dir/ora2bq_t16/dvt_test.tab_vol_100m"
```
#### Parallel 1
```
JOB_NAME=$(echo "dvt_ora2bq_px1_$(date +'%Y%m%d%H%M%S')"|tr _ -)
gcloud run jobs create ${JOB_NAME} \
  --project ${PROJECT} --region ${LOCATION} --network=${NETWORK} \
  --image ${LOCATION}-docker.pkg.dev/${PROJECT}/${REPO}/${DVT_TAG} \
  --service-account=${SA} \
  --tasks 16 --max-retries 1 --parallelism 1 \
  --memory 4G --cpu 1 \
  --task-timeout=900s --execute-now \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=gs://${BUCKET}/${PART_DIR}"
```
#### Parallel 2
```
JOB_NAME=$(echo "dvt_ora2bq_px2_$(date +'%Y%m%d%H%M%S')"|tr _ -)
gcloud run jobs create ${JOB_NAME} \
  --project ${PROJECT} --region ${LOCATION} --network=${NETWORK} \
  --image ${LOCATION}-docker.pkg.dev/${PROJECT}/${REPO}/${DVT_TAG} \
  --service-account=${SA} \
  --tasks 16 --max-retries 1 --parallelism 2 \
  --memory 4G --cpu 1 \
  --task-timeout=900s --execute-now \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=gs://${BUCKET}/${PART_DIR}"
```
#### Parallel 4
```
JOB_NAME=$(echo "dvt_ora2bq_px4_$(date +'%Y%m%d%H%M%S')"|tr _ -)
gcloud run jobs create ${JOB_NAME} \
  --project ${PROJECT} --region ${LOCATION} --network=${NETWORK} \
  --image ${LOCATION}-docker.pkg.dev/${PROJECT}/${REPO}/${DVT_TAG} \
  --service-account=${SA} \
  --tasks 16 --max-retries 1 --parallelism 4 \
  --memory 4G --cpu 1 \
  --task-timeout=900s --execute-now \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=gs://${BUCKET}/${PART_DIR}"
```
#### Parallel 8
```
JOB_NAME=$(echo "dvt_ora2bq_px8_$(date +'%Y%m%d%H%M%S')"|tr _ -)
gcloud run jobs create ${JOB_NAME} \
  --project ${PROJECT} --region ${LOCATION} --network=${NETWORK} \
  --image ${LOCATION}-docker.pkg.dev/${PROJECT}/${REPO}/${DVT_TAG} \
  --service-account=${SA} \
  --tasks 16 --max-retries 1 --parallelism 8 \
  --memory 4G --cpu 1 \
  --task-timeout=900s --execute-now \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=gs://${BUCKET}/${PART_DIR}"
```

### Oracle & BigQuery validation with result  handler writes
```
PSO_DV_CONN_HOME=gs://${BUCKET}
PART_DIR="partitions_dir/ora2bq_t16_rh/dvt_test.tab_vol_100m"
```

Same commands as those without results handler, only changing `JOB_NAME=$(echo "dvt_ora2bq_rh_px<parallelism_number>_$(date +'%Y%m%d%H%M%S')"|tr _ -)` to be able to properly distinguish the jobs with the result handler from the previous ones.