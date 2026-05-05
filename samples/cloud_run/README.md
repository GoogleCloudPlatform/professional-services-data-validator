# Data Validation on Cloud Run (Service)

### Quick Steps

The deployment logic is discussed in more detail below; however, to quickly
deploy DVT, follow this simple script:

```bash
export PROJECT_ID=<PROJECT-ID>
./deploy.sh
python3 test.py
```

### Build Docker Image

You will need to build a Docker image to be used by Cloud Run. In order to add
Teradata or SQL Server, you will need to customize the Dockerfile and add your
licensed utilities.

```bash
PROJECT_ID=<PROJECT-ID>
REGION=<REGION e.g. us-central1>
BUILD_SA=<SERVICE ACCOUNT _NAME>@${PROJECT}.iam.gserviceaccount.com
REPO="dvt"
TAG="dvt:latest"

gcloud builds submit \
  --project=${PROJECT_ID} \
  --tag=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${TAG} \
  --default-buckets-behavior=regional-user-owned-bucket \
  --service-account=projects/${PROJECT_ID}/serviceAccounts/${BUILD_SA}
```

### Deploy to Cloud Run

If calls to the Cloud Run service will be from within the VPC then you
may prefer to include `--ingress=internal` to restrict access to the service.

Setting `$PSO_DV_CONN_HOME` will make connections stored in Cloud Storage
available to DVT.

```bash
PROJECT_ID=<PROJECT-ID>
REGION=<REGION> # e.g. us-central1
NETWORK=<NETWORK>
REPO=<ARTIFACT-REGISTRY-REPOSITORY-NAME>
SA=<SERVICE_ACCOUNT_NAME>@${PROJECT_ID}.iam.gserviceaccount.com
TAG="dvt:latest"
PSO_DV_CONN_HOME=gs://<BUCKET>/connections/
gcloud run deploy dvt \
  --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${TAG} \
  --project=${PROJECT_ID} --region=${REGION} \
  --service-account=${SA} \
  --no-allow-unauthenticated \
  --network=${NETWORK} \
  --set-env-vars=PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME}
```

### IAM Permissions

To interact with the Cloud Run endpoint, you, or the service account that will be
used to call the endpoint, needs to have the Cloud Run Invoker role. For example:

```bash
PROJECT_ID=<PROJECT-ID>
REGION=<REGION> # e.g. us-central1
SA=<SERVICE_ACCOUNT_NAME>@${PROJECT_ID}.iam.gserviceaccount.com
gcloud run services add-iam-policy-binding dvt \
  --project=${PROJECT_ID} --region=${REGION} \
  --member="serviceAccount:${SA}" \
  --role="roles/run.invoker"
```

### Test Cloud Run Endpoint

For a quick test, we have provided this logic in `test.py` to run a validation
against a public BigQuery table:

```bash
export PROJECT_ID=<PROJECT-ID>
export REGION=<REGION> # defaults to us-central1
python3 test.py
```

If you intend to execute custom query validations, you will need to declare
each aggregation column from your SQL query inside the `data` configuration object
as shown in the code snippet below:

```python
query = """
SELECT start_station_id,
       SUM(CASE WHEN bikeid = 18104 THEN 1 END ) AS bike_18104
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE bikeid = 18104
GROUP BY start_station_id
ORDER BY bike_18104 DESC
"""

data = {
    "source_conn": {
        "source_type": "BigQuery",
        "project_id": PROJECT_ID,
    },
    "target_conn": {
        "source_type": "BigQuery",
        "project_id": PROJECT_ID,
    },
    "type": "Custom-query",
    "custom_query_type": "Column",
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_stations",
    "source_query": query,
    "target_query": query,
    "aggregates": [
        {
            "source_column": "bike_18104",
            "target_column": "bike_18104",
            "field_alias": "sum_bike",
            "type": "sum",
        }
    ],
}
```
