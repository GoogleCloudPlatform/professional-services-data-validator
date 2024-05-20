#!/bin/bash

export PROJECT_ID=<PROJECT-ID>
export BUCKET=<BUCKET>
export REGION=<REGION>
export SERVICE_ACCOUNT=<SERVICE-ACCOUNT>

cd samples/functions/

zip -r data_validation.zip .
gsutil cp data_validation.zip gs://${BUCKET}/

gcloud functions deploy data-validation --region=${REGION} \
	--entry-point=main \
	--runtime=python38 --trigger-http \
	--source=gs://${BUCKET}/data_validation.zip \
	--service-account=${SERVICE_ACCOUNT} \
	--project=${PROJECT_ID}

rm data_validation.zip
cd ../../

# Run Test
export TEST_DATA='{"config":{"type":"Column","source_conn":{"source_type":"BigQuery","project_id":"PROJECT_ID","google_service_account_key_path":null},"target_conn":{"source_type":"BigQuery","project_id":"PROJECT_ID","google_service_account_key_path":null},"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips","target_schema_name":"bigquery-public-data.new_york_citibike","target_table_name":"citibike_trips","result_handler":null,"filters":[],"aggregates":[{"source_column":null,"target_column":null,"field_alias":"count","type":"count"}]}}'
gcloud functions call data-validation --region=${REGION} --data=${TEST_DATA} --project=${PROJECT_ID}
