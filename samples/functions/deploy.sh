#!/bin/bash

export PROJECT_ID=pso-kokoro-resources
export BUCKET=pso-kokoro-resources
export REGION=us-central1


cp ../requirements.txt
zip -r data_validation.zip .
gsutil cp data_validation.zip gs://${BUCKET}/

gsutil iam ch serviceAccount:service-38376331460@gcf-admin-robot.iam.gserviceaccount.com:objectAdmin gs://artifacts.${PROJECT_ID}.appspot.com/
gsutil iam ch serviceAccount:service-38376331460@gcf-admin-robot.iam.gserviceaccount.com:objectAdmin gs://us.artifacts.${PROJECT_ID}.appspot.com/

gcloud functions deploy data-validation --region=${REGION} \
	--entry-point=main \
	--runtime=python38 --trigger-http \
	--source=gs://${BUCKET}/data_validation.zip \
	--service-account=pso-kokoro-resources@appspot.gserviceaccount.com \
	--project=${PROJECT_ID}

rm data_validation.zip

# Run Test
export TEST_DATA='{"cmd":"ls"}'
gcloud functions call data-validation --region=${REGION} --data=${TEST_DATA} --project=${PROJECT_ID}
