#!/bin/bash


export PROJECT_ID=pso-kokoro-resources
export BUCKET=pso-kokoro-resources
export REGION=us-central1

cd samples/functions/

cp ../../requirements.txt .
echo "# Install Data Validation" >> requirements.txt
echo "https://storage.googleapis.com/professional-services-data-validator/releases/latest/google_pso_data_validator-latest-py3-none-any.whl" >> requirements.txt

zip -r data_validation.zip .
gsutil cp data_validation.zip gs://${BUCKET}/

gcloud functions deploy data-validation --region=${REGION} \
	--entry-point=main \
	--runtime=python38 --trigger-http \
	--source=gs://${BUCKET}/data_validation.zip \
	--service-account=pso-kokoro-resources@appspot.gserviceaccount.com \
	--project=${PROJECT_ID}

rm data_validation.zip
rm requirements.txt
cd ../../

# Run Test
export TEST_DATA='{"cmd":"ls"}'
gcloud functions call data-validation --region=${REGION} --data=${TEST_DATA} --project=${PROJECT_ID}
