#!/bin/bash

export PROJECT_ID=pso-kokoro-resources
export BUCKET=pso-kokoro-resources
export REGION=us-central1

pwd
zip -r data_validation.zip ../professional-services-data-validator/
# gsutil cp data_validation.zip gs://${BUCKET}/

# gcloud functions deploy data-validation --region=${REGION} \
# 	--entry-point=main \
# 	--runtime=python38 --trigger-http \
# 	--source=gs://${BUCKET}/data_validation.zip \
# 	--project=${PROJECT_ID}
