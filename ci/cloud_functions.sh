#!/bin/bash

export PROJECT_ID=pso-kokoro-resources
export BUCKET=pso-kokoro-resources
export REGION=us-central1


zip -r data_validation.zip .
gsutil cp data_validation.zip gs://${BUCKET}/

gcloud functions deploy data-validation --region=${REGION} \
	--entry-point=main \
	--runtime=python38 --trigger-http \
	--source=gs://${BUCKET}/data_validation.zip \
	--project=${PROJECT_ID}

rm data_validation.zip






# gcloud functions deploy data-validation --region=REGION \
# 	--entry-point=main \
# 	--runtime=python38 --trigger-http \
# 	--source=gs://${BUCKET}/data_validation.zip \
# 	--project=${PROJECT_ID}
        # [--egress-settings=EGRESS_SETTINGS]
        # [--ignore-file=IGNORE_FILE] [--ingress-settings=INGRESS_SETTINGS]
        # [--memory=MEMORY] [--retry] [--runtime=RUNTIME]

        # [--service-account=SERVICE_ACCOUNT] [--source=SOURCE]
        # [--stage-bucket=STAGE_BUCKET] [--timeout=TIMEOUT]
        # [--update-labels=[KEY=VALUE,...]]
        # [--build-env-vars-file=FILE_PATH | --clear-build-env-vars
        #   | --set-build-env-vars=[KEY=VALUE,...]
        #   | --remove-build-env-vars=[KEY,...]
        #   --update-build-env-vars=[KEY=VALUE,...]]
        # [--clear-env-vars | --env-vars-file=FILE_PATH
        #   | --set-env-vars=[KEY=VALUE,...]
        #   | --remove-env-vars=[KEY,...] --update-env-vars=[KEY=VALUE,...]]
        # [--clear-labels | --remove-labels=[KEY,...]]
        # [--clear-max-instances | --max-instances=MAX_INSTANCES]
        # [--clear-vpc-connector | --vpc-connector=VPC_CONNECTOR]
        # [--trigger-bucket=TRIGGER_BUCKET | --trigger-http
        #   | --trigger-topic=TRIGGER_TOPIC
        #   | --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
        # [GCLOUD_WIDE_FLAG ...]
