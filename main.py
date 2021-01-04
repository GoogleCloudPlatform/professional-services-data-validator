# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
from yaml import dump, load, Dumper, Loader

from data_validation import cli_tools, clients, consts, jellyfish_distance
from data_validation.config_manager import ConfigManager
from data_validation.data_validation import DataValidation

def main(request):
	""" Handle incoming Data Validation requests.

		request (flask.Request): HTTP request object.
	"""
	return "Hello"



export PROJECT_ID=pso-kokoro-resources
export BUCKET=pso-kokoro-resources

gcloud functions deploy data-validation --region=REGION \
	--entry-point=main \
	--runtime=python38 --trigger-http \
	--source=gs://${BUCKET}/data_validation.zip \
	--project=${PROJECT_ID}

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
