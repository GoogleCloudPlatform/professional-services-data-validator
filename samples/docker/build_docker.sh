#!/bin/bash
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

# This script builds the Docker image for the Data Validator.
# It can optionally build a specific version of the tool if the `_APP_VERSION`
# argument is provided.

# Example usage:
# ./samples/docker/build_docker.sh  # Builds the latest version
# ./samples/docker/build_docker.sh 1.2.3 # Builds version 1.2.3

# Example run command with base64 encoded connection strings:
# export DVT_SRC_CONN=$(echo '{"source_type": "BigQuery", "project_id": "my-gcp-project"}' | base64 -w 0)
# export DVT_TGT_CONN=$(echo '{"source_type": "BigQuery", "project_id": "my-gcp-project"}' | base64 -w 0)
# docker run -e DVT_SRC_CONN -e DVT_TGT_CONN data_validation:latest \
#   --config-file my_validation.yaml

set -e

# This script should be run from the root of the project
if [ ! -f setup.py ]; then
    echo "Please run this script from the root of the project."
    exit 1
fi

APP_VERSION=${1:-$(grep 'version = ' setup.py | awk '{print $3;}' | sed 's/"//g')}
IMAGE_TAG=${APP_VERSION:-latest}


echo "Building Docker image with tag: ${IMAGE_TAG}"

docker build \
    --build-arg "_APP_VERSION=${APP_VERSION}" \
    -t "data_validation:${IMAGE_TAG}" \
    -f samples/docker/Dockerfile . # Build from the root context

echo "Successfully built Docker image: data_validation:${IMAGE_TAG}"
