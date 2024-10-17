#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
python3 -m venv rel_venv
source rel_venv/bin/activate

pip install setuptools wheel
python setup.py register sdist bdist_wheel

export PACKAGE_VERSION=$(grep 'version = ' setup.py | awk '{print $3;}' | sed 's/"//g')
export GCS_DIRECTORY=gs://professional-services-data-validator/releases/${PACKAGE_VERSION}/
pip install ./dist/google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl

data-validation -h
gsutil -m rm ${GCS_DIRECTORY}**

gsutil cp README.md CHANGELOG.md ${GCS_DIRECTORY}
gsutil cp dist/google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl ${GCS_DIRECTORY}google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl
gsutil cp dist/google-pso-data-validator-${PACKAGE_VERSION}.tar.gz ${GCS_DIRECTORY}google-pso-data-validator-${PACKAGE_VERSION}.tar.gz

gsutil -m acl ch -u AllUsers:R ${GCS_DIRECTORY}**
deactivate
rm -rf rel_venv/

# Push New Release to Latest

gsutil cp ${GCS_DIRECTORY}CHANGELOG.md gs://professional-services-data-validator/releases/latest/
gsutil cp ${GCS_DIRECTORY}README.md gs://professional-services-data-validator/releases/latest/
gsutil cp ${GCS_DIRECTORY}google-pso-data-validator-${PACKAGE_VERSION}.tar.gz gs://professional-services-data-validator/releases/latest/google-pso-data-validator-latest.tar.gz
gsutil cp ${GCS_DIRECTORY}google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl gs://professional-services-data-validator/releases/latest/google_pso_data_validator-latest-py3-none-any.whl

gsutil -m acl ch -u AllUsers:R gs://professional-services-data-validator/releases/latest/*
