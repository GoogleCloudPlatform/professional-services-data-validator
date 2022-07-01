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


export PACKAGE_VERSION=$(grep 'version = ' ../../setup.py | awk '{print $3;}' | sed 's/"//g')

python3 -m venv rel_venv
source rel_venv/bin/activate
pip install --upgrade pip

pip install setuptools wheel
cd ../../
python setup.py register sdist bdist_wheel

cd samples/docker
cp ../../dist/google_pso_data_validator-${PACKAGE_VERSION}-py2.py3-none-any.whl .
docker build -t data_validation:$PACKAGE_VERSION --build-arg _APP_VERSION=$PACKAGE_VERSION .
rm google_pso_data_validator-${PACKAGE_VERSION}-py2.py3-none-any.whl