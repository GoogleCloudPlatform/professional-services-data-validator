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

# Save current directory of the script.
# https://stackoverflow.com/a/246128/101923
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${DIR}/setup.sh"

# Run MySQL Integration Tests
export MYSQL_HOST=35.184.142.16
export MYSQL_PASSWORD=password
python -m nox -s integration_mysql

# Run BigQuery Integration Tests
export PROJECT_ID=pso-kokoro-resources
python -m nox --error-on-missing-interpreters -s integration_bigquery
