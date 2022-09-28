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

# Copied from https://github.com/googleapis/synthtool/blob/master/synthtool/gcp/templates/python_library/.kokoro/build.sh

set -eo pipefail

# Disable buffering, so that the logs stream through.
export PYTHONUNBUFFERED=1

# Install nox
python3.7 -m pip install --upgrade --quiet nox pip
python3.7 -m nox --version

# When NOX_SESSION is set, it only runs the specified session
if [[ -n "${NOX_SESSION:-}" &&  ( "$NOX_SESSION" == "integration_postgres" || "$NOX_SESSION" == "integration_sql_server" ) ]]; then
    ./cloud_sql_proxy -instances="$CLOUD_SQL_CONNECTION" & python3.7 -m nox --error-on-missing-interpreters -s "${NOX_SESSION:-}"
elif [[ -n "${NOX_SESSION:-}" ]]; then
    python3.7 -m nox --error-on-missing-interpreters -s "${NOX_SESSION:-}"
else 
    echo "NOX_SESSION env var not set"
    exit 1
fi
