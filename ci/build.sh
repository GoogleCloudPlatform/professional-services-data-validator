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
#

PYTHON="python3.9"
if [[ -n "$1" && "$1" =~ python.* ]]; then
    PYTHON=$1
fi

set -eo pipefail

if [[ "$NOX_SESSION" == "integration_sybase" ]]; then
    sudo add-apt-repository -y universe
    apt-get update
    sudo apt-get install -y freetds-bin freetds-common
    # Can't install tdsodbc due to libodbc conflict, but we can download the .so file:
    sudo apt-get download tdsodbc
    # apt-get remove -y msodbcsql17 libodbc2
    # apt autoremove -y
    # apt-get install -y tdsodbc
    # apt-get install -y freetds-dev freetds-bin tdsodbc
    echo "[FreeTDS]
Description=FreeTDS Driver for Linux
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
UsageCount=1" > /etc/odbcinst.ini
fi

# Disable buffering, so that the logs stream through.
export PYTHONUNBUFFERED=1

# Install nox
${PYTHON} -m pip install --upgrade --quiet nox pip
${PYTHON} -m nox --version

# When NOX_SESSION is set, it only runs the specified session
if [[ -n "${NOX_SESSION:-}" &&  ( "$NOX_SESSION" == "integration_postgres" || "$NOX_SESSION" == "integration_sql_server" || "$NOX_SESSION" == "integration_mysql" || "$NOX_SESSION" =~ integration_oracle.* ) ]]; then
    ./cloud_sql_proxy -instances="$CLOUD_SQL_CONNECTION" & ${PYTHON} -m nox --error-on-missing-interpreters -s "${NOX_SESSION:-}"
elif [[ -n "${NOX_SESSION:-}" ]]; then
    ${PYTHON} -m nox --error-on-missing-interpreters -s "${NOX_SESSION:-}"
else
    echo "NOX_SESSION env var not set"
    exit 1
fi
