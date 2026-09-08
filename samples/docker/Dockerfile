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

FROM python:3.12-slim

# App version is passed as a build argument.
# If not provided, the latest version will be installed.
ARG _APP_VERSION=""

# Set working directory
WORKDIR /app

# Copy and set up the entrypoint script
COPY samples/docker/entrypoint.sh .
RUN chmod +x entrypoint.sh

# Install the Data Validator from PyPI
RUN pip install --upgrade pip
RUN \
  if [ -z "$_APP_VERSION" ]; then \
    echo "Installing latest version of google-pso-data-validator"; \
    pip install google-pso-data-validator; \
  else \
    echo "Installing google-pso-data-validator version ${_APP_VERSION}"; \
    pip install google-pso-data-validator==${_APP_VERSION}; \
  fi

# Optional: Uncomment the following lines to install additional drivers.
# RUN pip install teradatasql
# RUN pip install oracledb
# RUN pip install pyodbc
# RUN pip install 'ibis-framework[impala]'
# RUN pip install snowflake-sqlalchemy
# RUN pip install snowflake-connector-python

# The entrypoint script will handle connections and run the application
ENTRYPOINT ["./entrypoint.sh"]