# Copyright 2024 Google LLC
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

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

ENV PIPOPTS="--root-user-action=ignore"

# Install dependencies.
RUN pip install ${PIPOPTS} --upgrade pip
RUN pip install ${PIPOPTS} Flask gunicorn google_pso_data_validator

# Optional: Uncomment the following lines to install additional drivers.
# RUN pip install ${PIPOPTS} 'ibis-framework[impala]'
# RUN pip install ${PIPOPTS} oracledb
# RUN pip install ${PIPOPTS} pyodbc
# RUN pip install ${PIPOPTS} snowflake-sqlalchemy snowflake-connector-python
# RUN pip install ${PIPOPTS} teradatasql

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
