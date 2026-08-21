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

# Install DVT
ENV DVT_HOME=/opt/dvt
ENV VIRTUAL_ENV=${DVT_HOME}/.venv
ENV PIPOPTS="--root-user-action=ignore"
RUN mkdir ${DVT_HOME}
RUN python3 -m venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
RUN pip install ${PIPOPTS} --upgrade pip
RUN pip install ${PIPOPTS} google_pso_data_validator

# Optional: Uncomment the following lines to install additional drivers.
# RUN pip install ${PIPOPTS} 'ibis-framework[impala]'
# RUN pip install ${PIPOPTS} oracledb
# RUN pip install ${PIPOPTS} pyodbc
# RUN pip install ${PIPOPTS} snowflake-sqlalchemy snowflake-connector-python
# RUN pip install ${PIPOPTS} teradatasql

ENTRYPOINT ["python","-m","data_validation"]
