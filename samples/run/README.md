# Data Validation on Cloud Run

### Quick Steps

The deployment logic is discussed in more detail below; however, to quickly
deploy DVT, follow this simple script:

```
export PROJECT_ID=<PROJECT-ID>
./deploy.sh
python3 test.py
```

### Build Docker Image

You will need to build a Docker image to be used by Cloud Run. In order to add
Teradata or Oracle, you will need to customize the Dockerfile and add your
licensed utilities.

```
export PROJECT_ID=<PROJECT-ID>
gcloud builds submit --tag gcr.io/${PROJECT_ID}/data-validation \
    --project=${PROJECT_ID}
```

### Deploy to Cloud Run

```
gcloud run deploy --image gcr.io/${PROJECT_ID}/data-validation \
    --project=${PROJECT_ID}
```

### Test Cloud Run Endpoint

You can easily run a request via Python.  For a quick test, we have provided this logic in `test.py` to run a validation against a public BigQuery table. The example is similar and also shows how you can forward results to BigQuery from the Cloud Run job:

```
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

import os
import re
import requests

PROJECT_ID = os.environ.get("PROJECT_ID")

DESCRIBE_SERVICE = """
gcloud run services describe {service_name} --region=us-central1 --project={project_id}
"""


def get_token():
  with os.popen("gcloud auth print-identity-token") as cmd:
    token = cmd.read().strip()

  return token


def get_cloud_run_url(service_name, project_id):
  describe_service = DESCRIBE_SERVICE.format(service_name=service_name,
                                             project_id=project_id)
  with os.popen(describe_service) as service:
    description = service.read()

  return re.findall("URL:.*\n", description)[0].split()[1].strip()


data = {
    "source_conn": {
        "source_type": "BigQuery",
        "project_id": PROJECT_ID,
    },
    "target_conn": {
        "source_type": "BigQuery",
        "project_id": PROJECT_ID,
    },
    "type": "Column",
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_stations",
    "target_schema_name": "bigquery-public-data.new_york_citibike",
    "target_table_name": "citibike_stations",
    "result_handler": {
      "type": "BigQuery",
      "project_id": PROJECT_ID,
      "table_id": "pso_data_validator.results"
   },

    "aggregates": [
        {
            "source_column": None,
            "target_column": None,
            "field_alias": "count",
            "type": "count"
        }],
}

url = get_cloud_run_url("data-validation", PROJECT_ID)
res = requests.post(
    url, headers={"Authorization": "Bearer " + get_token()}, json=data)
print(res.content.decode())
```

### Oracle Setup

If you would like to use Data Validation against an Oracle DB you will need to
supply your own license files.  To do so:

1) Create an `oracle` directory and add your .rpm files into it.

- oracle/oracle-instantclient12.2-basiclite-12.2.0.1.0-1.x86_64.rpm
- oracle/oracle-instantclient12.2-devel-12.2.0.1.0-1.x86_64.rpm
- oracle/oracle-instantclient12.2-odbc-12.2.0.1.0-2.x86_64.rpm

2) Uncomment all logic in the Dockerfile under the Oracle Dependencies comments

