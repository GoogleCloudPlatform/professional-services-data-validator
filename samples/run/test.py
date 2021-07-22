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
    describe_service = DESCRIBE_SERVICE.format(
        service_name=service_name, project_id=project_id
    )
    with os.popen(describe_service) as service:
        description = service.read()

    return re.findall("URL:.*\n", description)[0].split()[1].strip()


data = {
    "source_conn": {"source_type": "BigQuery", "project_id": PROJECT_ID,},
    "target_conn": {"source_type": "BigQuery", "project_id": PROJECT_ID,},
    "type": "Column",
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_stations",
    "target_schema_name": "bigquery-public-data.new_york_citibike",
    "target_table_name": "citibike_stations",
    "aggregates": [
        {
            "source_column": None,
            "target_column": None,
            "field_alias": "count",
            "type": "count",
        }
    ],
}

url = get_cloud_run_url("data-validation", PROJECT_ID)
res = requests.post(url, headers={"Authorization": "Bearer " + get_token()}, json=data)
print(res.content.decode())
