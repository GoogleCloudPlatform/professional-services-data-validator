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

import datetime
import os
import random

import google.cloud.bigquery
import pytest


ALPHABET = "abcdefghijklmnopqrstuvwxyz"


@pytest.fixture(scope="module")
def bigquery_client():
    project_id = os.environ["PROJECT_ID"]

    return google.cloud.bigquery.Client(project=project_id)


@pytest.fixture(scope="module")
def bigquery_dataset_id(bigquery_client):
    now = datetime.datetime.now()
    project_id = os.environ["PROJECT_ID"]
    dataset_id = (
        f"{project_id}.data_validator_tests_"
        + now.strftime("%Y%m%d%H%M")
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
    )
    bigquery_client.create_dataset(dataset_id)
    yield dataset_id
    bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
