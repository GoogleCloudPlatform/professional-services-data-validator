# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from third_party.ibis.ibis_cloud_spanner.api import connect as connect_to_cs


instance_id = "cloud_spanner_instance_id"
database_id = "cloud_spanner_databae_id"


def connect(instance_id, database_id):
    return connect_to_cs(instance_id, database_id)


@pytest.fixture(scope="session")
def inst_id():
    return instance_id


@pytest.fixture(scope="session")
def client():
    return connect(instance_id, database_id)


@pytest.fixture(scope="session")
def client2():
    return connect(instance_id, database_id)


@pytest.fixture(scope="session")
def alltypes(client):
    return client.table("functional_alltypes")


@pytest.fixture(scope="session")
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope="session")
def students(client):
    return client.table("students_pointer")


@pytest.fixture(scope="session")
def students_df(students):
    return students.execute()


@pytest.fixture(scope="session")
def array_table(client):
    return client.table("array_table")
