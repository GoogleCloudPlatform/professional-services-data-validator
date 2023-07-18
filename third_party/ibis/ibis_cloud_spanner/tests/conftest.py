# Copyright 2021 Google LLC
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

import datetime
import pathlib
import random

import pytest
from google.cloud import spanner_v1

from third_party.ibis.ibis_cloud_spanner.api import spanner_connect

DATA_DIR = pathlib.Path(__file__).parent

RANDOM_MAX = 0xFFFFFFFF
INSTANCE_ID_PREFIX = "data-validation-tool-"
INSTANCE_ID_TEMPLATE = f"{INSTANCE_ID_PREFIX}{{timestamp}}"
INSTANCE_ID_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"
DATABASE_ID_TEMPLATE = "db_{timestamp}_{randint}"


def load_sql(filename):
    lines = []
    with open(DATA_DIR / filename) as sql_file:
        for line in sql_file:
            if line.startswith("--"):
                continue
            lines.append(line)
    return [
        statement.strip()
        for statement in "".join(lines).split(";")
        if statement.strip()
    ]


def insert_rows(transaction):
    dml_statements = load_sql("dml.sql")
    for dml in dml_statements:
        transaction.execute_update(dml)


def insert_rows2(transaction):
    dml_statements = load_sql("dml2.sql")
    for dml in dml_statements:
        transaction.execute_update(dml)


@pytest.fixture(scope="session", autouse=True)
def cleanup_spanner_instances(spanner_client):
    """Remove any instances that are leftover from previous test runs.

    These instances can be leftover if Cloud Build terminates the test run
    before the cleanup in the instance_id fixture compltetes.
    """
    for instance in spanner_client.list_instances():
        instance_id = instance.name.split("/")[-1]
        if instance_id.startswith(INSTANCE_ID_PREFIX):
            creation_time = datetime.datetime.strptime(
                instance_id[len(INSTANCE_ID_PREFIX) :], INSTANCE_ID_TIMESTAMP_FORMAT
            )
            if datetime.datetime.now() - creation_time > datetime.timedelta(days=1):
                instance = spanner_client.instance(instance_id)
                instance.delete()


@pytest.fixture(scope="session")
def spanner_client():
    return spanner_v1.Client()


@pytest.fixture(scope="session")
def instance_id(spanner_client):
    config_name = "{}/instanceConfigs/regional-us-central1".format(
        spanner_client.project_name
    )
    instance_id = INSTANCE_ID_TEMPLATE.format(
        timestamp=datetime.datetime.utcnow().strftime(INSTANCE_ID_TIMESTAMP_FORMAT)
    )
    instance = spanner_client.instance(
        instance_id,
        configuration_name=config_name,
        display_name="Test for Data Validation Tool",
        node_count=1,
    )
    operation = instance.create()
    operation.result()
    yield instance_id
    instance.delete()


@pytest.fixture(scope="session")
def database_id(spanner_client, instance_id):
    database_id = DATABASE_ID_TEMPLATE.format(
        timestamp=datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        randint=random.randint(0, RANDOM_MAX),
    )
    ddl_statements = load_sql("ddl.sql")
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id, ddl_statements=ddl_statements)
    operation = database.create()
    operation.result()
    database.run_in_transaction(insert_rows)
    yield database_id
    database.drop()


@pytest.fixture(scope="session")
def database_id2(spanner_client, instance_id):
    database_id = DATABASE_ID_TEMPLATE.format(
        timestamp=datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        randint=random.randint(0, RANDOM_MAX),
    )
    ddl_statements = load_sql("ddl2.sql")
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id, ddl_statements=ddl_statements)
    operation = database.create()
    operation.result()
    database.run_in_transaction(insert_rows2)
    yield database_id
    database.drop()


@pytest.fixture(scope="session")
def client(instance_id, database_id):
    return spanner_connect(instance_id, database_id)


@pytest.fixture(scope="session")
def client2(instance_id, database_id):
    return spanner_connect(instance_id, database_id)


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
