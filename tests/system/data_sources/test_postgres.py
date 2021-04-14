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

from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from data_validation import data_validation, consts

import os

# Local testing requires the Cloud SQL Proxy.
# https://cloud.google.com/sql/docs/postgres/connect-admin-proxy


POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")


def test_postgres_count():
    """ Test count validation on Postgres instance """
    postgres_instance = CloudSQLResourceManager(
        PROJECT_ID,
        "POSTGRES_12",
        "data-validator-postgres12",
        POSTGRES_PASSWORD,
        database_id="guestbook",
        assign_public_ip=True,
        authorized_networks=None,
        cpu=1,
        memory="4GB",
        enable_bin_logs=False,
        already_exists=True,
    )
    postgres_instance.setup()
    postgres_instance.add_data("gs://pso-kokoro-resources/postgres_data.sql")

    # Cloud SQL proxy listens to localhost
    conn = {
        "source_type": "Postgres",
        "host": "localhost",
        "user": "postgres",
        "password": POSTGRES_PASSWORD,
        "port": 5432,
        "database": "guestbook",
    }

    config_count_valid = {
        # BigQuery Specific Connection Config
        consts.CONFIG_SOURCE_CONN: conn,
        consts.CONFIG_TARGET_CONN: conn,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
        ],
    }

    data_validator = data_validation.DataValidation(config_count_valid, verbose=False,)
    df = data_validator.execute()
    assert df["source_agg_value"][0] == df["target_agg_value"][0]
