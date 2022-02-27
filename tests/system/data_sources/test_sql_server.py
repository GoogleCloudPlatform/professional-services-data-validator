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

import pytest

# Local testing requires the Cloud SQL Proxy.
# https://cloud.google.com/sql/docs/sqlserver/connect-admin-proxy

# Cloud SQL Proxy listens on localhost
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "127.0.0.1")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "sqlserver")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")


@pytest.fixture
def cloud_sql(request):
    if not request.config.getoption("--no-cloud-sql"):
        mssql_instance = CloudSQLResourceManager(
            PROJECT_ID,
            "SQLSERVER_2017_STANDARD",
            "data-validator-mssql2017",
            SQL_SERVER_PASSWORD,
            database_id="guestbook",
            assign_public_ip=True,
            authorized_networks=None,
            cpu=1,
            memory="4GB",
            enable_bin_logs=False,
            already_exists=True,
        )

        # If instance already exists, returns host IP and does not add new data
        mssql_instance.setup()
        mssql_instance.add_data("gs://pso-kokoro-resources/mssql_data.sql")


def test_sql_server_count(cloud_sql):
    """ Test count validation on SQL Server instance """
    conn = {
        "source_type": "MSSQL",
        "host": SQL_SERVER_HOST,
        "user": SQL_SERVER_USER,
        "password": SQL_SERVER_PASSWORD,
        "port": 1433,
        "database": "guestbook",
    }

    config_count_valid = {
        # BigQuery Specific Connection Config
        consts.CONFIG_SOURCE_CONN: conn,
        consts.CONFIG_TARGET_CONN: conn,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "dbo",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
        ],
        consts.CONFIG_FORMAT: "table",
    }

    data_validator = data_validation.DataValidation(config_count_valid, verbose=False,)
    df = data_validator.execute()
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def test_schema_validation():
    conn = {
        "source_type": "MSSQL",
        "host": SQL_SERVER_HOST,
        "user": SQL_SERVER_USER,
        "password": SQL_SERVER_PASSWORD,
        "port": 1433,
        "database": "guestbook",
    }

    config = {
        consts.CONFIG_SOURCE_CONN: conn,
        consts.CONFIG_TARGET_CONN: conn,
        consts.CONFIG_TYPE: "Schema",
        consts.CONFIG_SCHEMA_NAME: "dbo",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_FORMAT: "table",
    }

    validator = data_validation.DataValidation(config, verbose=True)
    df = validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation["status"] == "Pass"
