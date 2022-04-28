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

from data_validation import data_validation, consts, exceptions


# TODO: To use this code I would need to use the Cloud SQL Proxy.
#       https://cloud.google.com/sql/docs/mysql/quickstart-proxy-test

CONN = {
    "source_type": "MySQL",
    "host": os.getenv("MYSQL_HOST"),
    "user": "root",
    "password": os.getenv("MYSQL_PASSWORD"),
    "port": 3306,
    "database": "guestbook",
    "driver": "pymysql",
}
CONFIG_COUNT_VALID = {
    # BigQuery Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "guestbook",
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

CONFIG_SCHEMA_VALID = {
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    consts.CONFIG_TYPE: "Column",
    consts.CONFIG_SCHEMA_NAME: "guestbook",
    consts.CONFIG_TABLE_NAME: "entries",
    consts.CONFIG_FORMAT: "table",
}


def test_mysql_count_invalid_host():
    try:
        data_validator = data_validation.DataValidation(
            CONFIG_COUNT_VALID,
            verbose=False,
        )
        df = data_validator.execute()
        assert df["source_agg_value"][0] == df["target_agg_value"][0]
    except exceptions.DataClientConnectionFailure:
        # Local Testing will not work for MySQL
        pass


def test_schema_validation():
    try:
        data_validator = data_validation.DataValidation(
            CONFIG_SCHEMA_VALID,
            verbose=False,
        )
        df = data_validator.execute()

        for validation in df.to_dict(orient="records"):
            assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS
    except exceptions.DataClientConnectionFailure:
        # Local Testing will not work for MySQL
        pass
