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
from data_validation.query_builder import query_builder


# TODO: To use this code I would need to use the Cloud SQL Proxy.
#       https://cloud.google.com/sql/docs/mysql/quickstart-proxy-test
MYSQL_CONFIG_INVALID = {
    # Configuration Required for All Data Soures
    "source_type": "MySQL",
    # MySQL Specific Connection Config
    "config": {
        "host": os.getenv("MYSQL_HOST"),
        "user": "root",
        "password": os.getenv("MYSQL_PASSWORD"),
        "port": 3306,
        "database": "guestbook",
        "driver": "pymysql",
    },
    # Configuration Required Depending on Validator Type
    "schema_name": "guestbook",
    "table_name": "entries",
    consts.PARTITION_COLUMN: "starttime",
}


def test_mysql_count_invalid_host():
    builder = query_builder.QueryBuilder.build_count_validator()
    try:
        data_validator = data_validation.DataValidation(
            builder,
            MYSQL_CONFIG_INVALID,
            MYSQL_CONFIG_INVALID,
            result_handler=None,
            verbose=False,
        )
        data_validator.execute()
    except exceptions.DataClientConnectionFailure as e:
        # pass
        raise
    else:
        raise AssertionError("Expected DataClientConnectionFailure")
