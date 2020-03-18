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
import airflow
import warnings

# from data_validation import data_validation, consts, exceptions
# from data_validation.query_builder import query_builder
from airflow import DAG
from datetime import timedelta

from data_validation.composer import operators

BQ_CONFIG_VALID = {
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",
    # BigQuery Specific Connection Config
    "config": {"project_id": os.environ["GCP_PROJECT"]},
    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    "partition_column": "starttime",
}

# TODO: To use this code I would need to whitelist the MySQL instance
MYSQL_CONFIG_INVALID = {
    # Configuration Required for All Data Soures
    "source_type": "MySQL",
    # BigQuery Specific Connection Config
    "config": {
        "host": "35.227.139.75",
        "user": "root",
        "password": "password",
        "port": 3306,
        "database": "guestbook",
        "driver": "pymysql",
    },
    # Configuration Required Depending on Validator Type
    "schema_name": "guestbook",
    "table_name": "entries",
    "partition_column": "starttime",
}


default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "data_validation_test",
    default_args=default_args,
    description="Test Data Validation Operators",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)


# priority_weight has type int in Airflow DB, uses the maximum.
validate_bq_table_count = operators.DataValidationCountOperator(
    task_id="validate_bq_table_count",
    source_config=BQ_CONFIG_VALID,
    target_config=BQ_CONFIG_VALID,
    dag=dag,
    # depends_on_past=False,
    # priority_weight=2**31-1
)

validate_mysql_table_count = operators.DataValidationCountOperator(
    task_id="validate_mysql_table_count",
    source_config=MYSQL_CONFIG_INVALID,
    target_config=MYSQL_CONFIG_INVALID,
    dag=dag,
    # depends_on_past=False,
    # priority_weight=2**31-1
)
