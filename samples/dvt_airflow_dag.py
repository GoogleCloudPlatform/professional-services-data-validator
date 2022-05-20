"""
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

"""
DAG for Data Validator Tool

https://github.com/GoogleCloudPlatform/professional-services-data-validator

Requirements
- Composer created with Public IP (Private environment disabled)

Steps before run DAG
- Create Airflow variable PROJECT_ID with the GCP Project ID
- Create BigQuery dataset and table:

bq mk pso_data_validator
wget https://raw.githubusercontent.com/GoogleCloudPlatform/professional-services-data-validator/develop/terraform/results_schema.json
bq mk --table \
  --time_partitioning_field start_time \
  --clustering_fields validation_name,run_id \
  pso_data_validator.results \
  results_schema.json
"""

from datetime import timedelta

import airflow
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonVirtualenvOperator

default_args = {
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dvt_example_dag",
    default_args=default_args,
    description="Data Validation Tool Example DAG",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
) as dag:

  def data_validation_function(project_id):
    from data_validation import data_validation
    from data_validation.result_handlers import bigquery as bqhandler

    BQ_CONN = {"source_type": "BigQuery", "project_id": project_id}

    GROUPED_CONFIG_COUNT_VALID = {
        # BigQuery Specific Connection Config
        "source_conn": BQ_CONN,
        "target_conn": BQ_CONN,
        # Validation Type
        "type": "Column",
        # Configuration Required Depending on Validator Type
        "schema_name": "bigquery-public-data.new_york_citibike",
        "table_name": "citibike_trips",
        "grouped_columns": [
            {
                "field_alias": "starttime",
                "source_column": "starttime",
                "target_column": "starttime",
                "cast": "date",
            },
        ],
        "aggregates": [
            {
                "field_alias": "count",
                "source_column": None,
                "target_column": None,
                "type": "count",
            },
        ],
    }

    result_handler = bqhandler.BigQueryResultHandler.get_handler_for_project(project_id)
    validator = data_validation.DataValidation(
        GROUPED_CONFIG_COUNT_VALID, verbose=True, result_handler=result_handler
    )
    df = validator.execute()

  PROJECT_ID = models.Variable.get('PROJECT_ID')

  virtualenv_task = PythonVirtualenvOperator(
      task_id="dvt-virtualenv",
      python_callable=data_validation_function,
      op_args=[PROJECT_ID],
      requirements=["google-pso-data-validator"],
      system_site_packages=False,
  )

virtualenv_task
