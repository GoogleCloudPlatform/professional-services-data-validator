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
    "config": {"host": "35.227.139.75", "user": "root", "password": "password", "port": 3306, "database": 'guestbook', "driver": 'pymysql'},

    # Configuration Required Depending on Validator Type
    "schema_name": "guestbook",
    "table_name": "entries",
    "partition_column": "starttime",
}


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_validation_test',
    default_args=default_args,
    description='Test Data Validation Operators',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60))


# priority_weight has type int in Airflow DB, uses the maximum.
validate_table = operators.DataValidationCountOperator(
    task_id="validate_table",
    source_config=BQ_CONFIG_VALID,
    target_config=BQ_CONFIG_VALID,
    dag=dag,
    # depends_on_past=False,
    # priority_weight=2**31-1
    )

