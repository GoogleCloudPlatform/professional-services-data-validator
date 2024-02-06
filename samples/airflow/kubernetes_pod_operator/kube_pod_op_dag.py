"""
 * Copyright 2024 Google LLC
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

from datetime import timedelta

import airflow
from airflow import DAG
from airflow import models
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

default_args = {
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dvt_dag_kubernetes_pod_operator",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    run_dvt = KubernetesPodOperator(
        # The ID specified for the task.
        task_id="dvt-validation",
        name="dvt-validation",
        cmds=["bash", "-cx"],
        # Performs a simple column validation on public data
        arguments=[
            "source $HOME/.venv/dvt/bin/activate && data-validation connections add --connection-name bq-connect BigQuery --project-id {{PROJECT_ID}} && data-validation validate column -sc bq-connect -tc bq-connect -tbls bigquery-public-data.new_york_citibike.citibike_trips -bqrh {{PROJECT_ID}}.pso_data_validator.results "
        ],
        # The namespace to run within Kubernetes. In Composer 2 environments
        # after December 2022, the default namespace is
        # `composer-user-workloads`.
        namespace="composer-user-workloads",
        # DVT image built from README instructions
        image="gcr.io/{{PROJECT_ID}}/data-validation",
        # default to '~/.kube/config'. The config_file is templated.
        config_file="/home/airflow/composer_kube_config",
        # Identifier of connection that should be used
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        env_vars={
            # Uncomment this env variable to reference connections stored in a GCS bucket
            # "PSO_DV_CONFIG_HOME": "gs://{{BUCKET_NAME}}/",
            "GOOGLE_CLOUD_PROJECT": "{{PROJECT_ID}}",
        },
    )
