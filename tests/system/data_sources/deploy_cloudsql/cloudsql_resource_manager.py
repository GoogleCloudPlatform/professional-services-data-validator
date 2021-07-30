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

from tests.system.data_sources.deploy_cloudsql.gcloud_context import GCloudContext

import json
import random
import string

DATABASE_TYPES = ("MYSQL_5_7", "POSTGRES_12", "SQLSERVER_2017_STANDARD")


class CloudSQLResourceManager:
    def __init__(
        self,
        project_id,
        database_type,
        instance_id,
        password,
        database_id=None,
        assign_public_ip=False,
        authorized_networks=None,
        cpu=None,
        memory=None,
        enable_bin_logs=True,
        already_exists=False,
    ):
        """Initialize a CloudSQLResourceManager """
        if database_type not in DATABASE_TYPES:
            raise ValueError(
                f"Invalid database type. Must be of the form {str(DATABASE_TYPES)}"
            )
        self._project_id = project_id
        self._database_type = database_type
        self._instance_id = instance_id
        self._password = password
        self._database_id = database_id
        self._assign_public_ip = assign_public_ip
        self._authorized_networks = authorized_networks
        self._cpu = cpu
        self._memory = memory
        self._enable_bin_logs = enable_bin_logs
        self._already_exists = already_exists
        self.db = {}

    def describe(self):
        """ Returns description of resource manager instance """
        print(
            f"Creates a {self._database_type} instance in project {self._project_id} with "
            f"database_id: {self._database_id}, instance_id: {self._instance_id}."
        )

    def setup(self):
        """ Creates Cloud SQL instance and database """
        with GCloudContext(self._project_id) as gcloud:
            if self._already_exists:
                json_describe = gcloud.Run(
                    "sql", "instances", "describe", self._instance_id, "--format=json"
                ).decode("utf-8")
                sql_describe = json.loads(json_describe)
                return sql_describe["ipAddresses"][0].get("ipAddress")
            else:
                gcloud_create_params = [
                    "sql",
                    "instances",
                    "create",
                    self._instance_id,
                    "--region=us-central1",
                    f"--root-password={self._password}",
                    f"--database-version={self._database_type}",
                ]
                if self._enable_bin_logs:
                    gcloud_create_params.append("--enable-bin-log")
                if self._assign_public_ip:
                    gcloud_create_params.append("--assign-ip")
                if self._authorized_networks:
                    gcloud_create_params.append(
                        f"--authorized-networks={self._authorized_networks}"
                    )
                if self._cpu:
                    gcloud_create_params.append(f"--cpu={self._cpu}")
                if self._memory:
                    gcloud_create_params.append(f"--memory={self._memory}")

                db_info = gcloud.Run(*gcloud_create_params).decode("utf-8")
                self.db = dict(
                    zip(
                        db_info.strip().split("\n")[0].split(),
                        db_info.strip().split("\n")[1].split(),
                    )
                )
                print("CLOUDSQL_DB Info")
                print(self.db)

                gcloud.Run(
                    "sql",
                    "databases",
                    "create",
                    self._database_id,
                    f"--instance={self._instance_id}",
                )

                return self.db["PRIMARY_ADDRESS"]

    def add_data(self, gcs_data_path):
        """ Adds data to Cloud SQL database """
        if self._already_exists:
            return
        with GCloudContext(self._project_id) as gcloud:
            gcloud.Run(
                "sql",
                "import",
                "sql",
                self._instance_id,
                gcs_data_path,
                f"--database={self._database_id}",
                "--quiet",
            )

    def teardown(self):
        """ Deletes Cloud SQL instance """
        # If instance is deleted per integration test, instance_id will need a random
        # suffix appended since Cloud SQL cannot re-use the same instance name until
        # 1 week after deletion.
        with GCloudContext(self._project_id) as gcloud:
            gcloud.Run("--quiet", "sql", "instances", "delete", self._instance_id)

    def _get_random_string(self, length=5):
        """ Returns random string
        Args:
            length (int): Desired length of random string"""
        return "".join(random.choice(string.ascii_lowercase) for i in range(length))
