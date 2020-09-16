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


import pandas
import warnings

from ibis.bigquery.client import BigQueryClient
import ibis.pandas
from ibis.sql.mysql.client import MySQLClient
from ibis.sql.postgres.client import PostgreSQLClient

from third_party.ibis.ibis_impala.api import impala_connect

# TODO(googleapis/google-auth-library-python#520): Remove after issue is resolved
warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)
warnings.filterwarnings(
    "ignore", "Cannot create BigQuery Storage client, the dependency google-cloud-bigquery-storage is not installed"
)

# If you have a Teradata License there is an optional teradatasql import
try:
    from third_party.ibis.ibis_teradata.client import TeradataClient
except Exception:
    TeradataClient = None


def get_pandas_client(table_name, file_path, file_type):
    """ Return pandas client and env with file loaded into DataFrame

        table_name (str): Table name to use as reference for file data
        file_path (str): The local, s3, or GCS file path to the data
        file_type (str): The file type of the file (csv or json)
    """
    if file_type == "csv":
        df = pandas.read_csv(file_path)
    elif file_type == "json":
        df = pandas.read_json(file_path)
    else:
        raise ValueError(f"Unknown Pandas File Type: {file_type}")

    pandas_client = ibis.pandas.connect({table_name: df})

    return pandas_client


CLIENT_LOOKUP = {
    "BigQuery": BigQueryClient,
    "Impala": impala_connect,
    "MySQL": MySQLClient,
    "Postgres": PostgreSQLClient,
    "Teradata": TeradataClient,
    "Pandas": get_pandas_client,
}
