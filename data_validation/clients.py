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

from google.cloud import bigquery
from ibis.backends.bigquery.client import BigQueryClient
import ibis.backends.pandas
from ibis.backends.pandas.client import PandasClient
from ibis.backends.mysql.client import MySQLClient
from ibis.backends.postgres.client import PostgreSQLClient

from third_party.ibis.ibis_impala.api import impala_connect
import third_party.ibis.ibis_addon.datatypes
from data_validation import client_info


# Our customized Ibis Datatype logic add support for new types
third_party.ibis.ibis_addon.datatypes

# TODO(googleapis/google-auth-library-python#520): Remove after issue is resolved
warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)
warnings.filterwarnings(
    "ignore",
    "Cannot create BigQuery Storage client, the dependency google-cloud-bigquery-storage is not installed",
)
warnings.filterwarnings(
    "ignore", "The GenericFunction 'regex_extract' is already registered"
)


def _raise_missing_client_error(msg):
    def get_client_call(*args, **kwargs):
        raise Exception(msg)

    return get_client_call


# If you have a Teradata License there is an optional teradatasql import
try:
    from third_party.ibis.ibis_teradata.client import TeradataClient
except Exception:
    msg = "pip install teradatasql (requires Teradata licensing)"
    TeradataClient = _raise_missing_client_error(msg)

# If you have an cx_Oracle driver installed
try:
    from third_party.ibis.ibis_oracle.client import OracleClient
except Exception:
    OracleClient = _raise_missing_client_error("pip install cx_Oracle")

try:
    from third_party.ibis.ibis_mssql import connect as mssql_connect
except Exception:
    mssql_connect = _raise_missing_client_error("pip install pymssql")

try:
    from third_party.ibis.ibis_snowflake.client import (
        SnowflakeClient as snowflake_connect,
    )
except Exception:
    snowflake_connect = _raise_missing_client_error(
        "pip install snowflake-connector-python"
    )


def get_bigquery_client(project_id, dataset_id=None, credentials=None):
    info = client_info.get_http_client_info()
    google_client = bigquery.Client(
        project=project_id, client_info=info, credentials=credentials
    )
    ibis_client = BigQueryClient(
        project_id, dataset_id=dataset_id, credentials=credentials
    )

    # Override the BigQuery client object to ensure the correct user agent is
    # included.
    ibis_client.client = google_client
    return ibis_client


def get_pandas_client(table_name, file_path, file_type):
    """Return pandas client and env with file loaded into DataFrame

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

    pandas_client = ibis.backends.pandas.connect({table_name: df})

    return pandas_client


def get_ibis_table(client, schema_name, table_name, database_name=None):
    """Return Ibis Table for Supplied Client.

    client (IbisClient): Client to use for table
    schema_name (str): Schema name of table object
    table_name (str): Table name of table object
    database_name (str): Database name (generally default is used)
    """
    if type(client) in [OracleClient, PostgreSQLClient]:
        return client.table(table_name, database=database_name, schema=schema_name)
    elif type(client) in [PandasClient]:
        return client.table(table_name, schema=schema_name)
    else:
        return client.table(table_name, database=schema_name)


def list_schemas(client):
    """Return a list of schemas in the DB."""
    if type(client) in [OracleClient, PostgreSQLClient]:
        return client.list_schemas()
    elif hasattr(client, "list_databases"):
        return client.list_databases()
    else:
        return [None]


def list_tables(client, schema_name):
    """Return a list of tables in the DB schema."""
    if type(client) in [OracleClient, PostgreSQLClient]:
        return client.list_tables(schema=schema_name)
    elif schema_name:
        return client.list_tables(database=schema_name)
    else:
        return client.list_tables()


def get_all_tables(client):
    """Return a list of tuples with database and table names.

    client (IbisClient): Client to use for tables
    """
    table_objs = []
    schemas = list_schemas(client)

    for schema_name in schemas:
        try:
            tables = list_tables(client, schema_name)
        except Exception as e:
            print(f"List Tables Error: {schema_name} -> {e}")
            continue

        for table_name in tables:
            table_objs.append((schema_name, table_name))

    return table_objs


CLIENT_LOOKUP = {
    "BigQuery": get_bigquery_client,
    "Impala": impala_connect,
    "MySQL": MySQLClient,
    "Oracle": OracleClient,
    "Pandas": get_pandas_client,
    "Postgres": PostgreSQLClient,
    "Redshift": PostgreSQLClient,
    "Teradata": TeradataClient,
    "MSSQL": mssql_connect,
    "Snowflake": snowflake_connect,
}
