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


""" The Data Validation CLI tool is intended to help to build and execute
data validation runs with ease.

The Data Validator can be called either using:
    data-validation -h
    python -m data_validation -h

ex.
Step 1) Store Connection to be used in validation
data-validation connections store -c my_bq_conn -t BigQuery -p pso-kokoro-resources

Step 2) Run Validation using supplied connections
data-validation run -t Column -sc my_bq_conn -tc my_bq_conn \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"},{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' \
--sum '*' --count '*'

python -m data_validation run -t GroupedColumn -sc my_bq_conn -tc my_bq_conn \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' \
--grouped-columns '["starttime"]' \
--sum '["tripduration"]' --count '["tripduration"]'

data-validation run -t Column \
-sc my_bq_conn -tc my_bq_conn \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"},{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' \
--sum '["tripduration","start_station_name"]' --count '["tripduration","start_station_name"]' \
-rc '{"project_id":"pso-kokoro-resources","type":"BigQuery","table_id":"pso_data_validator.results"}'
-c ex_yaml.yaml

data-validation run-config -c ex_yaml.yaml
"""

import argparse
import json
import os
import uuid

from data_validation import consts


CONNECTION_SOURCE_FIELDS = {
    "BigQuery": ["project_id"],
    "Teradata": ["host", "port", "user", "password"],
}


def get_parsed_args():
    """ Return ArgParser with configured CLI arguments."""
    parser = configure_arg_parser()
    return parser.parse_args()


def configure_arg_parser():
    """Extract Args for Run."""
    parser = argparse.ArgumentParser(
        usage=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run", help="Manually run a validation and optionally store to config"
    )
    run_config_parser = subparsers.add_parser(
        "run-config", help="Run validations stored in a YAML config file"
    )
    connection_parser = subparsers.add_parser(
        "connections", help="Manage & Store connections to your Databases"
    )

    _configure_run_parser(run_parser)
    _configure_run_config_parser(run_config_parser)
    _configure_connection_parser(connection_parser)

    return parser


def _configure_run_config_parser(run_config_parser):
    """ Configure arguments to run a data validation YAML config."""
    run_config_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File Path to be used for building or running validations.",
    )


def _configure_run_parser(run_parser):
    """ Configure arguments to run a data validation."""
    run_parser.add_argument(
        "--type", "-t", help="Type of Data Validation (Column, GroupedColumn)"
    )
    run_parser.add_argument("--source-conn", "-sc", help="Source connection name")
    run_parser.add_argument("--target-conn", "-tc", help="Target connection name")
    run_parser.add_argument(
        "--tables-list",
        "-tbls",
        help="JSON List of dict {schema:schema_name, table:table_name}",
    )
    run_parser.add_argument(
        "--count",
        "-count",
        help="JSON List of columns sum '[\"col_a\"]' or * for all columns",
    )
    run_parser.add_argument(
        "--sum",
        "-sum",
        help="JSON List of columns sum '[\"col_a\"]' or * for all numeric",
    )
    run_parser.add_argument(
        "--grouped-columns",
        "-gc",
        help="JSON List of columns to use in group by '[\"col_a\"]'",
    )
    run_parser.add_argument(
        "--result-handler-config", "-rc", help="Result handler config details"
    )
    run_parser.add_argument(
        "--config-file",
        "-c",
        help="Store the validation in the YAML Config File Path specified.",
    )


def _configure_connection_parser(connection_parser):
    """ Configure the Parser for Connection Management. """
    subparsers = connection_parser.add_subparsers(dest="connect_cmd")
    _ = subparsers.add_parser("list", help="List your connections")
    store_parser = subparsers.add_parser("store", help="Store a new connection")

    store_parser.add_argument(
        "--connection-name", "-c", help="Name of connection used as reference"
    )
    store_parser.add_argument(
        "--type", "-t", help="Source Type (BigQuery, Teradata, MySQL)"
    )
    store_parser.add_argument("--project-id", "-p", help="GCP Project ID")
    store_parser.add_argument("--json", "-j", help="(Optional) Json string config")


def get_connection_config_from_args(args):
    """ Return dict with connection config supplied."""
    config = {consts.SOURCE_TYPE: args.type}

    if args.json:
        return json.loads(args.json)

    for field in CONNECTION_SOURCE_FIELDS[args.type]:
        config[field] = getattr(args, field)

    return config


def _get_data_validation_directory():
    dir_path = os.environ.get(consts.ENV_DIRECTORY_VAR) or consts.DEFAULT_ENV_DIRECTORY
    if not dir_path.endswith("/"):
        dir_path = dir_path + "/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path


def _generate_random_name(conn):
    name = f"{conn[consts.SOURCE_TYPE]}_{str(uuid.uuid4())}"
    return name


def store_connection(connection_name, conn):
    """ Store the connection config under the given name."""
    dir_path = _get_data_validation_directory()
    connection_name = connection_name or _generate_random_name(conn)
    file_path = f"{dir_path}{connection_name}.connection.json"

    with open(file_path, "w") as file:
        file.write(json.dumps(conn))


def list_connections(args):
    """ List all saved connections."""
    dir_path = _get_data_validation_directory()
    all_config_files = os.listdir(dir_path)

    for config_file in all_config_files:
        if config_file.endswith(".connection.json"):
            conn_name = config_file.split(".")[0]
            print(f"Connection Name: {conn_name}")


def get_connection(connection_name):
    """ Return dict connection details for a specific connection."""
    dir_path = _get_data_validation_directory()
    file_path = f"{dir_path}{connection_name}.connection.json"

    with open(file_path, "r") as file:
        conn_str = file.read()

    return json.loads(conn_str)
