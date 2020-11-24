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
data-validation connections add -c my_bq_conn BigQuery --project-id pso-kokoro-resources

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
    "BigQuery": [
        ["project_id", "GCP Project to use for BigQuery"],
        ["google_service_account_key_path", "(Optional) GCP SA Key Path"],
    ],
    "Teradata": [
        ["host", "Desired Teradata host"],
        ["port", "Teradata port to connect on"],
        ["user", "User used to connect"],
        ["password", "Password for supplied user"],
    ],
    "Oracle": [
        ["host", "Desired Oracle host"],
        ["port", "Oracle port to connect on"],
        ["user", "User used to connect"],
        ["password", "Password for supplied user"],
        ["database", "Database to connect to"],
    ],
    "MSSQL": [
        ["host", "Desired SQL Server host (default localhost)"],
        ["port", "SQL Server port to connect on (default 1433)"],
        ["user", "User used to connect"],
        ["password", "Password for supplied user"],
        ["database", "Database to connect to (default master)"],
    ],
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

    _configure_run_parser(subparsers)
    _configure_run_config_parser(subparsers)
    _configure_connection_parser(subparsers)
    _configure_find_tables(subparsers)

    return parser


def _configure_find_tables(subparsers):
    """Configure arguments for text search table matching."""
    find_tables_parser = subparsers.add_parser(
        "find-tables", help="Build tables list using approx string matching"
    )
    find_tables_parser.add_argument(
        "--source-conn", "-sc", help="Source connection name"
    )
    find_tables_parser.add_argument(
        "--target-conn", "-tc", help="Target connection name"
    )


def _configure_run_config_parser(subparsers):
    """ Configure arguments to run a data validation YAML config."""
    run_config_parser = subparsers.add_parser(
        "run-config", help="Run validations stored in a YAML config file"
    )
    run_config_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File Path to be used for building or running validations.",
    )


def _configure_run_parser(subparsers):
    """ Configure arguments to run a data validation."""
    run_parser = subparsers.add_parser(
        "run", help="Manually run a validation and optionally store to config"
    )

    run_parser.add_argument(
        "--type", "-t", help="Type of Data Validation (Column, GroupedColumn, Row)"
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
        help="JSON List of columns count '[\"col_a\"]' or * for all columns",
    )
    run_parser.add_argument(
        "--sum",
        "-sum",
        help="JSON List of columns sum '[\"col_a\"]' or * for all numeric",
    )
    run_parser.add_argument(
        "--avg",
        "-avg",
        help="JSON List of columns average '[\"col_a\"]' or * for all numeric",
    )
    run_parser.add_argument(
        "--min",
        "-min",
        help="JSON List of columns min '[\"col_a\"]' or * for all numeric",
    )
    run_parser.add_argument(
        "--max",
        "-max",
        help="JSON List of columns max '[\"col_a\"]' or * for all numeric",
    )

    run_parser.add_argument(
        "--grouped-columns",
        "-gc",
        help="JSON List of columns to use in group by '[\"col_a\"]'",
    )
    run_parser.add_argument(
        "--primary-keys",
        "-pk",
        help="JSON List of columns to use as primary keys '[\"id\"]'",
    )
    run_parser.add_argument(
        "--result-handler-config", "-rc", help="Result handler config details"
    )
    run_parser.add_argument(
        "--config-file",
        "-c",
        help="Store the validation in the YAML Config File Path specified.",
    )


def _configure_connection_parser(subparsers):
    """ Configure the Parser for Connection Management. """
    connection_parser = subparsers.add_parser(
        "connections", help="Manage & Store connections to your Databases"
    )
    connect_subparsers = connection_parser.add_subparsers(dest="connect_cmd")
    _ = connect_subparsers.add_parser("list", help="List your connections")

    add_parser = connect_subparsers.add_parser("add", help="Store a new connection")
    add_parser.add_argument(
        "--connection-name", "-c", help="Name of connection used as reference"
    )
    _configure_database_specific_parsers(add_parser)


def _configure_database_specific_parsers(parser):
    """Configure a separate subparser for each supported DB."""
    subparsers = parser.add_subparsers(dest="connect_type")

    raw_parser = subparsers.add_parser(
        "Raw", help="Supply Raw JSON config for a connection"
    )
    raw_parser.add_argument("--json", "-j", help="Json string config")

    for database in CONNECTION_SOURCE_FIELDS:
        db_parser = subparsers.add_parser(
            database, help=f"Store a {database} connection"
        )

        for field_obj in CONNECTION_SOURCE_FIELDS[database]:
            arg_field = "--" + field_obj[0].replace("_", "-")
            help_txt = field_obj[1]
            db_parser.add_argument(arg_field, help=help_txt)


def get_connection_config_from_args(args):
    """ Return dict with connection config supplied."""
    config = {consts.SOURCE_TYPE: args.connect_type}

    if args.connect_type == "Raw":
        return json.loads(args.json)

    for field_obj in CONNECTION_SOURCE_FIELDS[args.connect_type]:
        field = field_obj[0]
        config[field] = getattr(args, field)

    return config


def _get_data_validation_directory():
    raw_dir_path = (
        os.environ.get(consts.ENV_DIRECTORY_VAR) or consts.DEFAULT_ENV_DIRECTORY
    )
    dir_path = os.path.expanduser(raw_dir_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path


def _get_connection_file(connection_name):
    dir_path = _get_data_validation_directory()
    file_name = f"{connection_name}.connection.json"

    return os.path.join(dir_path, file_name)


def _generate_random_name(conn):
    name = f"{conn[consts.SOURCE_TYPE]}_{str(uuid.uuid4())}"
    return name


def store_connection(connection_name, conn):
    """ Store the connection config under the given name."""
    connection_name = connection_name or _generate_random_name(conn)
    file_path = _get_connection_file(connection_name)

    with open(file_path, "w") as file:
        file.write(json.dumps(conn))


def get_connections():
    """ Return dict with connection name and path key pairs."""
    connections = {}

    dir_path = _get_data_validation_directory()
    all_config_files = os.listdir(dir_path)
    for config_file in all_config_files:
        if config_file.endswith(".connection.json"):
            config_file_path = os.path.join(dir_path, config_file)
            conn_name = config_file.split(".")[0]

            connections[conn_name] = config_file_path

    return connections


def list_connections():
    """ List all saved connections."""
    connections = get_connections()

    for conn_name in connections:
        print(f"Connection Name: {conn_name}")


def get_connection(connection_name):
    """ Return dict connection details for a specific connection."""
    file_path = _get_connection_file(connection_name)
    with open(file_path, "r") as file:
        conn_str = file.read()

    return json.loads(conn_str)
