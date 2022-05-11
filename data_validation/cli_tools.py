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
"""The Data Validation CLI tool is intended to help to build and execute
data validation runs with ease.

The Data Validator can be called either using:
    data-validation -h
    python -m data_validation -h

ex.
Step 1) Store Connection to be used in validation
data-validation connections add -c my_bq_conn BigQuery --project-id pso-kokoro-resources

Step 2) Run Validation using supplied connections
data-validation validate column -sc my_bq_conn -tc my_bq_conn \
-tbls bigquery-public-data.new_york_citibike.citibike_trips,bigquery-public-data.new_york_citibike.citibike_stations \
--sum '*' --count '*'

python -m data_validation validate column -sc my_bq_conn -tc my_bq_conn \
-tbls bigquery-public-data.new_york_citibike.citibike_trips \
--grouped-columns starttime \
--sum tripduration --count tripduration

data-validation validate column \
-sc my_bq_conn -tc my_bq_conn \
-tbls bigquery-public-data.new_york_citibike.citibike_trips,bigquery-public-data.new_york_citibike.citibike_stations \
--sum tripduration,start_station_name --count tripduration,start_station_name \
-bqrh pso-kokoro-resources.pso_data_validator.results
-c ex_yaml.yaml

data-validation run-config -c ex_yaml.yaml
"""

import argparse
import csv
import json
import sys
import uuid

from data_validation import consts
from data_validation import state_manager

CONNECTION_SOURCE_FIELDS = {
    "BigQuery": [
        ["project_id", "GCP Project to use for BigQuery"],
        ["google_service_account_key_path", "(Optional) GCP SA Key Path"],
    ],
    "Teradata": [
        ["host", "Desired Teradata host"],
        ["port", "Teradata port to connect on"],
        ["user_name", "User used to connect"],
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
    "MySQL": [
        ["host", "Desired MySQL host (default localhost)"],
        ["port", "MySQL port to connect on (default 3306)"],
        ["user", "User used to connect"],
        ["password", "Password for supplied user"],
        ["database", "Database to connect to (default master)"],
    ],
    "Snowflake": [
        ["user", "Username to connect to"],
        ["password", "Password for authentication of user"],
        ["account", "Snowflake account to connect to"],
        ["database", "Database in snowflake to connect to"],
        ["schema", "Schema in the database to connect to"],
    ],
    "Postgres": [
        ["host", "Desired Postgres host."],
        ["port", "Postgres port to connect on (ie. 5432)"],
        ["user", "Username to connect to"],
        ["password", "Password for authentication of user"],
        ["database", "Database in postgres to connect to (default postgres)"],
    ],
    "Redshift": [
        ["host", "Desired Postgres host."],
        ["port", "Postgres port to connect on (ie. 5439)"],
        ["user", "Username to connect to"],
        ["password", "Password for authentication of user"],
        ["database", "Database in postgres to connect to (default postgres)"],
    ],
    "Spanner": [
        ["project_id", "GCP Project to use for Spanner"],
        ["instance_id", "ID of Spanner instance to connect to"],
        ["database_id", "ID of Spanner database (schema) to connect to"],
        ["google_service_account_key_path", "(Optional) GCP SA Key Path"],
    ],
    "FileSystem": [
        ["table_name", "Table name to use as reference for file data"],
        ["file_path", "The local, s3, or GCS file path to the data"],
        ["file_type", "The file type of the file.'csv' or 'json'"],
    ],
    "Impala": [
        ["host", "Desired Impala host"],
        ["port", "Desired Imapala port (10000 if not provided)"],
        ["database", "Desired Impala database (default if not provided)"],
        ["auth_mechanism", "Desired Impala auth mechanism (PLAIN if not provided)"],
        [
            "kerberos_service_name",
            "Desired Kerberos service name ('impala' if not provided)",
        ],
    ],
    "DB2": [
        ["host", "Desired DB2 host"],
        ["port", "Desired DB2 port (50000 if not provided)"],
        ["user", "Username to connect to"],
        ["password", "Password for authentication of user"],
        ["database", "Database in DB2 to connect to"],
        ["url", "URL link in DB2 to connect to"],
        ["driver", "Driver link in DB2 to connect to (default ibm_db_sa)"],
    ],
}


def get_parsed_args():
    """Return ArgParser with configured CLI arguments."""
    parser = configure_arg_parser()
    return parser.parse_args()


def configure_arg_parser():
    """Extract Args for Run."""
    parser = argparse.ArgumentParser(
        usage=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    subparsers = parser.add_subparsers(dest="command")
    _configure_validate_parser(subparsers)
    _configure_run_config_parser(subparsers)
    _configure_validation_config_parser(subparsers)
    _configure_connection_parser(subparsers)
    _configure_find_tables(subparsers)
    _configure_raw_query(subparsers)
    _configure_run_parser(subparsers)
    _configure_beta_parser(subparsers)
    return parser


def _configure_beta_parser(subparsers):
    """Configure beta commands for the parser."""
    connection_parser = subparsers.add_parser(
        "beta", help="Run a Beta command for new utilities and features."
    )
    beta_subparsers = connection_parser.add_subparsers(dest="beta_cmd")

    _configure_run_parser(beta_subparsers)
    _configure_validate_parser(beta_subparsers)
    _configure_deploy(beta_subparsers)


def _configure_deploy(subparsers):
    """Configure arguments for deploying as a service."""
    subparsers.add_parser(
        "deploy", help="Deploy Data Validation as a Service (w/ Flask)"
    )


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
    find_tables_parser.add_argument(
        "--allowed-schemas", "-as", help="List of source schemas to match."
    )
    find_tables_parser.add_argument(
        "--score-cutoff",
        "-score",
        type=float,
        help="The minimum distance score allowed to match tables (0 to 1).",
    )


def _configure_raw_query(subparsers):
    """Configure arguments for text search table matching."""
    query_parser = subparsers.add_parser(
        "query", help="Run an adhoc query against the supplied connection"
    )
    query_parser.add_argument("--conn", "-c", help="Connection name to query")
    query_parser.add_argument("--query", "-q", help="Raw query to execute")


def _configure_run_config_parser(subparsers):
    """Configure arguments to run a data validation YAML config using the legacy run-config command."""
    run_config_parser = subparsers.add_parser(
        "run-config",
        help="Run validations stored in a YAML config file. Note: the 'configs run' command is now the recommended approach",
    )

    run_config_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File Path to be used for building or running validations.",
    )


def _configure_validation_config_parser(subparsers):
    """Configure arguments to run a data validation YAML config."""
    validation_config_parser = subparsers.add_parser(
        "configs", help="Run validations stored in a YAML config file"
    )
    configs_subparsers = validation_config_parser.add_subparsers(
        dest="validation_config_cmd"
    )
    _ = configs_subparsers.add_parser("list", help="List your validation configs")
    run_parser = configs_subparsers.add_parser(
        "run", help="Run your validation configs"
    )
    run_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File Path to be used for building or running validations.",
    )

    get_parser = configs_subparsers.add_parser(
        "get", help="Get and print a validation config"
    )
    get_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File Path to be used for building or running validations.",
    )


def _configure_run_parser(subparsers):
    """Configure arguments to run a data validation."""

    # subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run", help="Run a validation and optionally store to config (deprecated)"
    )

    run_parser.add_argument(
        "--type",
        "-t",
        help="Type of Data Validation (Column, GroupedColumn, Row, Schema)",
    )
    run_parser.add_argument("--source-conn", "-sc", help="Source connection name")
    run_parser.add_argument("--target-conn", "-tc", help="Target connection name")
    run_parser.add_argument(
        "--tables-list",
        "-tbls",
        help="Comma separated tables list in the form 'schema.table=target_schema.target_table'",
    )
    run_parser.add_argument(
        "--result-handler-config", "-rc", help="Result handler config details"
    )
    run_parser.add_argument(
        "--bq-result-handler", "-bqrh", help="BigQuery result handler config details"
    )
    run_parser.add_argument(
        "--config-file",
        "-c",
        help="Store the validation in the YAML Config File Path specified",
    )
    run_parser.add_argument(
        "--labels",
        "-l",
        help="Key value pair labels for validation run",
    )
    run_parser.add_argument(
        "--hash",
        "-hash",
        help="Comma separated list of columns for hash 'col_a,col_b' or * for all columns",
    )
    run_parser.add_argument(
        "--service-account",
        "-sa",
        help="Path to SA key file for result handler output",
    )
    run_parser.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    run_parser.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    run_parser.add_argument(
        "--format",
        "-fmt",
        default="table",
        help="Set the format for printing command output, Supported formats are (text, csv, json, table). It defaults "
        "to table",
    )
    run_parser.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    run_parser.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )
    run_parser.add_argument(
        "--wildcard-include-string-len",
        "-wis",
        action="store_true",
        help="Include string fields for wildcard aggregations.",
    )
    run_parser.add_argument(
        "--cast-to-bigint",
        "-ctb",
        action="store_true",
        help="Cast any int32 fields to int64 for large aggregations.",
    )


def _configure_connection_parser(subparsers):
    """Configure the Parser for Connection Management."""
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


def _configure_validate_parser(subparsers):
    """Configure arguments to run validations."""
    validate_parser = subparsers.add_parser(
        "validate", help="Run a validation and optionally store to config"
    )

    # Keep these in order to support data-validation run command for backwards-compatibility
    validate_parser.add_argument("--type", "-t", help="Type of Data Validation")
    validate_parser.add_argument(
        "--result-handler-config", "-rc", help="Result handler config details"
    )

    validate_subparsers = validate_parser.add_subparsers(dest="validate_cmd")

    column_parser = validate_subparsers.add_parser(
        "column", help="Run a column validation"
    )
    _configure_column_parser(column_parser)

    row_parser = validate_subparsers.add_parser("row", help="Run a row validation")
    _configure_row_parser(row_parser)

    schema_parser = validate_subparsers.add_parser(
        "schema", help="Run a schema validation"
    )
    _configure_schema_parser(schema_parser)

    custom_query_parser = validate_subparsers.add_parser(
        "custom-query", help="Run a custom query validation"
    )
    _configure_custom_query_parser(custom_query_parser)


def _configure_row_parser(row_parser):
    """Configure arguments to run row level validations."""
    _add_common_arguments(row_parser)
    row_parser.add_argument(
        "--hash",
        "-hash",
        help="Comma separated list of columns for hash 'col_a,col_b' or * for all columns",
    )
    row_parser.add_argument(
        "--comparison-fields",
        "-comp-fields",
        help="Individual columns to compare. If comparing a calculated field use the column alias.",
    )
    row_parser.add_argument(
        "--primary-keys",
        "-pk",
        required=True,
        help="Comma separated list of primary key columns 'col_a,col_b'",
    )
    row_parser.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    row_parser.add_argument(
        "--grouped-columns",
        "-gc",
        help="Comma separated list of columns to use in GroupBy 'col_a,col_b'",
    )
    row_parser.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    row_parser.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    row_parser.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )


def _configure_column_parser(column_parser):
    """Configure arguments to run column level validations."""
    _add_common_arguments(column_parser)
    column_parser.add_argument(
        "--count",
        "-count",
        help="Comma separated list of columns for count 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--sum",
        "-sum",
        help="Comma separated list of columns for sum 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--avg",
        "-avg",
        help="Comma separated list of columns for avg 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--min",
        "-min",
        help="Comma separated list of columns for min 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--max",
        "-max",
        help="Comma separated list of columns for max 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--hash",
        "-hash",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--bit_xor",
        "-bit_xor",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    column_parser.add_argument(
        "--comparison-fields",
        "-comp-fields",
        help="list of fields to perform exact comparisons to. Use column aliases if this is calculated.",
    )
    column_parser.add_argument(
        "--grouped-columns",
        "-gc",
        help="Comma separated list of columns to use in GroupBy 'col_a,col_b'",
    )
    column_parser.add_argument(
        "--primary-keys",
        "-pk",
        help="Comma separated list of primary key columns 'col_a,col_b'",
    )
    column_parser.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    column_parser.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    column_parser.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    column_parser.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )
    column_parser.add_argument(
        "--wildcard-include-string-len",
        "-wis",
        action="store_true",
        help="Include string fields for wildcard aggregations.",
    )
    column_parser.add_argument(
        "--cast-to-bigint",
        "-ctb",
        action="store_true",
        help="Cast any int32 fields to int64 for large aggregations.",
    )


def _configure_schema_parser(schema_parser):
    """Configure arguments to run column level validations."""
    _add_common_arguments(schema_parser)


def _configure_custom_query_parser(custom_query_parser):
    """Configure arguments to run custom-query validations."""
    _add_common_arguments(custom_query_parser)
    custom_query_parser.add_argument(
        "--custom-query-type",
        "-cqt",
        required=True,
        help="Which type of custom query (row/column)",
    )
    custom_query_parser.add_argument(
        "--source-query-file",
        "-sqf",
        help="File containing the source sql query",
    )
    custom_query_parser.add_argument(
        "--target-query-file",
        "-tqf",
        help="File containing the target sql query",
    )
    custom_query_parser.add_argument(
        "--count",
        "-count",
        help="Comma separated list of columns for count 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--sum",
        "-sum",
        help="Comma separated list of columns for sum 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--avg",
        "-avg",
        help="Comma separated list of columns for avg 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--min",
        "-min",
        help="Comma separated list of columns for min 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--max",
        "-max",
        help="Comma separated list of columns for max 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--bit_xor",
        "-bit_xor",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--hash",
        "-hash",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    custom_query_parser.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    custom_query_parser.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    custom_query_parser.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    custom_query_parser.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )
    custom_query_parser.add_argument(
        "--primary-keys",
        "-pk",
        help="Comma separated list of primary key columns 'col_a,col_b'",
    )
    custom_query_parser.add_argument(
        "--wildcard-include-string-len",
        "-wis",
        action="store_true",
        help="Include string fields for wildcard aggregations.",
    )
    custom_query_parser.add_argument(
        "--cast-to-bigint",
        "-ctb",
        action="store_true",
        help="Cast any int32 fields to int64 for large aggregations.",
    )


def _add_common_arguments(parser):
    parser.add_argument(
        "--source-conn", "-sc", required=True, help="Source connection name"
    )
    parser.add_argument(
        "--target-conn", "-tc", required=True, help="Target connection name"
    )
    parser.add_argument(
        "--tables-list",
        "-tbls",
        help="Comma separated tables list in the form 'schema.table=target_schema.target_table'",
    )
    parser.add_argument(
        "--bq-result-handler", "-bqrh", help="BigQuery result handler config details"
    )
    parser.add_argument(
        "--labels", "-l", help="Key value pair labels for validation run"
    )
    parser.add_argument(
        "--service-account",
        "-sa",
        help="Path to SA key file for result handler output",
    )
    parser.add_argument(
        "--config-file",
        "-c",
        help="Store the validation in the YAML Config File Path specified",
    )
    parser.add_argument(
        "--format",
        "-fmt",
        default="table",
        help="Set the format for printing command output, Supported formats are (text, csv, json, table). Defaults "
        "to table",
    )


def get_connection_config_from_args(args):
    """Return dict with connection config supplied."""
    config = {consts.SOURCE_TYPE: args.connect_type}

    if args.connect_type == "Raw":
        return json.loads(args.json)

    for field_obj in CONNECTION_SOURCE_FIELDS[args.connect_type]:
        field = field_obj[0]
        config[field] = getattr(args, field)

    return config


def threshold_float(x):
    """Restrict threshold arg to be a positive float."""
    try:
        x = float(x)
    except ValueError:
        raise argparse.ArgumentTypeError("%r not a floating-point literal" % (x,))

    if x < 0.0 or x > sys.float_info.max:
        raise argparse.ArgumentTypeError(
            "%r must be positive and below the max float value" % (x,)
        )
    elif x != x:
        raise argparse.ArgumentTypeError("%r must be a number" % (x,))
    return x


# def _get_data_validation_directory():
#     raw_dir_path = (
#         os.environ.get(consts.ENV_DIRECTORY_VAR) or consts.DEFAULT_ENV_DIRECTORY
#     )
#     dir_path = os.path.expanduser(raw_dir_path)
#     if not os.path.exists(dir_path):
#         os.makedirs(dir_path)
#     return dir_path

# def _get_connection_file(connection_name):
#     dir_path = _get_data_validation_directory()
#     file_name = f"{connection_name}.connection.json"
#     return os.path.join(dir_path, file_name)


def _generate_random_name(conn):
    name = f"{conn[consts.SOURCE_TYPE]}_{str(uuid.uuid4())}"
    return name


def store_connection(connection_name, conn):
    """Store the connection config under the given name."""
    mgr = state_manager.StateManager()
    mgr.create_connection(connection_name, conn)


#     connection_name = connection_name or _generate_random_name(conn)
#     file_path = _get_connection_file(connection_name)

#     with open(file_path, "w") as file:
#         file.write(json.dumps(conn))

# def get_connections():
#     """ Return dict with connection name and path key pairs."""
#     connections = {}

#     dir_path = _get_data_validation_directory()
#     all_config_files = os.listdir(dir_path)
#     for config_file in all_config_files:
#         if config_file.endswith(".connection.json"):
#             config_file_path = os.path.join(dir_path, config_file)
#             conn_name = config_file.split(".")[0]

#             connections[conn_name] = config_file_path

#     return connections


def list_connections():
    """List all saved connections."""
    mgr = state_manager.StateManager()
    connections = mgr.list_connections()

    for conn_name in connections:
        print(f"Connection Name: {conn_name}")


def get_connection(connection_name):
    """Return dict connection details for a specific connection."""
    mgr = state_manager.StateManager()
    return mgr.get_connection_config(connection_name)


#     with open(file_path, "r") as file:
#         conn_str = file.read()
#     return json.loads(conn_str)


def store_validation(validation_file_name, yaml_config):
    """Store the validation YAML config under the given name."""
    mgr = state_manager.StateManager()
    mgr.create_validation_yaml(validation_file_name, yaml_config)


def get_validation(validation_name):
    """Return validation YAML for a specific connection."""
    mgr = state_manager.StateManager()
    return mgr.get_validation_config(validation_name)


def list_validations():
    """List all saved validation YAMLs."""
    mgr = state_manager.StateManager()
    validations = mgr.list_validations()

    print("Validation YAMLs found:")
    for validation_name in validations:
        print(f"{validation_name}.yaml")


def get_labels(arg_labels):
    """Return list of tuples representing key-value label pairs."""
    labels = []
    if arg_labels:
        pairs = arg_labels.split(",")
        for pair in pairs:
            kv = pair.split("=")
            if len(kv) == 2:
                labels.append((kv[0], kv[1]))
            else:
                raise ValueError("Labels must be comma-separated key-value pairs.")
    return labels


def get_filters(filter_value):
    """Returns parsed JSON from filter file. Backwards compatible for JSON input.

    filter_value (str): Filter argument specified.
    """
    try:
        filter_config = json.loads(filter_value)
    except json.decoder.JSONDecodeError:
        filter_config = []
        filter_vals = filter_value.split(":")
        if len(filter_vals) == 1:
            filter_dict = {
                "type": "custom",
                "source": filter_vals[0],
                "target": filter_vals[0],
            }
        elif len(filter_vals) == 2:
            if not filter_vals[1]:
                raise ValueError("Please provide valid target filter.")
            filter_dict = {
                "type": "custom",
                "source": filter_vals[0],
                "target": filter_vals[1],
            }
        else:
            raise ValueError("Unable to parse filter arguments.")
        filter_config.append(filter_dict)
    return filter_config


def get_result_handler(rc_value, sa_file=None):
    """Returns dict of result handler config. Backwards compatible for JSON input.

    rc_value (str): Result config argument specified.
    sa_file (str): SA path argument specified.
    """
    try:
        result_handler = json.loads(rc_value)
    except json.decoder.JSONDecodeError:
        config = rc_value.split(".", 1)
        if len(config) == 2:
            result_handler = {
                "type": "BigQuery",
                "project_id": config[0],
                "table_id": config[1],
            }
        else:
            raise ValueError(f"Unable to parse result handler config: `{rc_value}`")

        if sa_file:
            result_handler["google_service_account_key_path"] = sa_file

    return result_handler


def get_arg_list(arg_value, default_value=None):
    """Returns list of values from argument provided. Backwards compatible for JSON input.

    arg_value (str): Argument supplied
    default_value (Any): A default value to supply when arg_value is empty.
    """
    if not arg_value:
        return default_value

    try:
        if isinstance(arg_value, list):
            arg_value = str(arg_value)
        # arg_value = "hash_all"
        arg_list = json.loads(arg_value)
    except json.decoder.JSONDecodeError:
        arg_list = arg_value.split(",")
    return arg_list


def get_tables_list(arg_tables, default_value=None, is_filesystem=False):
    """Returns dictionary of tables. Backwards compatible for JSON input.

    arg_table (str): tables_list argument specified
    default_value (Any): A default value to supply when arg_value is empty.
    is_filesystem (boolean): Boolean indicating whether source connection is a FileSystem. In this case, a schema is not required.
    """
    if not arg_tables:
        return default_value

    try:
        # Backwards compatibility for JSON input
        tables_list = json.loads(arg_tables)
    except json.decoder.JSONDecodeError:
        tables_list = []
        tables_mapping = list(csv.reader([arg_tables]))[0]
        source_schema_required = False if is_filesystem else True

        for mapping in tables_mapping:
            tables_map = mapping.split("=")
            if len(tables_map) == 1:
                schema, table = split_table(
                    tables_map, schema_required=source_schema_required
                )
                table_dict = {
                    "schema_name": schema,
                    "table_name": table,
                }
            elif len(tables_map) == 2:
                src_schema, src_table = split_table(
                    [tables_map[0]], schema_required=source_schema_required
                )

                table_dict = {
                    "schema_name": src_schema,
                    "table_name": src_table,
                }

                targ_schema, targ_table = split_table(
                    [tables_map[1]], schema_required=False
                )

                if targ_schema:
                    table_dict["target_schema_name"] = targ_schema
                table_dict["target_table_name"] = targ_table

            else:
                raise ValueError(
                    "Unable to parse tables list. Please provide valid mapping."
                )

            tables_list.append(table_dict)

    return tables_list


def split_table(table_ref, schema_required=True):
    """Returns schema and table name given list of input values.

    table_ref (List): Table reference i.e ['my.schema.my_table']
    schema_required (boolean): Indicates whether schema is required. A source
    table reference requires schema. A target table reference does not.
    """
    table_ref_list = list(csv.reader(table_ref, delimiter=".", quotechar='"'))[0]

    if len(table_ref_list) == 1 and schema_required:
        raise ValueError("Please provide schema in tables list.")
    elif len(table_ref_list) == 1:
        return None, table_ref_list[0].strip()

    table = table_ref_list.pop()
    schema = ".".join(table_ref_list)
    return schema.strip(), table.strip()
