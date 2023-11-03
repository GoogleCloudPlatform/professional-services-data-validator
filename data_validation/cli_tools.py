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

command:
data-validation
"""

import argparse
import csv
import json
import logging
import sys
import uuid
from argparse import Namespace
from typing import Dict, List

from data_validation import clients, consts, state_manager

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
        ["logmech", "(Optional) Log on mechanism"],
        ["use_no_lock_tables", "Use an access lock for queries (defaults to False)"],
        ["json_params", "(Optional) Additional teradatasql JSON string parameters"],
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
        ["query", "Connection query parameters"],
        ["url", "SQL Server SQLAlchemy connection URL"],
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
        ["connect_args", "(Optional) Additional connection arg mapping"],
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
        ["file_type", "The file type of the file. 'csv' or 'json'"],
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
        ["use_ssl", "Use SSL when connecting to HiveServer2 (default is False)"],
        [
            "timeout",
            "Connection timeout in seconds when communicating with HiveServer2 (default is 45)",
        ],
        [
            "ca_cert",
            "Local path to 3rd party CA certificate or copy of server certificate for self-signed certificates. If SSL is enabled, but this argument is None, then certificate validation is skipped.",
        ],
        ["user", "LDAP user to authenticate"],
        ["password", "LDAP password to authenticate"],
        [
            "pool_size",
            "Size of the connection pool. Typically this is not necessary to configure. (default is 8)",
        ],
        ["hdfs_client", "An existing HDFS client"],
        ["use_http_transport", "Boolean if HTTP proxy is provided (default is False)"],
        ["http_path", "URL path of HTTP proxy"],
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


def get_parsed_args() -> Namespace:
    """Return ArgParser with configured CLI arguments."""
    parser = configure_arg_parser()
    args = ["--help"] if len(sys.argv) == 1 else None
    return parser.parse_args(args)


def configure_arg_parser():
    """Extract Args for Run."""
    parser = argparse.ArgumentParser(
        usage=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--log-level",
        "-ll",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log Level to be assigned. This will print logs with level same or above",
    )

    subparsers = parser.add_subparsers(dest="command")
    _configure_validate_parser(subparsers)
    _configure_validation_config_parser(subparsers)
    _configure_connection_parser(subparsers)
    _configure_find_tables(subparsers)
    _configure_raw_query(subparsers)
    _configure_beta_parser(subparsers)
    _configure_partition_parser(subparsers)
    return parser


def _configure_partition_parser(subparsers):
    """Configure arguments to generate partitioned config files."""
    partition_parser = subparsers.add_parser(
        "generate-table-partitions",
        help=(
            "Generate partitions for validation and store the Config files in "
            "a directory"
        ),
    )

    # Group all optional arguments together
    optional_arguments = partition_parser.add_argument_group("optional arguments")
    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )

    # Group all required arguments together
    required_arguments = partition_parser.add_argument_group("required arguments")
    required_arguments.add_argument(
        "--primary-keys",
        "-pk",
        required=True,
        help="Comma separated list of primary key columns 'col_a,col_b'",
    )
    required_arguments.add_argument(
        "--tables-list",
        "-tbls",
        required=True,
        help=(
            "Comma separated tables list in the form "
            "'schema.table=target_schema.target_table'"
        ),
    )

    # Group for mutually exclusive required arguments. Either must be supplied
    mutually_exclusive_arguments = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    mutually_exclusive_arguments.add_argument(
        "--hash",
        "-hash",
        help=(
            "Comma separated list of columns for hash 'col_a,col_b' or * for "
            "all columns"
        ),
    )
    mutually_exclusive_arguments.add_argument(
        "--concat",
        "-concat",
        help=(
            "Comma separated list of columns for concat 'col_a,col_b' or * "
            "for all columns"
        ),
    )

    mutually_exclusive_arguments.add_argument(
        "--comparison-fields",
        "-comp-fields",
        help=(
            "Individual columns to compare. If comparing a calculated field use "
            "the column alias."
        ),
    )

    _add_common_partition_arguments(optional_arguments, required_arguments)


def _configure_beta_parser(subparsers):
    """Configure beta commands for the parser."""
    connection_parser = subparsers.add_parser(
        "beta", help="Run a Beta command for new utilities and features."
    )
    beta_subparsers = connection_parser.add_subparsers(dest="beta_cmd")

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
        "--dry-run",
        "-dr",
        action="store_true",
        help="Prints source and target SQL to stdout in lieu of performing a validation.",
    )
    run_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File path to be used for building or running validations.",
    )
    run_parser.add_argument(
        "--config-dir",
        "-cdir",
        help="Directory path containing YAML Config Files to be used for running validations.",
    )

    get_parser = configs_subparsers.add_parser(
        "get", help="Get and print a validation config"
    )
    get_parser.add_argument(
        "--config-file",
        "-c",
        help="YAML Config File Path to be used for building or running validations.",
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
    add_parser.add_argument(
        "--secret-manager-type",
        "-sm",
        default=None,
        help="Secret manager type to store credentials by default will be None ",
    )
    add_parser.add_argument(
        "--secret-manager-project-id",
        "-sm-prj-id",
        default=None,
        help="Project ID for the secret manager that stores the credentials",
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

    validate_parser.add_argument(
        "--dry-run",
        "-dr",
        action="store_true",
        help="Prints source and target SQL to stdout in lieu of performing a validation.",
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

    # Group optional arguments
    optional_arguments = row_parser.add_argument_group("optional arguments")
    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    optional_arguments.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    optional_arguments.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )

    # Group required arguments
    required_arguments = row_parser.add_argument_group("required arguments")
    required_arguments.add_argument(
        "--tables-list",
        "-tbls",
        default=None,
        required=True,
        help="Comma separated tables list in the form 'schema.table=target_schema.target_table'",
    )
    required_arguments.add_argument(
        "--primary-keys",
        "-pk",
        required=True,
        help="Comma separated list of primary key columns 'col_a,col_b'",
    )

    # Group for mutually exclusive required arguments. Either must be supplied
    mutually_exclusive_arguments = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    mutually_exclusive_arguments.add_argument(
        "--hash",
        "-hash",
        help=(
            "Comma separated list of columns for hash 'col_a,col_b' or * for "
            "all columns"
        ),
    )
    mutually_exclusive_arguments.add_argument(
        "--concat",
        "-concat",
        help=(
            "Comma separated list of columns for concat 'col_a,col_b' or * "
            "for all columns"
        ),
    )

    mutually_exclusive_arguments.add_argument(
        "--comparison-fields",
        "-comp-fields",
        help=(
            "Individual columns to compare. If comparing a calculated field use "
            "the column alias."
        ),
    )
    _add_common_arguments(optional_arguments, required_arguments)


def _configure_column_parser(column_parser):
    """Configure arguments to run column level validations."""
    # Group optional arguments
    optional_arguments = column_parser.add_argument_group("optional arguments")
    optional_arguments.add_argument(
        "--count",
        "-count",
        help="Comma separated list of columns for count 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--sum",
        "-sum",
        help="Comma separated list of columns for sum 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--avg",
        "-avg",
        help="Comma separated list of columns for avg 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--min",
        "-min",
        help="Comma separated list of columns for min 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--max",
        "-max",
        help="Comma separated list of columns for max 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--bit_xor",
        "-bit_xor",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--std",
        "-std",
        help="Comma separated list of columns for standard deviation 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--grouped-columns",
        "-gc",
        help="Comma separated list of columns to use in GroupBy 'col_a,col_b'",
    )
    optional_arguments.add_argument(
        "--exclude-columns",
        "-ec",
        action="store_true",
        help="Flag to indicate the list of columns should be excluded from validation and not included.",
    )
    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    optional_arguments.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    optional_arguments.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )
    optional_arguments.add_argument(
        "--wildcard-include-string-len",
        "-wis",
        action="store_true",
        help="Include string fields for wildcard aggregations.",
    )
    optional_arguments.add_argument(
        "--wildcard-include-timestamp",
        "-wit",
        action="store_true",
        help="Include timestamp/date fields for wildcard aggregations.",
    )
    optional_arguments.add_argument(
        "--cast-to-bigint",
        "-ctb",
        action="store_true",
        help="Cast any int32 fields to int64 for large aggregations.",
    )

    # Group required arguments
    required_arguments = column_parser.add_argument_group("required arguments")
    required_arguments.add_argument(
        "--tables-list",
        "-tbls",
        default=None,
        required=True,
        help="Comma separated tables list in the form 'schema.table=target_schema.target_table'",
    )
    _add_common_arguments(optional_arguments, required_arguments)


def _configure_schema_parser(schema_parser):
    """Configure arguments to run schema level validations."""

    # Group optional arguments
    optional_arguments = schema_parser.add_argument_group("optional arguments")
    optional_arguments.add_argument(
        "--exclusion-columns",
        "-ec",
        help="Comma separated list of columns 'col_a,col_b' to be excluded from the schema validation",
    )
    optional_arguments.add_argument(
        "--allow-list",
        "-al",
        help="Comma separated list of datatype mappings due to incompatible datatypes in source and target. e.g.: decimal(12,2):decimal(38,9),!string:string,decimal(10-18,0):int64",
    )
    optional_arguments.add_argument(
        "--allow-list-file",
        "-alf",
        help="YAML file containing default --allow-list mappings. Can be used in conjunction with --allow-list. e.g.: samples/allow_list/oracle_to_bigquery.yaml or gs://dvt-allow-list-files/oracle_to_bigquery.yaml. See example files in samples/allow_list/",
    )

    # Group required arguments
    required_arguments = schema_parser.add_argument_group("required arguments")
    required_arguments.add_argument(
        "--tables-list",
        "-tbls",
        default=None,
        required=True,
        help="Comma separated tables list in the form 'schema.table=target_schema.target_table'",
    )
    _add_common_arguments(optional_arguments, required_arguments)


def _configure_custom_query_parser(custom_query_parser):
    """Configure arguments to run custom-query validations."""

    custom_query_subparsers = custom_query_parser.add_subparsers(
        dest="custom_query_type"
    )

    # Add arguments for custom-query row parser
    custom_query_row_parser = custom_query_subparsers.add_parser(
        "row", help="Run a custom query row validation"
    )
    _configure_custom_query_row_parser(custom_query_row_parser)

    # Add arguments for custom-query column parser
    custom_query_column_parser = custom_query_subparsers.add_parser(
        "column", help="Run a custom query column validation"
    )
    _configure_custom_query_column_parser(custom_query_column_parser)


def _configure_custom_query_row_parser(custom_query_row_parser):
    # Group optional arguments
    optional_arguments = custom_query_row_parser.add_argument_group(
        "optional arguments"
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    optional_arguments.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )

    # Group required arguments
    required_arguments = custom_query_row_parser.add_argument_group(
        "required arguments"
    )
    required_arguments.add_argument(
        "--primary-keys",
        "-pk",
        required=True,
        help="Comma separated list of primary key columns 'col_a,col_b'",
    )

    # Group for mutually exclusive source query arguments. Either must be supplied
    source_mutually_exclusive = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    source_mutually_exclusive.add_argument(
        "--source-query-file",
        "-sqf",
        help="File containing the source sql query",
    )
    source_mutually_exclusive.add_argument(
        "--source-query",
        "-sq",
        help="Source sql query",
    )

    # Group for mutually exclusive target query arguments. Either must be supplied
    target_mutually_exclusive = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    target_mutually_exclusive.add_argument(
        "--target-query-file",
        "-tqf",
        help="File containing the target sql query",
    )
    target_mutually_exclusive.add_argument(
        "--target-query",
        "-tq",
        help="Target sql query",
    )

    # Group for mutually exclusive required arguments. Either must be supplied
    required_mutually_exclusive = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    required_mutually_exclusive.add_argument(
        "--hash",
        "-hash",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    required_mutually_exclusive.add_argument(
        "--concat",
        "-concat",
        help="Comma separated list of columns for concat 'col_a,col_b' or * for all columns",
    )
    required_mutually_exclusive.add_argument(
        "--comparison-fields",
        "-comp-fields",
        help=(
            "Individual columns to compare. If comparing a calculated field use "
            "the column alias."
        ),
    )
    _add_common_arguments(optional_arguments, required_arguments)


def _configure_custom_query_column_parser(custom_query_column_parser):
    # Group optional arguments
    optional_arguments = custom_query_column_parser.add_argument_group(
        "optional arguments"
    )
    optional_arguments.add_argument(
        "--count",
        "-count",
        help="Comma separated list of columns for count 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--sum",
        "-sum",
        help="Comma separated list of columns for sum 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--avg",
        "-avg",
        help="Comma separated list of columns for avg 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--min",
        "-min",
        help="Comma separated list of columns for min 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--max",
        "-max",
        help="Comma separated list of columns for max 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--bit_xor",
        "-bit_xor",
        help="Comma separated list of columns for hashing a concatenate 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--std",
        "-std",
        help="Comma separated list of columns for standard deviation 'col_a,col_b' or * for all columns",
    )
    optional_arguments.add_argument(
        "--exclude-columns",
        "-ec",
        action="store_true",
        help="Flag to indicate the list of columns should be excluded from validation and not included.",
    )
    optional_arguments.add_argument(
        "--wildcard-include-string-len",
        "-wis",
        action="store_true",
        help="Include string fields for wildcard aggregations.",
    )
    optional_arguments.add_argument(
        "--wildcard-include-timestamp",
        "-wit",
        action="store_true",
        help="Include timestamp/date fields for wildcard aggregations.",
    )
    optional_arguments.add_argument(
        "--cast-to-bigint",
        "-ctb",
        action="store_true",
        help="Cast any int32 fields to int64 for large aggregations.",
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        help="Filters in the format source_filter:target_filter",
    )
    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--use-random-row",
        "-rr",
        action="store_true",
        help="Finds a set of random rows of the first primary key supplied.",
    )
    optional_arguments.add_argument(
        "--random-row-batch-size",
        "-rbs",
        help="Row batch size used for random row filters (default 10,000).",
    )

    # Group required arguments
    required_arguments = custom_query_column_parser.add_argument_group(
        "required arguments"
    )

    # Group for mutually exclusive source query arguments. Either must be supplied
    source_mutually_exclusive = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    source_mutually_exclusive.add_argument(
        "--source-query-file",
        "-sqf",
        help="File containing the source sql query",
    )
    source_mutually_exclusive.add_argument(
        "--source-query",
        "-sq",
        help="Source sql query",
    )

    # Group for mutually exclusive target query arguments. Either must be supplied
    target_mutually_exclusive = required_arguments.add_mutually_exclusive_group(
        required=True
    )
    target_mutually_exclusive.add_argument(
        "--target-query-file",
        "-tqf",
        help="File containing the target sql query",
    )
    target_mutually_exclusive.add_argument(
        "--target-query",
        "-tq",
        help="Target sql query",
    )

    _add_common_arguments(optional_arguments, required_arguments)


def _add_common_arguments(optional_arguments, required_arguments):
    # Group all Required Arguments together
    required_arguments.add_argument(
        "--source-conn", "-sc", required=True, help="Source connection name"
    )
    required_arguments.add_argument(
        "--target-conn", "-tc", required=True, help="Target connection name"
    )

    # Optional arguments
    optional_arguments.add_argument(
        "--bq-result-handler", "-bqrh", help="BigQuery result handler config details"
    )
    optional_arguments.add_argument(
        "--labels", "-l", help="Key value pair labels for validation run"
    )
    optional_arguments.add_argument(
        "--service-account",
        "-sa",
        help="Path to SA key file for result handler output",
    )
    optional_arguments.add_argument(
        "--config-file",
        "-c",
        help="Store the validation in the YAML Config File Path specified",
    )
    optional_arguments.add_argument(
        "--format",
        "-fmt",
        default="table",
        help="Set the format for printing command output, Supported formats are (text, csv, json, table). Defaults "
        "to table",
    )
    optional_arguments.add_argument(
        "--filter-status",
        "-fs",
        # TODO: update if we start to support other statuses
        help="Comma separated list of statuses to filter the validation results. Supported statuses are (success, fail). If no list is provided, all statuses are returned",
    )


def _add_common_partition_arguments(optional_arguments, required_arguments):
    """Add all arguments common to get-partition command"""

    # Group all Required Arguments together
    required_arguments.add_argument(
        "--source-conn", "-sc", required=True, help="Source connection name"
    )
    required_arguments.add_argument(
        "--target-conn", "-tc", required=True, help="Target connection name"
    )
    required_arguments.add_argument(
        "--config-dir",
        "-cdir",
        required=True,
        help="Directory path to store YAML config files. "
        "GCS: Provide a full gs:// path of the target directory. "
        "Eg: `gs://<BUCKET>/partiitons_dir`. "
        "Local: Provide a relative path of the target directory. "
        "Eg: `partitions_dir`",
    )
    required_arguments.add_argument(
        "--partition-num",
        "-pn",
        required=True,
        help="Number of partitions/config files to generate",
        type=int,
        choices=range(1, 1001),
        metavar="[1-1000]",
    )

    # Optional arguments
    optional_arguments.add_argument(
        "--bq-result-handler",
        "-bqrh",
        help="BigQuery result handler config details",
    )
    optional_arguments.add_argument(
        "--labels", "-l", help="Key value pair labels for validation run"
    )
    optional_arguments.add_argument(
        "--service-account",
        "-sa",
        help="Path to SA key file for result handler output",
    )
    optional_arguments.add_argument(
        "--format",
        "-fmt",
        default="table",
        help=(
            "Set the format for printing command output, Supported formats are "
            "(text, csv, json, table). Defaults to table"
        ),
    )
    optional_arguments.add_argument(
        "--filter-status",
        "-fs",
        # TODO: update if we start to support other statuses
        help=(
            "Comma separated list of statuses to filter the validation results. "
            "Supported statuses are (success, fail). If no list is provided, "
            "all statuses are returned"
        ),
    )


def get_connection_config_from_args(args):
    """Return dict with connection config supplied."""
    config = {
        consts.SOURCE_TYPE: args.connect_type,
        consts.SECRET_MANAGER_TYPE: getattr(args, consts.SECRET_MANAGER_TYPE),
        consts.SECRET_MANAGER_PROJECT_ID: getattr(
            args, consts.SECRET_MANAGER_PROJECT_ID
        ),
    }

    if args.connect_type == "Raw":
        return json.loads(args.json)

    for field_obj in CONNECTION_SOURCE_FIELDS[args.connect_type]:
        field = field_obj[0]
        if getattr(args, field) is None:
            continue
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


def _generate_random_name(conn):
    name = f"{conn[consts.SOURCE_TYPE]}_{str(uuid.uuid4())}"
    return name


def store_connection(connection_name, conn):
    """Store the connection config under the given name."""
    mgr = state_manager.StateManager()
    mgr.create_connection(connection_name, conn)


def list_connections():
    """List all saved connections."""
    mgr = state_manager.StateManager()
    connections = mgr.list_connections()
    for conn_name in connections:
        source_type = mgr.get_connection_config(conn_name).get("source_type")
        logging.info(f"Connection Name: {conn_name} : {source_type}")


def get_connection(connection_name):
    """Return dict connection details for a specific connection."""
    mgr = state_manager.StateManager()
    return mgr.get_connection_config(connection_name)


def store_validation(validation_file_name, yaml_config):
    """Store the validation YAML config under the given name."""
    mgr = state_manager.StateManager()
    mgr.create_validation_yaml(validation_file_name, yaml_config)


def store_partition(target_file_path, yaml_config, target_folder_path=None):
    """Store the partition YAML config under the given name."""
    mgr = state_manager.StateManager(target_folder_path)
    mgr.create_partition_yaml(target_file_path, yaml_config)


def get_validation(validation_name, config_dir=None):
    """Return validation YAML for a specific connection."""
    if config_dir:
        mgr = state_manager.StateManager(file_system_root_path=config_dir)
        return mgr.get_validation_config(validation_name, config_dir)
    else:
        if validation_name.startswith("gs://"):
            obj_depth = len(validation_name.split("/"))
            gcs_prefix = "/".join(validation_name.split("/")[: obj_depth - 1])
            mgr = state_manager.StateManager(file_system_root_path=gcs_prefix)
            return mgr.get_validation_config(
                validation_name.split("/")[obj_depth - 1], gcs_prefix
            )
        else:
            mgr = state_manager.StateManager()
            return mgr.get_validation_config(validation_name)


def list_validations():
    """List all saved validation YAMLs."""
    mgr = state_manager.StateManager()
    validations = mgr.list_validations()

    logging.info("Validation YAMLs found:")
    for validation_name in validations:
        logging.info(validation_name)


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

    return _read_json_value(arg_value) or arg_value.split(",")


def _read_json_value(arg_value: str) -> list:
    """Returns a deserialized JSON value or None if an error occurs."""
    try:
        if isinstance(arg_value, list):
            arg_value = str(arg_value)
        return json.loads(arg_value)
    except json.decoder.JSONDecodeError:
        return None


def get_tables_list(arg_tables, default_value=None, is_filesystem=False):
    """Returns dictionary of tables. Backwards compatible for JSON input.

    arg_table (str): tables_list argument specified
    default_value (Any): A default value to supply when arg_value is empty.
    is_filesystem (boolean): Boolean indicating whether source connection is a FileSystem. In this case, a schema is not required.
    """
    if not arg_tables:
        return default_value

    json_tables_list = _read_json_value(arg_tables)
    if json_tables_list:
        return json_tables_list

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


def get_pre_build_configs(args: Namespace, validate_cmd: str) -> List[Dict]:
    """Return a dict of configurations to build ConfigManager object"""

    # Since `generate-table-partitions` defaults to `validate_cmd=row`,
    # `validate_cmd` is passed along while calling this method
    if validate_cmd is None:
        validate_cmd = args.validate_cmd.capitalize()

    if validate_cmd == "Schema":
        config_type = consts.SCHEMA_VALIDATION
    elif validate_cmd == "Column":
        config_type = consts.COLUMN_VALIDATION
    elif validate_cmd == "Row":
        config_type = consts.ROW_VALIDATION
    elif validate_cmd == "Custom-query":
        config_type = consts.CUSTOM_QUERY
    else:
        raise ValueError(f"Unknown Validation Type: {validate_cmd}")

    # Get result handler config
    if args.bq_result_handler:
        result_handler_config = get_result_handler(
            args.bq_result_handler, args.service_account
        )
    else:
        result_handler_config = None

    # Get filter_config and threshold. Not supported in case of schema validation
    filter_config = []
    threshold = 0.0
    if config_type != consts.SCHEMA_VALIDATION:
        if args.filters:
            filter_config = get_filters(args.filters)
        if args.threshold:
            threshold = args.threshold

    # Get labels
    if args.labels is None:
        labels = []
    else:
        labels = get_labels(args.labels)

    # Get source and target clients
    mgr = state_manager.StateManager()
    source_client = clients.get_data_client(mgr.get_connection_config(args.source_conn))
    target_client = clients.get_data_client(mgr.get_connection_config(args.target_conn))

    # Get format: text, csv, json, table. Default is table
    format = args.format if args.format else "table"

    # Get random row arguments. Not supported in case of schema validation,
    # custom-query validation and generate-table-partitions
    use_random_rows = None
    random_row_batch_size = None
    if (
        args.command != "generate-table-partitions"
        and config_type != consts.SCHEMA_VALIDATION
    ):
        use_random_rows = args.use_random_row
        random_row_batch_size = args.random_row_batch_size

    # Get table list. Not supported in case of custom query validation
    is_filesystem = source_client._source_type == "FileSystem"
    if config_type == consts.CUSTOM_QUERY:
        tables_list = get_tables_list(
            None, default_value=[{}], is_filesystem=is_filesystem
        )
    else:
        tables_list = get_tables_list(
            args.tables_list, default_value=[{}], is_filesystem=is_filesystem
        )

    # Get validation filter status: success, fail
    if args.filter_status is not None:
        arg_list = get_arg_list(args.filter_status)
        if all(arg in consts.VALIDATION_STATUSES for arg in arg_list):
            filter_status = arg_list
        else:
            raise ValueError("An unsupported status was provided")
    else:
        filter_status = None

    pre_build_configs_list = []
    for table_obj in tables_list:
        pre_build_configs = {
            "config_type": config_type,
            "source_conn_name": args.source_conn,
            "target_conn_name": args.target_conn,
            "table_obj": table_obj,
            "labels": labels,
            "threshold": threshold,
            "format": format,
            "use_random_rows": use_random_rows,
            "random_row_batch_size": random_row_batch_size,
            "source_client": source_client,
            "target_client": target_client,
            "result_handler_config": result_handler_config,
            "filter_config": filter_config,
            "filter_status": filter_status,
            "verbose": args.verbose,
        }
        pre_build_configs_list.append(pre_build_configs)

    return pre_build_configs_list
