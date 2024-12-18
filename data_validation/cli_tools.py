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
import copy
import csv
import json
import logging
import sys
import uuid
import os
import math
import re
from argparse import Namespace
from typing import Dict, List, Optional
from yaml import Dumper, Loader, dump, load

from data_validation import clients, consts, find_tables, state_manager, gcs_helper
from data_validation.validation_builder import list_to_sublists


CONNECTION_SOURCE_FIELDS = {
    "BigQuery": [
        ["project_id", "GCP Project to use for BigQuery"],
        ["google_service_account_key_path", "(Optional) GCP SA Key Path"],
        [
            "api_endpoint",
            '(Optional) GCP BigQuery API endpoint (e.g. "https://mybq.p.googleapis.com")',
        ],
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
        ["url", "Oracle SQLAlchemy connection URL"],
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
        ["host", "Desired PostgreSQL host."],
        ["port", "PostgreSQL port to connect on (e.g. 5432)"],
        ["user", "Username to connect to"],
        ["password", "Password for authentication of user"],
        ["database", "Database in PostgreSQL to connect to (default postgres)"],
    ],
    "Redshift": [
        ["host", "Desired Redshift host."],
        ["port", "Redshift port to connect on (e.g. 5439)"],
        ["user", "Username to connect to"],
        ["password", "Password for authentication of user"],
        ["database", "Database in Redshift to connect to"],
    ],
    "Spanner": [
        ["project_id", "GCP Project to use for Spanner"],
        ["instance_id", "ID of Spanner instance to connect to"],
        ["database_id", "ID of Spanner database (schema) to connect to"],
        ["google_service_account_key_path", "(Optional) GCP SA Key Path"],
        [
            "api_endpoint",
            '(Optional) GCP Spanner API endpoint (e.g. "https://mycs.p.googleapis.com")',
        ],
    ],
    "FileSystem": [
        ["table_name", "Table name to use as reference for file data"],
        ["file_path", "The local, s3, or GCS file path to the data"],
        ["file_type", "The file type of the file. 'csv', 'orc', 'parquet' or 'json'"],
    ],
    "Impala": [
        ["host", "Desired Impala host"],
        ["port", "Desired Impala port (10000 if not provided)"],
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

VALIDATE_HELP_TEXT = "Run a validation and optionally store to config"
VALIDATE_COLUMN_HELP_TEXT = "Run a column validation"
VALIDATE_ROW_HELP_TEXT = "Run a row validation"
VALIDATE_SCHEMA_HELP_TEXT = "Run a schema validation"
VALIDATE_CUSTOM_QUERY_HELP_TEXT = "Run a custom query validation"


def _check_custom_query_args(parser: argparse.ArgumentParser, parsed_args: Namespace):
    # This is where we make additional checks if the arguments provided are what we expect
    # For example, only one of -tbls and custom query options can be provided
    if hasattr(parsed_args, "tables_list") and hasattr(
        parsed_args, "source_query"
    ):  # New Format
        if (
            parsed_args.tables_list
        ):  # Tables_list is not None - so source and target queries all must be None
            if (
                parsed_args.source_query_file
                or parsed_args.source_query
                or parsed_args.target_query_file
                or parsed_args.target_query
            ):
                parser.error(
                    f"{parsed_args.command}: when --tables-list/-tbls is specified, --source-query-file/-sqf, --source-query/-sq, --target-query-file/-tqf and --target-query/-tq must not be specified"
                )
            else:
                return
        elif (parsed_args.source_query_file or parsed_args.source_query) and (
            parsed_args.target_query_file or parsed_args.target_query
        ):
            return
        else:
            parser.error(
                f"{parsed_args.command}: Must specify both source (--source-query-file/-sqf or --source-query/-sq) and target (--target-query-file/-tqf or --target-query/-tq) - when --tables-list/-tbls is not specified"
            )
    else:
        return  # old format - only one of them is present


def get_parsed_args() -> Namespace:
    """Return ArgParser with configured CLI arguments."""
    parser = configure_arg_parser()
    args = ["--help"] if len(sys.argv) == 1 else None
    parsed_args = parser.parse_args(args)
    _check_custom_query_args(parser, parsed_args)
    return parsed_args


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
        help=("Generate table partitions and store validation config files"),
    )
    optional_arguments = partition_parser.add_argument_group("optional arguments")
    required_arguments = partition_parser.add_argument_group("required arguments")

    _configure_row_parser(
        partition_parser,
        optional_arguments,
        required_arguments,
        is_generate_partitions=True,
    )
    optional_arguments.add_argument(
        "--parts-per-file",
        "-ppf",
        type=_check_positive,
        default=1,
        help="Number of partitions to be validated in a single yaml file.",
    )

    required_arguments.add_argument(
        "--config-dir",
        "-cdir",
        required=True,
        help="Directory path to store YAML config files. "
        "GCS: Provide a full gs:// path of the target directory. "
        "Eg: `gs://<BUCKET>/partitons_dir`. "
        "Local: Provide a relative path of the target directory. "
        "Eg: `partitions_dir`",
    )
    required_arguments.add_argument(
        "--partition-num",
        "-pn",
        required=True,
        help="Number of partitions into which the table should be split",
        type=_check_positive,
    )
    # User can provide tables or custom queries, but not both
    # However, Argparse does not support adding an argument_group to an argument_group or adding a
    # mutually_exclusive_group or argument_group to a mutually_exclusive_group since version 3.11.
    # We are only ensuring leaf level mutual exclusivity here and will need to check higher level
    # mutual exclusivity in the code - i.e. a) when --tables-list is present, there can be no custom
    # query parameters and b) when custom query parameters are specified, both source and target must be
    # specified.
    optional_arguments.add_argument(
        "--tables-list",
        "-tbls",
        help=(
            "Comma separated tables list in the form "
            "'schema.table=target_schema.target_table'"
        ),
    )

    source_mutually_exclusive = optional_arguments.add_mutually_exclusive_group()
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
    target_mutually_exclusive = optional_arguments.add_mutually_exclusive_group()
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
        "find-tables", help="Build tables list using approx string matching."
    )
    find_tables_parser.add_argument(
        "--source-conn", "-sc", help="Source connection name."
    )
    find_tables_parser.add_argument(
        "--target-conn", "-tc", help="Target connection name."
    )
    find_tables_parser.add_argument(
        "--allowed-schemas", "-as", help="List of source schemas to match."
    )
    find_tables_parser.add_argument(
        "--include-views",
        "-iv",
        default=False,
        action="store_true",
        help="Include views in results.",
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
    """Configure arguments to run a data validation YAML config file."""
    validation_config_parser = subparsers.add_parser(
        "configs", help="Run validations stored in a YAML config file"
    )
    configs_subparsers = validation_config_parser.add_subparsers(
        dest="validation_config_cmd"
    )
    list_parser = configs_subparsers.add_parser(
        "list", help="List your validation configs"
    )
    list_parser.add_argument(
        "--config-dir",
        "-cdir",
        help="Directory path from which to list validation YAML configs.",
    )

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
    run_parser.add_argument(
        "--kube-completions",
        "-kc",
        action="store_true",
        help="When validating multiple table partitions generated by generate-table-partitions, using DVT in Kubernetes in index completion mode use this flag so that all the validations are completed",
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
        article = "an" if database[0].lower() in "aeiou" else "a"
        db_parser = subparsers.add_parser(
            database, help=f"Store {article} {database} connection"
        )

        for field_obj in CONNECTION_SOURCE_FIELDS[database]:
            arg_field = "--" + field_obj[0].replace("_", "-")
            help_txt = field_obj[1]
            db_parser.add_argument(arg_field, help=help_txt)


def _configure_validate_parser(subparsers):
    """Configure arguments to run validations."""
    validate_parser = subparsers.add_parser("validate", help=VALIDATE_HELP_TEXT)

    validate_parser.add_argument(
        "--dry-run",
        "-dr",
        action="store_true",
        help="Prints source and target SQL to stdout in lieu of performing a validation.",
    )

    validate_subparsers = validate_parser.add_subparsers(dest="validate_cmd")

    column_parser = validate_subparsers.add_parser(
        "column", help=VALIDATE_COLUMN_HELP_TEXT
    )
    _configure_column_parser(column_parser)

    row_parser = validate_subparsers.add_parser("row", help=VALIDATE_ROW_HELP_TEXT)
    optional_arguments = row_parser.add_argument_group("optional arguments")
    required_arguments = row_parser.add_argument_group("required arguments")
    _configure_row_parser(row_parser, optional_arguments, required_arguments)

    schema_parser = validate_subparsers.add_parser(
        "schema", help=VALIDATE_SCHEMA_HELP_TEXT
    )
    _configure_schema_parser(schema_parser)

    custom_query_parser = validate_subparsers.add_parser(
        "custom-query", help=VALIDATE_CUSTOM_QUERY_HELP_TEXT
    )
    _configure_custom_query_parser(custom_query_parser)


def _configure_row_parser(
    parser,
    optional_arguments,
    required_arguments,
    is_generate_partitions=False,
    is_custom_query=False,
):
    """Configure arguments to run row level validations."""
    # Group optional arguments
    optional_arguments.add_argument(
        "--primary-keys",
        "-pk",
        help=(
            "Comma separated list of primary key columns 'col_a,col_b', "
            "when not specified the value will be inferred from the source or target table if available"
        ),
    )

    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        default=0.0,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--exclude-columns",
        "-ec",
        action="store_true",
        help="Flag to indicate the list of columns should be excluded from hash or concat instead of included.",
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        type=get_filters,
        default=[],
        help="Filters in the format source_filter:target_filter",
    )
    optional_arguments.add_argument(
        "--trim-string-pks",
        "-tsp",
        action="store_true",
        help=(
            "Trims string based primary key values, intended for use when one engine uses "
            "padded string semantics (e.g. CHAR(n)) and the other does not (e.g. VARCHAR(n))."
        ),
    )
    optional_arguments.add_argument(
        "--case-insensitive-match",
        "-cim",
        action="store_true",
        help=(
            "Performs a case insensitive match by adding an UPPER() before comparison."
        ),
    )
    optional_arguments.add_argument(
        "--max-concat-columns",
        "-mcc",
        type=int,
        help=(
            "The maximum number of columns accepted by a --hash or --concat validation. When there are "
            "more columns than this the validation will implicitly be split into multiple validations. "
            "This option has engine specific defaults."
        ),
    )
    # Generate-table-partitions and custom-query does not support random row
    if not (is_generate_partitions or is_custom_query):
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
        # Generate table partitions follows a new argument spec where either the table names or queries can be provided, but not both.
        # that is specified in configure_partition_parser. If we use the same spec for row and column validation, the custom query commands
        # may get subsumed by validate and validate commands by specifying tables name or queries. Until this -tbls will be
        # a required argument for validate row, validate column and validate schema.
        required_arguments.add_argument(
            "--tables-list",
            "-tbls",
            default=None,
            required=True,
            help="Comma separated tables list in the form 'schema.table=target_schema.target_table'",
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
    _add_common_arguments(
        optional_arguments,
        required_arguments,
        is_generate_partitions=is_generate_partitions,
    )


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
        default=0.0,
        help="Float max threshold for percent difference",
    )
    optional_arguments.add_argument(
        "--filters",
        "-filters",
        type=get_filters,
        default=[],
        help="Filters in the format source_filter:target_filter",
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
        help="Comma separated tables list in the form 'schema.table=target_schema.target_table'. Or shorthand schema.* for all tables.",
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
    optional_arguments = custom_query_row_parser.add_argument_group(
        "optional arguments"
    )
    required_arguments = custom_query_row_parser.add_argument_group(
        "required arguments"
    )

    _configure_row_parser(
        custom_query_row_parser,
        optional_arguments,
        required_arguments,
        is_custom_query=True,
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
        type=get_filters,
        default=[],
        help="Filters in the format source_filter:target_filter",
    )
    optional_arguments.add_argument(
        "--threshold",
        "-th",
        type=threshold_float,
        default=0.0,
        help="Float max threshold for percent difference",
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


def _add_common_arguments(
    optional_arguments, required_arguments, is_generate_partitions=False
):
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
    if not is_generate_partitions:
        optional_arguments.add_argument(
            "--config-file",
            "-c",
            help="Store the validation config in the YAML File Path specified",
        )
        optional_arguments.add_argument(
            "--config-file-json",
            "-cj",
            help="Store the validation config in the JSON File Path specified to be used for application use cases",
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
    optional_arguments.add_argument(
        "--run-id",
        "-rid",
        default=None,
        help="Set a string for the run_id, if None is input then a randomly generated UUID will be used, which is the default behaviour",
    )


def _check_positive(value: int) -> int:
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue


def check_no_yaml_files(partition_num: int, parts_per_file: int):
    """Check that number of yaml files generated is less than 10,001
    Will be invoked after all the arguments are processed."""
    if math.ceil(partition_num / parts_per_file) < 10001:
        return
    else:
        raise argparse.ArgumentTypeError(
            f"partition-num={partition_num} results in more than the maximum number of yaml files (i.e. 10,000). Reduce the number of yaml files by using the --parts-per-file argument or decreasing the number of partitions."
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


def store_validation(validation_file_name, config, include_log=True):
    """Store the validation config under the given name."""
    validation_path = gcs_helper.get_validation_path(validation_file_name)

    if validation_file_name.endswith(".yaml"):
        config_str = dump(config, Dumper=Dumper)
    elif validation_file_name.endswith("json"):
        config_str = json.dumps(config)
    else:
        raise ValueError(f"Invalid validation file name: {validation_file_name}")

    gcs_helper.write_file(validation_path, config_str, include_log=include_log)


def get_validation(name: str, config_dir: str = None):
    """Return validation YAML config."""
    if config_dir:
        validation_path = os.path.join(config_dir, name)
    else:
        validation_path = gcs_helper.get_validation_path(name)

    validation_bytes = gcs_helper.read_file(validation_path)
    return load(validation_bytes, Loader=Loader)


def list_validations(config_dir="./"):
    """List all saved validation YAMLs in a directory."""
    logging.info(f"Looking for validations in path {config_dir}")
    if gcs_helper._is_gcs_path(config_dir):
        if not config_dir.endswith("/"):
            config_dir += "/"
        files = gcs_helper.list_gcs_directory(config_dir)
    else:
        files = os.listdir(config_dir)

    return [file_name for file_name in files if file_name.endswith(".yaml")]


def print_validations_in_dir(config_dir="./"):
    validations = list_validations(config_dir=config_dir)
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


def get_filters(filter_value: str) -> List[Dict]:
    """Returns filters for source and target from --filters argument.
    A filter is the condition that is used in a SQL WHERE clause.
    If only one filter is specified, it applies to both source and target
    For a doc on regular expression for filters see docs/internal/filters_regex.md
    """

    single_filter = r"([^':]*('[^']*')*)*"
    double_filter = (
        r"(?P<source>" + single_filter + r"):(?P<target>" + single_filter + r")"
    )
    filter_config = []
    if result := re.fullmatch(single_filter, filter_value):
        if result.group(0) == "":
            raise argparse.ArgumentTypeError("Empty string not allowed in filter")
        filter_dict = {
            "type": "custom",
            "source": result.group(0),
            "target": result.group(0),
        }
    elif result := re.fullmatch(double_filter, filter_value):
        if result.group("source") == "" or result.group("target") == "":
            raise argparse.ArgumentTypeError("Empty string not allowed in filter")
        filter_dict = {
            "type": "custom",
            "source": result.group("source"),
            "target": result.group("target"),
        }
    else:
        raise argparse.ArgumentTypeError("Unable to parse filter arguments.")
    filter_config.append(filter_dict)
    return filter_config


def get_result_handler(rc_value: str, sa_file=None) -> dict:
    """Returns dict of result handler config. Backwards compatible for JSON input.

    rc_value (str): Result config argument specified.
    sa_file (str): SA path argument specified.
    """
    config = rc_value.split(".", 1)
    if len(config) != 2:
        raise ValueError(f"Unable to parse result handler config: `{rc_value}`")

    # Check if the first part of the BQRH is a connection name.
    mgr = state_manager.StateManager()
    connections = mgr.list_connections()
    if config[0] in connections:
        # We received connection_name.bq_results_table
        conn_from_file = get_connection(config[0])
        result_handler = {
            "type": "BigQuery",
            consts.PROJECT_ID: conn_from_file["project_id"],
            consts.TABLE_ID: config[1],
            consts.API_ENDPOINT: conn_from_file.get("api_endpoint", None),
        }
    else:
        # We received project_name.bq_results_table
        result_handler = {
            "type": "BigQuery",
            consts.PROJECT_ID: config[0],
            consts.TABLE_ID: config[1],
        }

    if sa_file:
        result_handler[consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH] = sa_file

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
    source_schema_required = bool(not is_filesystem)

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


def get_query_from_file(filename):
    """Return query from input file"""
    query = ""
    try:
        query = gcs_helper.read_file(filename, download_as_text=True)
        query = query.rstrip(";\n")
    except IOError:
        logging.error("Cannot read query file: ", filename)

    if not query or query.isspace():
        raise ValueError(
            "Expected file with sql query, got empty file or file with white spaces. "
            f"input file: {filename}"
        )
    return query


def get_query_from_inline(inline_query):
    """Return query from inline query arg"""

    query = inline_query.strip()
    query = query.rstrip(";\n")

    if not query or query.isspace():
        raise ValueError(
            "Expected arg with sql query, got empty arg or arg with white "
            f"spaces. input query: '{inline_query}'"
        )
    return query


def get_query_from_query_args(query_str_arg, query_file_arg) -> str:
    if query_str_arg:
        return get_query_from_inline(query_str_arg)
    else:
        return get_query_from_file(query_file_arg)


def _max_concat_columns(
    max_concat_columns_option: int, source_client, target_client
) -> Optional[int]:
    """Determine any upper limit on number of columns allowed into concat() operation."""
    if max_concat_columns_option:
        # User specified limit takes precedence.
        return max_concat_columns_option
    else:
        source_max = consts.MAX_CONCAT_COLUMNS_DEFAULTS.get(source_client.name, None)
        target_max = consts.MAX_CONCAT_COLUMNS_DEFAULTS.get(target_client.name, None)
        if source_max and target_max:
            return min(source_max, target_max)
        else:
            return source_max or target_max


def _concat_column_count_configs(
    cols: list,
    pre_build_configs: dict,
    arg_to_override: str,
    max_col_count: int,
) -> list:
    """
    Ensure we don't have too many columns for the engines involved.
    https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1216
    """
    return_list = []
    if max_col_count and len(cols) > max_col_count:
        for col_chunk in list_to_sublists(cols, max_col_count):
            col_csv = ",".join(col_chunk)
            pre_build_configs_copy = copy.copy(pre_build_configs)
            pre_build_configs_copy[arg_to_override] = col_csv
            return_list.append(pre_build_configs_copy)
    else:
        return_list.append(pre_build_configs)
    return return_list


def get_pre_build_configs(args: Namespace, validate_cmd: str) -> List[Dict]:
    """Return a dict of configurations to build ConfigManager object"""

    def cols_from_arg(concat_arg: str, client, table_obj: dict, query_str: str) -> list:
        if concat_arg == "*":
            # If validating with "*" then we need to expand to count the columns.
            if table_obj:
                return clients.get_ibis_table_schema(
                    client,
                    table_obj["schema_name"],
                    table_obj["table_name"],
                ).names
            else:
                return clients.get_ibis_query_schema(
                    client,
                    query_str,
                ).names
        else:
            return get_arg_list(concat_arg)

    # validate_cmd will be set to 'row`, or 'Custom-query' if invoked by generate-table-partitions depending
    # on what is being partitioned. Otherwise validate_cmd will be set to None
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

    # Set filter_config and threshold. Not supported in case of schema validation
    filter_config = getattr(args, consts.CONFIG_FILTERS, [])
    threshold = getattr(args, consts.CONFIG_THRESHOLD, 0.0)

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

    # Get random row arguments. Only in row validations these attributes can be present.
    # Bad coding here, but keeping it so as not to introduce a breaking change. See
    # consts.py Line 17 for a more detailed explanation.
    use_random_rows = getattr(args, "use_random_row", False)
    random_row_batch_size = getattr(args, consts.CONFIG_RANDOM_ROW_BATCH_SIZE, None)

    # Get table list. Not supported in case of custom query validation
    is_filesystem = source_client._source_type == "FileSystem"
    query_str = None
    if config_type == consts.CUSTOM_QUERY:
        tables_list = get_tables_list(
            None, default_value=[{}], is_filesystem=is_filesystem
        )
        query_str = get_query_from_query_args(args.source_query, args.source_query_file)
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
    if config_type != consts.CUSTOM_QUERY:
        tables_list = find_tables.expand_tables_of_asterisk(
            tables_list, source_client, target_client
        )
    for table_obj in tables_list:
        pre_build_configs = {
            "config_type": config_type,
            consts.CONFIG_SOURCE_CONN_NAME: args.source_conn,
            consts.CONFIG_TARGET_CONN_NAME: args.target_conn,
            "table_obj": table_obj,
            consts.CONFIG_LABELS: labels,
            consts.CONFIG_THRESHOLD: threshold,
            consts.CONFIG_FORMAT: format,
            consts.CONFIG_USE_RANDOM_ROWS: use_random_rows,
            consts.CONFIG_RANDOM_ROW_BATCH_SIZE: random_row_batch_size,
            "source_client": source_client,
            "target_client": target_client,
            "result_handler_config": result_handler_config,
            "filter_config": filter_config,
            consts.CONFIG_FILTER_STATUS: filter_status,
            consts.CONFIG_TRIM_STRING_PKS: getattr(
                args, consts.CONFIG_TRIM_STRING_PKS, False
            ),
            consts.CONFIG_CASE_INSENSITIVE_MATCH: getattr(
                args, consts.CONFIG_CASE_INSENSITIVE_MATCH, False
            ),
            consts.CONFIG_ROW_CONCAT: getattr(args, consts.CONFIG_ROW_CONCAT, None),
            consts.CONFIG_ROW_HASH: getattr(args, consts.CONFIG_ROW_HASH, None),
            consts.CONFIG_RUN_ID: getattr(args, consts.CONFIG_RUN_ID, None),
            "verbose": args.verbose,
        }
        if (
            pre_build_configs[consts.CONFIG_ROW_CONCAT]
            or pre_build_configs[consts.CONFIG_ROW_HASH]
        ):
            # Ensure we don't have too many columns for the engines involved.
            cols = cols_from_arg(
                pre_build_configs[consts.CONFIG_ROW_HASH]
                or pre_build_configs[consts.CONFIG_ROW_CONCAT],
                source_client,
                table_obj,
                query_str,
            )
            new_pre_build_configs = _concat_column_count_configs(
                cols,
                pre_build_configs,
                consts.CONFIG_ROW_HASH if args.hash else consts.CONFIG_ROW_CONCAT,
                _max_concat_columns(
                    args.max_concat_columns, source_client, target_client
                ),
            )
            if len(new_pre_build_configs) > 1:
                message_type = (
                    f'{table_obj["schema_name"]}.{table_obj["table_name"]}'
                    if table_obj
                    else "custom query"
                )
                logging.info(
                    f"Splitting validation into {len(new_pre_build_configs)} queries for {message_type}"
                )
            pre_build_configs_list.extend(new_pre_build_configs)
        else:
            pre_build_configs_list.append(pre_build_configs)

    return pre_build_configs_list
