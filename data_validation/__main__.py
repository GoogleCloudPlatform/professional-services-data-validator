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

import json
import logging
import os
import sys

from yaml import Dumper, dump
from argparse import Namespace
from typing import List
from data_validation import (
    cli_tools,
    clients,
    consts,
    exceptions,
    raw_query,
    state_manager,
    util,
)
from data_validation.config_manager import ConfigManager
from data_validation.data_validation import DataValidation
from data_validation.find_tables import find_tables_using_string_matching
from data_validation.partition_builder import PartitionBuilder

# by default yaml dumps lists as pointers. This disables that feature
Dumper.ignore_aliases = lambda *args: True

# Log level mappings for the input argument of log level string
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def _get_arg_config_file(args):
    """Return String YAML config file path."""
    if not args.config_file:
        raise ValueError("YAML Config File was not supplied.")
    elif not args.config_file.endswith(".yaml"):
        raise ValueError(
            f"Invalid YAML config name: {args.config_file}. Provide YAML file extension."
        )
    return args.config_file


def _get_arg_config_file_json(args):
    """Return String JSON config file path."""
    if not args.config_file_json:
        raise ValueError("JSON Config File was not supplied.")
    elif not args.config_file_json.endswith(".json"):
        raise ValueError(
            f"Invalid JSON config name: {args.config_file_json}. Provide JSON file extension."
        )
    return args.config_file_json


def get_aggregate_config(args, config_manager: ConfigManager):
    """Return list of formatted aggregation objects.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
    """
    aggregate_configs = [config_manager.build_config_count_aggregate()]
    supported_data_types = [
        "float64",
        "float32",
        "int8",
        "int16",
        "int32",
        "int64",
        "decimal",
        "!float64",
        "!float32",
        "!int8",
        "!int16",
        "!int32",
        "!int64",
        "!decimal",
    ]
    uuid_types = ["uuid", "!uuid"]

    if args.wildcard_include_string_len:
        supported_data_types.extend(["string", "!string"])

    if args.wildcard_include_timestamp:
        supported_data_types.extend(["timestamp", "!timestamp", "date", "!date"])

    cast_to_bigint = True if args.cast_to_bigint else False

    if args.count:
        col_args = None if args.count == "*" else cli_tools.get_arg_list(args.count)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "count", col_args, args.exclude_columns, None, cast_to_bigint=cast_to_bigint
        )
    if args.sum:
        col_args = None if args.sum == "*" else cli_tools.get_arg_list(args.sum)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "sum",
            col_args,
            args.exclude_columns,
            supported_data_types,
            cast_to_bigint=cast_to_bigint,
        )
    if args.avg:
        col_args = None if args.avg == "*" else cli_tools.get_arg_list(args.avg)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "avg",
            col_args,
            args.exclude_columns,
            supported_data_types,
            cast_to_bigint=cast_to_bigint,
        )
    if args.min:
        col_args = None if args.min == "*" else cli_tools.get_arg_list(args.min)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "min",
            col_args,
            args.exclude_columns,
            supported_data_types + uuid_types,
            cast_to_bigint=cast_to_bigint,
        )
    if args.max:
        col_args = None if args.max == "*" else cli_tools.get_arg_list(args.max)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "max",
            col_args,
            args.exclude_columns,
            supported_data_types + uuid_types,
            cast_to_bigint=cast_to_bigint,
        )
    if args.bit_xor:
        col_args = None if args.bit_xor == "*" else cli_tools.get_arg_list(args.bit_xor)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "bit_xor",
            col_args,
            args.exclude_columns,
            supported_data_types,
            cast_to_bigint=cast_to_bigint,
        )
    if args.std:
        col_args = None if args.std == "*" else cli_tools.get_arg_list(args.std)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "std",
            col_args,
            args.exclude_columns,
            supported_data_types,
            cast_to_bigint=cast_to_bigint,
        )
    return aggregate_configs


def _get_calculated_config(args, config_manager: ConfigManager) -> List[dict]:
    """Return list of formatted calculated objects.

    Args:
        config_manager(ConfigManager): Validation config manager instance.
    """
    calculated_configs = []
    fields = []
    if config_manager.hash:
        col_list = (
            None
            if config_manager.hash == "*"
            else cli_tools.get_arg_list(config_manager.hash)
        )
        fields = config_manager.build_dependent_aliases(
            "hash", col_list, args.exclude_columns
        )
    elif config_manager.concat:
        col_list = (
            None
            if config_manager.concat == "*"
            else cli_tools.get_arg_list(config_manager.concat)
        )
        fields = config_manager.build_dependent_aliases(
            "concat", col_list, args.exclude_columns
        )

    if len(fields) > 0:
        max_depth = max([x["depth"] for x in fields])
    else:
        max_depth = 0
    for field in fields:
        calculated_configs.append(
            config_manager.build_config_calculated_fields(
                field["source_reference"],
                field["target_reference"],
                field["calc_type"],
                field["name"],
                field["depth"],
                custom_params=field.get("calc_params"),
            )
        )
    if config_manager.hash:
        config_manager.append_comparison_fields(
            config_manager.build_config_comparison_fields(
                ["hash__all"], depth=max_depth
            )
        )
    elif config_manager.concat:
        config_manager.append_comparison_fields(
            config_manager.build_config_comparison_fields(
                ["concat__all"], depth=max_depth
            )
        )
    return calculated_configs


def _get_comparison_config(
    args, config_manager: ConfigManager, primary_keys: list
) -> List[dict]:
    col_list = (
        None
        if args.comparison_fields == "*"
        else cli_tools.get_arg_list(args.comparison_fields)
    )
    comparison_fields = config_manager.build_comp_fields(
        col_list,
        args.exclude_columns,
    )
    # We can't have the PK columns in the comparison SQL twice therefore filter them out here if included.
    comparison_fields = [_ for _ in comparison_fields if _ not in primary_keys]

    # As per #1190, add rstrip for Teradata string comparison fields
    if (
        config_manager.source_client.name == "teradata"
        or config_manager.target_client.name == "teradata"
    ):
        comparison_fields = config_manager.add_rstrip_to_comp_fields(comparison_fields)

    return config_manager.build_config_comparison_fields(comparison_fields)


def build_config_from_args(args: Namespace, config_manager: ConfigManager):
    """This function is used to append build configs to the config manager for all validation commands and generate-table-partitions.
    Instead of having two separate commands, e.g. validate row and validate custom-query row, generate-table-partitions
    uses implicit choice of table or custom-query. A user can specify either tables or source/target query/file,
    but not both. In the case of generate-table-partitions with custom query, the user will not provide
    args.custom_query_type. However, the code will inject args.custom_query_type as 'row' before invoking
    build_config_from_args.


    Args:
        args (Namespace): User specified Arguments
        config_manager (ConfigManager): Validation config manager instance.
    """

    # Append SCHEMA_VALIDATION configs
    if config_manager.validation_type == consts.SCHEMA_VALIDATION:
        if args.exclusion_columns:
            exclusion_columns = cli_tools.get_arg_list(args.exclusion_columns)
            config_manager.append_exclusion_columns(
                [col.casefold() for col in exclusion_columns]
            )
        config_manager.append_allow_list(args.allow_list, args.allow_list_file)

    # Append configs specific to CUSTOM_QUERY (i.e. query strings or strings from files)
    if config_manager.validation_type == consts.CUSTOM_QUERY:
        config_manager.append_custom_query_type(args.custom_query_type)

        # Get source sql query from source sql file or inline query
        config_manager.append_source_query(
            cli_tools.get_query_from_query_args(
                args.source_query, args.source_query_file
            )
        )

        # Get target sql query from target sql file or inline query
        config_manager.append_target_query(
            cli_tools.get_query_from_query_args(
                args.target_query, args.target_query_file
            )
        )

    # Append COLUMN_VALIDATION configs, including custom-query column validation
    if (
        config_manager.validation_type == consts.COLUMN_VALIDATION
        or config_manager.validation_type == consts.CUSTOM_QUERY
        and args.custom_query_type == consts.COLUMN_VALIDATION.lower()
    ):
        config_manager.append_aggregates(get_aggregate_config(args, config_manager))
        if (
            config_manager.validation_type == consts.COLUMN_VALIDATION
            and args.grouped_columns  # grouped_columns not supported in custom queries - at least now.
        ):
            grouped_columns = cli_tools.get_arg_list(args.grouped_columns)
            config_manager.append_query_groups(
                config_manager.build_column_configs(grouped_columns)
            )

    # Append ROW_VALIDATION configs, including custom-query row validation
    if (
        config_manager.validation_type == consts.ROW_VALIDATION
        or config_manager.validation_type == consts.CUSTOM_QUERY
        and args.custom_query_type == consts.ROW_VALIDATION.lower()
    ):
        # Append calculated fields: --hash/--concat
        config_manager.append_calculated_fields(
            _get_calculated_config(args, config_manager)
        )

        # Append primary_keys
        primary_keys = cli_tools.get_arg_list(args.primary_keys)
        if not primary_keys and config_manager.validation_type != consts.CUSTOM_QUERY:
            primary_keys = config_manager.auto_list_primary_keys()
        if not primary_keys:
            raise ValueError(
                "No primary keys were provided and neither the source or target tables have primary keys. Please include --primary-keys argument"
            )
        primary_keys = [_.casefold() for _ in primary_keys]
        config_manager.append_primary_keys(
            config_manager.build_column_configs(primary_keys)
        )

        # Append Comparison fields
        if args.comparison_fields:
            config_manager.append_comparison_fields(
                _get_comparison_config(args, config_manager, primary_keys)
            )

    return config_manager


def build_config_managers_from_args(
    args: Namespace, validate_cmd: str = None
) -> List[ConfigManager]:
    """Return a list of config managers ready to execute."""

    def _build_configs():
        configs = []

        # Get pre build configs to build ConfigManager objects
        pre_build_configs_list = cli_tools.get_pre_build_configs(args, validate_cmd)

        # Build a list of ConfigManager objects
        for pre_build_configs in pre_build_configs_list:
            config_manager = ConfigManager.build_config_manager(**pre_build_configs)

            # Append post build configs to ConfigManager object
            config_manager = build_config_from_args(args, config_manager)

            # Append ConfigManager object to configs list
            configs.append(config_manager)
        return configs

    return util.timed_call("Build config", _build_configs)


def config_runner(args):
    """Config Runner is where the decision is made to run validations from one or more files.
    One file can produce multiple validations - for example when more than one set of tables are being validated
    between the source and target. If multiple files are to be run and if the associated configuration files
    are numbered sequentially, say from '0000.yaml' to '0012.yaml' (for 13 validations),
    it is possible to run them concurrently in a Kubernetes / Cloud Run environment.
    If the user wants that, they need to specify a -kc or --kube-completions which tells
    DVT to only run the validation corresponding to the index number provided in the
    JOB_COMPLETION_INDEX (for Kubernetes) or CLOUD_RUN_TASK_INDEX (for Cloud Run) environment
    variable. This environment variable is set by the Kubernetes/Cloud Run container orchestrator.
    The orchestrator spins up containers to complete each validation, one at a time.
    """
    if args.config_dir:
        if args.kube_completions and (
            ("JOB_COMPLETION_INDEX" in os.environ.keys())
            or ("CLOUD_RUN_TASK_INDEX" in os.environ.keys())
        ):
            # Running in Kubernetes in Job completions - only run the yaml file corresponding to index
            job_index = (
                int(os.environ.get("JOB_COMPLETION_INDEX"))
                if "JOB_COMPLETION_INDEX" in os.environ.keys()
                else int(os.environ.get("CLOUD_RUN_TASK_INDEX"))
            )
            config_file_path = (
                f"{args.config_dir}{job_index:04d}.yaml"
                if args.config_dir.endswith("/")
                else f"{args.config_dir}/{job_index:04d}.yaml"
            )
            setattr(args, "config_dir", None)
            setattr(args, "config_file", config_file_path)
            config_managers = build_config_managers_from_yaml(args, config_file_path)
            run_validations(args, config_managers)
        else:
            if args.kube_completions:
                logging.warning(
                    "--kube-completions or -kc specified, however not running in Kubernetes Job completion, check your command line."
                )
            config_file_names = cli_tools.list_validations(config_dir=args.config_dir)
            config_managers = []
            errors = False
            for file in config_file_names:
                config_managers = build_config_managers_from_yaml(args, file)
                try:
                    logging.info(
                        "Currently running the validation for YAML file: %s",
                        file,
                    )
                    run_validations(args, config_managers)
                except Exception as e:
                    errors = True
                    logging.error(
                        "Error '%s' occurred while running config file %s. Skipping it for now.",
                        str(e),
                        file,
                    )
            if errors:
                raise exceptions.ValidationException(
                    "Some of the validations raised an exception"
                )
    else:
        if args.kube_completions:
            logging.warning(
                "--kube-completions or -kc specified, which requires a config directory, however a specific config file is provided."
            )
        config_file_path = _get_arg_config_file(args)
        config_managers = build_config_managers_from_yaml(args, config_file_path)
        run_validations(args, config_managers)


def build_config_managers_from_yaml(args, config_file_path):
    """Returns List[ConfigManager] instances ready to be executed."""
    if args.config_dir:
        yaml_configs = cli_tools.get_validation(config_file_path, args.config_dir)
    else:
        yaml_configs = cli_tools.get_validation(config_file_path)

    mgr = state_manager.StateManager()
    source_conn = mgr.get_connection_config(yaml_configs[consts.YAML_SOURCE])
    target_conn = mgr.get_connection_config(yaml_configs[consts.YAML_TARGET])

    source_client = clients.get_data_client(source_conn)
    target_client = clients.get_data_client(target_conn)

    config_managers = []
    for config in yaml_configs[consts.YAML_VALIDATIONS]:
        config[consts.CONFIG_SOURCE_CONN] = source_conn
        config[consts.CONFIG_TARGET_CONN] = target_conn
        config[consts.CONFIG_RESULT_HANDLER] = yaml_configs[consts.YAML_RESULT_HANDLER]
        config_manager = ConfigManager(
            config, source_client, target_client, verbose=args.verbose
        )
        config_manager.config[consts.CONFIG_FILE] = config_file_path
        config_managers.append(config_manager)

    return config_managers


def convert_config_to_yaml(args, config_managers: list):
    """Return dict objects formatted for yaml validations.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    yaml_config = {
        consts.YAML_SOURCE: args.source_conn,
        consts.YAML_TARGET: args.target_conn,
        consts.YAML_RESULT_HANDLER: config_managers[0].result_handler_config,
        consts.YAML_VALIDATIONS: [],
    }

    for config_manager in config_managers:
        yaml_config[consts.YAML_VALIDATIONS].append(
            config_manager.get_yaml_validation_block()
        )
    return yaml_config


def convert_config_to_json(config_managers: list) -> dict:
    """Return dict objects formatted for json validations.
    JSON configs correspond to ConfigManager objects and therefore can only correspond to
    one table validation.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """

    if len(config_managers) > 1:
        raise ValueError(
            "JSON configs can only be created for single table validations."
        )
    config_manager = config_managers[0]
    json_config = config_manager.config
    json_config[consts.CONFIG_SOURCE_CONN] = config_manager.get_source_connection()
    json_config[consts.CONFIG_TARGET_CONN] = config_manager.get_target_connection()
    return json_config


def run_validation(config_manager: ConfigManager, dry_run=False, verbose=False):
    """Run a single validation.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
        dry_run (bool): Print source and target SQL to stdout in lieu of validation.
        verbose (bool): Validation setting to log queries run.
    """
    # Only use cached connection for SQLAlchemy backends that manage reconnects for us.
    source_client = (
        config_manager.source_client
        if clients.is_sqlalchemy_backend(config_manager.source_client)
        else None
    )
    target_client = (
        config_manager.target_client
        if clients.is_sqlalchemy_backend(config_manager.target_client)
        else None
    )
    with DataValidation(
        config_manager.config,
        validation_builder=None,
        result_handler=None,
        verbose=verbose,
        source_client=source_client,
        target_client=target_client,
    ) as validator:

        if dry_run:
            sql_alchemy_clients = [
                "mysql",
                "oracle",
                "postgres",
                "db2",
                "mssql",
                "redshift",
                "snowflake",
            ]

            source_query = validator.validation_builder.get_source_query().compile()
            if config_manager.source_client.name in sql_alchemy_clients:
                source_query = source_query.compile(
                    config_manager.source_client.con.engine,
                    compile_kwargs={"literal_binds": True},
                )

            target_query = validator.validation_builder.get_target_query().compile()
            if config_manager.target_client.name in sql_alchemy_clients:
                target_query = target_query.compile(
                    config_manager.target_client.con.engine,
                    compile_kwargs={"literal_binds": True},
                )

            print(
                json.dumps(
                    {
                        "source_query": str(source_query),
                        "target_query": str(target_query),
                    },
                    indent=4,
                )
            )
        else:
            validator.execute()


def run_validations(args, config_managers):
    """Run and manage a series of validations.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    # TODO(issue/31): Add parallel execution logic
    for config_manager in config_managers:
        run_validation(config_manager, dry_run=args.dry_run, verbose=args.verbose)


def store_yaml_config_file(args, config_managers):
    """Build a YAML config file from the supplied configs.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    yaml_configs = convert_config_to_yaml(args, config_managers)
    config_file_path = _get_arg_config_file(args)
    cli_tools.store_validation(config_file_path, yaml_configs)


def store_json_config_file(args, config_managers):
    """Build a JSON config file from the supplied configs.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    json_config = convert_config_to_json(config_managers)
    config_file_path = _get_arg_config_file_json(args)
    cli_tools.store_validation(config_file_path, json_config)


def partition_and_store_config_files(args: Namespace) -> None:
    """Build multiple YAML Config files using user specified partition logic

    Args:
        args (Namespace): User specified Arguments

    Returns:
        None
    """
    # Default Validate Type
    if args.tables_list:
        config_managers = build_config_managers_from_args(args, consts.ROW_VALIDATION)
    else:
        setattr(args, "custom_query_type", "row")
        config_managers = build_config_managers_from_args(args, consts.CUSTOM_QUERY)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_builder.partition_configs()


def run(args) -> None:
    """Splits execution into:
    1. Build and save single Config file (YAML or JSON)
    2. Run Validations

    Args:
        args (Namespace): User specified Arguments.

    Returns:
        None
    """
    config_managers = build_config_managers_from_args(args)
    if args.config_file:
        store_yaml_config_file(args, config_managers)
    elif args.config_file_json:
        store_json_config_file(args, config_managers)
    else:
        run_validations(args, config_managers)


def run_connections(args):
    """Run commands related to connection management."""
    if args.connect_cmd == "list":
        cli_tools.list_connections()
    elif args.connect_cmd == "add":
        conn = cli_tools.get_connection_config_from_args(args)
        # Test getting a client to validate connection details
        with clients.get_data_client_ctx(conn) as _:
            cli_tools.store_connection(args.connection_name, conn)
    else:
        raise ValueError(f"Connections Argument '{args.connect_cmd}' is not supported")


def run_validation_configs(args):
    """Run commands related to validation config YAMLs."""
    if args.validation_config_cmd == "run":
        config_runner(args)
    elif args.validation_config_cmd == "list":
        config_dir = args.config_dir or "./"
        cli_tools.print_validations_in_dir(config_dir=config_dir)
    elif args.validation_config_cmd == "get":
        # Get and print yaml file config.
        yaml = cli_tools.get_validation(_get_arg_config_file(args))
        dump(yaml, sys.stdout)
    else:
        raise ValueError(f"Configs argument '{args.validate_cmd}' is not supported")


def validate(args):
    """Run commands related to data validation."""
    if args.validate_cmd in ["column", "row", "schema", "custom-query"]:
        run(args)
    else:
        raise ValueError(f"Validation Argument '{args.validate_cmd}' is not supported")


def main():
    # Create Parser and Get Deployment Info
    args = cli_tools.get_parsed_args()
    logging.basicConfig(
        level=LOG_LEVEL_MAP[args.log_level],
        format="%(asctime)s-%(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )
    if args.command == "connections":
        run_connections(args)
    elif args.command == "configs":
        run_validation_configs(args)
    elif args.command == "find-tables":
        print(find_tables_using_string_matching(args))
    elif args.command == "query":
        raw_query.print_raw_query_output(
            raw_query.run_raw_query_against_connection(args)
        )
    elif args.command == "validate":
        validate(args)
    elif args.command == "generate-table-partitions":
        cli_tools.check_no_yaml_files(args.partition_num, args.parts_per_file)
        partition_and_store_config_files(args)
    elif args.command == "deploy":
        from data_validation import app

        app.app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        raise ValueError(f"Positional Argument '{args.command}' is not supported")


if __name__ == "__main__":
    main()
