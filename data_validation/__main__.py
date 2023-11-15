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
    jellyfish_distance,
    state_manager,
)
from data_validation.config_manager import ConfigManager
from data_validation.data_validation import DataValidation
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
    """Return String yaml config file path."""
    if not args.config_file:
        raise ValueError("YAML Config File was not supplied.")

    return args.config_file


def get_aggregate_config(args, config_manager: ConfigManager):
    """Return list of formated aggregation objects.

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
            supported_data_types,
            cast_to_bigint=cast_to_bigint,
        )
    if args.max:
        col_args = None if args.max == "*" else cli_tools.get_arg_list(args.max)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "max",
            col_args,
            args.exclude_columns,
            supported_data_types,
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


def get_calculated_config(args, config_manager):
    """Return list of formatted calculated objects.

    Args:
        config_manager(ConfigManager): Validation config manager instance.
    """
    calculated_configs = []
    fields = []
    if args.hash:
        col_list = None if args.hash == "*" else cli_tools.get_arg_list(args.hash)
        fields = config_manager.build_dependent_aliases("hash", col_list)
    elif args.concat:
        col_list = None if args.concat == "*" else cli_tools.get_arg_list(args.concat)
        fields = config_manager.build_dependent_aliases("concat", col_list)

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
    if args.hash:
        config_manager.append_comparison_fields(
            config_manager.build_config_comparison_fields(
                ["hash__all"], depth=max_depth
            )
        )
    elif args.concat:
        config_manager.append_comparison_fields(
            config_manager.build_config_comparison_fields(
                ["concat__all"], depth=max_depth
            )
        )
    return calculated_configs


def build_config_from_args(args: Namespace, config_manager: ConfigManager):
    """Append build configs to ConfigManager object.

    Args:
        args (Namespace): User specified Arguments
        config_manager (ConfigManager): Validation config manager instance.
    """
    # Append SCHEMA_VALIDATION configs
    if config_manager.validation_type == consts.SCHEMA_VALIDATION:
        if args.exclusion_columns is not None:
            exclusion_columns = cli_tools.get_arg_list(args.exclusion_columns)
            config_manager.append_exclusion_columns(
                [col.casefold() for col in exclusion_columns]
            )
        config_manager.append_allow_list(args.allow_list, args.allow_list_file)

    # Append CUSTOM_QUERY configs
    if config_manager.validation_type == consts.CUSTOM_QUERY:
        config_manager.append_custom_query_type(args.custom_query_type)

        # Get source sql query from source sql file or inline query
        if args.source_query:
            source_query_str = config_manager.get_query_from_inline(args.source_query)
        else:
            source_query_str = config_manager.get_query_from_file(
                args.source_query_file
            )
        config_manager.append_source_query(source_query_str)

        # Get target sql query from target sql file or inline query
        if args.target_query:
            target_query_str = config_manager.get_query_from_inline(args.target_query)
        else:
            target_query_str = config_manager.get_query_from_file(
                args.target_query_file
            )
        config_manager.append_target_query(target_query_str)

        # For custom-query column command
        if args.custom_query_type == consts.COLUMN_VALIDATION.lower():
            config_manager.append_aggregates(get_aggregate_config(args, config_manager))

        # For custom-query row command
        if args.custom_query_type == consts.ROW_VALIDATION.lower():

            # Append Comparison fields
            if args.comparison_fields is not None:
                comparison_fields = cli_tools.get_arg_list(
                    args.comparison_fields, default_value=[]
                )
                config_manager.append_comparison_fields(
                    config_manager.build_config_comparison_fields(comparison_fields)
                )

            # Append calculated fields: --hash/--concat
            config_manager.append_calculated_fields(
                get_calculated_config(args, config_manager)
            )

            # Append primary_keys
            primary_keys = cli_tools.get_arg_list(args.primary_keys)
            config_manager.append_primary_keys(
                config_manager.build_column_configs(primary_keys)
            )

    # Append COLUMN_VALIDATION configs
    if config_manager.validation_type == consts.COLUMN_VALIDATION:
        config_manager.append_aggregates(get_aggregate_config(args, config_manager))
        if args.grouped_columns is not None:
            grouped_columns = cli_tools.get_arg_list(args.grouped_columns)
            config_manager.append_query_groups(
                config_manager.build_column_configs(grouped_columns)
            )

    # Append ROW_VALIDATION configs
    if config_manager.validation_type == consts.ROW_VALIDATION:

        # Append calculated fields: --hash/--concat
        config_manager.append_calculated_fields(
            get_calculated_config(args, config_manager)
        )

        # Append Comparison fields
        if args.comparison_fields is not None:
            comparison_fields = cli_tools.get_arg_list(
                args.comparison_fields, default_value=[]
            )
            config_manager.append_comparison_fields(
                config_manager.build_config_comparison_fields(comparison_fields)
            )

        # Append primary_keys
        primary_keys = cli_tools.get_arg_list(args.primary_keys)
        config_manager.append_primary_keys(
            config_manager.build_column_configs(primary_keys)
        )

    return config_manager


def build_config_managers_from_args(
    args: Namespace, validate_cmd: str = None
) -> List[ConfigManager]:
    """Return a list of config managers ready to execute."""
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


def config_runner(args):
    if args.config_dir:
        mgr = state_manager.StateManager(file_system_root_path=args.config_dir)
        config_file_names = mgr.list_validations_in_dir(args.config_dir)

        config_managers = []
        for file in config_file_names:
            config_managers.extend(build_config_managers_from_yaml(args, file))
    else:
        config_file_path = _get_arg_config_file(args)
        config_managers = build_config_managers_from_yaml(args, config_file_path)

    run_validations(args, config_managers)


def build_config_managers_from_yaml(args, config_file_path):
    """Returns List[ConfigManager] instances ready to be executed."""
    if "config_dir" in args and args.config_dir:
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


def _compare_match_tables(source_table_map, target_table_map, score_cutoff=0.8):
    """Return dict config object from matching tables."""
    # TODO(dhercher): evaluate if improved comparison and score cutoffs should be used.
    table_configs = []

    target_keys = target_table_map.keys()
    for source_key in source_table_map:
        target_key = jellyfish_distance.extract_closest_match(
            source_key, target_keys, score_cutoff=score_cutoff
        )
        if target_key is None:
            continue

        table_config = {
            consts.CONFIG_SCHEMA_NAME: source_table_map[source_key][
                consts.CONFIG_SCHEMA_NAME
            ],
            consts.CONFIG_TABLE_NAME: source_table_map[source_key][
                consts.CONFIG_TABLE_NAME
            ],
            consts.CONFIG_TARGET_SCHEMA_NAME: target_table_map[target_key][
                consts.CONFIG_SCHEMA_NAME
            ],
            consts.CONFIG_TARGET_TABLE_NAME: target_table_map[target_key][
                consts.CONFIG_TABLE_NAME
            ],
        }
        table_configs.append(table_config)

    return table_configs


def get_table_map(client, allowed_schemas=None):
    """Return dict with searchable keys for table matching."""
    table_map = {}
    table_objs = clients.get_all_tables(client, allowed_schemas=allowed_schemas)

    for table_obj in table_objs:
        table_key = ".".join([t for t in table_obj if t])
        table_map[table_key] = {
            consts.CONFIG_SCHEMA_NAME: table_obj[0],
            consts.CONFIG_TABLE_NAME: table_obj[1],
        }

    return table_map


def find_tables_using_string_matching(args):
    """Return JSON String with matched tables for use in validations."""
    score_cutoff = args.score_cutoff or 1

    mgr = state_manager.StateManager()
    source_client = clients.get_data_client(mgr.get_connection_config(args.source_conn))
    target_client = clients.get_data_client(mgr.get_connection_config(args.target_conn))

    allowed_schemas = cli_tools.get_arg_list(args.allowed_schemas)
    source_table_map = get_table_map(source_client, allowed_schemas=allowed_schemas)
    target_table_map = get_table_map(target_client)

    table_configs = _compare_match_tables(
        source_table_map, target_table_map, score_cutoff=score_cutoff
    )
    return json.dumps(table_configs)


def run_raw_query_against_connection(args):
    """Return results of raw query for ad hoc usage."""
    mgr = state_manager.StateManager()
    client = clients.get_data_client(mgr.get_connection_config(args.conn))
    cursor = client.raw_sql(args.query)
    res = cursor.fetchall()
    try:
        cursor.close()
    except Exception:
        pass
    return res


def convert_config_to_yaml(args, config_managers):
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


def run_validation(config_manager, dry_run=False, verbose=False):
    """Run a single validation.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
        dry_run (bool): Print source and target SQL to stdout in lieu of validation.
        verbose (bool): Validation setting to log queries run.
    """
    validator = DataValidation(
        config_manager.config,
        validation_builder=None,
        result_handler=None,
        verbose=verbose,
    )

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
        if config_manager.config and consts.CONFIG_FILE in config_manager.config:
            logging.info(
                "Currently running the validation for yml file: %s",
                config_manager.config[consts.CONFIG_FILE],
            )
            try:
                run_validation(
                    config_manager, dry_run=args.dry_run, verbose=args.verbose
                )
            except Exception as e:
                logging.error(
                    "Error %s occurred while running config file %s. Skipping it for now.",
                    str(e),
                    config_manager.config[consts.CONFIG_FILE],
                )
        else:
            run_validation(config_manager, dry_run=args.dry_run, verbose=args.verbose)


def store_yaml_config_file(args, config_managers):
    """Build a YAML config file from the supplied configs.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    yaml_configs = convert_config_to_yaml(args, config_managers)
    config_file_path = _get_arg_config_file(args)
    cli_tools.store_validation(config_file_path, yaml_configs)


def partition_and_store_config_files(args: Namespace) -> None:
    """Build multiple YAML Config files using user specified partition logic

    Args:
        args (Namespace): User specified Arguments

    Returns:
        None
    """
    # Default Validate Type
    config_managers = build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_builder.partition_configs()


def run(args) -> None:
    """Splits execution into:
    1. Build and save single Yaml Config file
    2. Run Validations

    Args:
        args (Namespace): User specified Arguments.

    Returns:
        None
    """
    config_managers = build_config_managers_from_args(args)
    if args.config_file:
        store_yaml_config_file(args, config_managers)
    else:
        run_validations(args, config_managers)


def run_connections(args):
    """Run commands related to connection management."""
    if args.connect_cmd == "list":
        cli_tools.list_connections()
    elif args.connect_cmd == "add":
        conn = cli_tools.get_connection_config_from_args(args)
        # Test getting a client to validate connection details
        _ = clients.get_data_client(conn)
        cli_tools.store_connection(args.connection_name, conn)
    else:
        raise ValueError(f"Connections Argument '{args.connect_cmd}' is not supported")


def run_validation_configs(args):
    """Run commands related to validation config YAMLs."""
    if args.validation_config_cmd == "run":
        config_runner(args)
    elif args.validation_config_cmd == "list":
        cli_tools.list_validations()
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
        print(run_raw_query_against_connection(args))
    elif args.command == "validate":
        validate(args)
    elif args.command == "generate-table-partitions":
        partition_and_store_config_files(args)
    elif args.command == "deploy":
        from data_validation import app

        app.app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        raise ValueError(f"Positional Argument '{args.command}' is not supported")


if __name__ == "__main__":
    main()
