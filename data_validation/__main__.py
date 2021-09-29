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
from yaml import dump, load, Dumper, Loader

from data_validation import (
    cli_tools,
    clients,
    consts,
    jellyfish_distance,
    state_manager,
)
from data_validation.config_manager import ConfigManager
from data_validation.data_validation import DataValidation


def _get_arg_config_file(args):
    """Return String yaml config file path."""
    if not args.config_file:
        raise ValueError("YAML Config File was not supplied.")

    return args.config_file


def _get_yaml_config_from_file(config_file_path):
    """Return Dict of yaml validation data."""
    with open(config_file_path, "r") as yaml_file:
        yaml_configs = load(yaml_file.read(), Loader=Loader)

    return yaml_configs


def get_aggregate_config(args, config_manager):
    """Return list of formated aggregation objects.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
    """
    aggregate_configs = [config_manager.build_config_count_aggregate()]

    if args.count:
        col_args = None if args.count == "*" else cli_tools.get_arg_list(args.count)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "count", col_args, None
        )
    if args.sum:
        col_args = None if args.sum == "*" else cli_tools.get_arg_list(args.sum)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "sum", col_args, consts.NUMERIC_DATA_TYPES
        )
    if args.avg:
        col_args = None if args.avg == "*" else cli_tools.get_arg_list(args.avg)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "avg", col_args, consts.NUMERIC_DATA_TYPES
        )
    if args.min:
        col_args = None if args.min == "*" else cli_tools.get_arg_list(args.min)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "min", col_args, consts.NUMERIC_DATA_TYPES
        )
    if args.max:
        col_args = None if args.max == "*" else cli_tools.get_arg_list(args.max)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "max", col_args, consts.NUMERIC_DATA_TYPES
        )
    return aggregate_configs


def build_config_from_args(args, config_manager):
    """Return config manager object ready to execute.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
    """
    config_manager.append_aggregates(get_aggregate_config(args, config_manager))
    if args.primary_keys and not args.grouped_columns:
        raise ValueError(
            "Grouped columns must be specified for primary key level validation."
        )
    if args.grouped_columns:
        grouped_columns = cli_tools.get_arg_list(args.grouped_columns)
        config_manager.append_query_groups(
            config_manager.build_config_grouped_columns(grouped_columns)
        )
    if args.primary_keys:
        primary_keys = cli_tools.get_arg_list(args.primary_keys, default_value=[])
        config_manager.append_primary_keys(
            config_manager.build_config_grouped_columns(primary_keys)
        )

    # TODO(GH#18): Add query filter config logic

    return config_manager


def build_config_managers_from_args(args):
    """Return a list of config managers ready to execute."""
    configs = []

    if args.type is None:
        config_type = args.validate_cmd.capitalize()
    else:
        config_type = args.type

    result_handler_config = None
    if args.bq_result_handler:
        result_handler_config = cli_tools.get_result_handler(
            args.bq_result_handler, args.service_account
        )
    elif args.result_handler_config:
        result_handler_config = cli_tools.get_result_handler(
            args.result_handler_config, args.service_account
        )

    # Schema validation will not accept filters, labels, or threshold as flags
    filter_config, labels, threshold = [], [], 0.0
    if config_type != consts.SCHEMA_VALIDATION:
        if args.filters:
            filter_config = cli_tools.get_filters(args.filters)
        if args.threshold:
            threshold = args.threshold
        labels = cli_tools.get_labels(args.labels)

    mgr = state_manager.StateManager()
    source_client = clients.get_data_client(mgr.get_connection_config(args.source_conn))
    target_client = clients.get_data_client(mgr.get_connection_config(args.target_conn))

    format = args.format if args.format else "table"

    is_filesystem = source_client._source_type == "FileSystem"
    tables_list = cli_tools.get_tables_list(
        args.tables_list, default_value=[], is_filesystem=is_filesystem
    )

    for table_obj in tables_list:
        config_manager = ConfigManager.build_config_manager(
            config_type,
            args.source_conn,
            args.target_conn,
            table_obj,
            labels,
            threshold,
            format,
            source_client=source_client,
            target_client=target_client,
            result_handler_config=result_handler_config,
            filter_config=filter_config,
            verbose=args.verbose,
        )
        if config_type != consts.SCHEMA_VALIDATION:
            config_manager = build_config_from_args(args, config_manager)

        configs.append(config_manager)

    return configs


def build_config_managers_from_yaml(args):
    """Returns List[ConfigManager] instances ready to be executed."""
    config_managers = []

    config_file_path = _get_arg_config_file(args)
    yaml_configs = _get_yaml_config_from_file(config_file_path)

    mgr = state_manager.StateManager()
    source_conn = mgr.get_connection_config(yaml_configs[consts.YAML_SOURCE])
    target_conn = mgr.get_connection_config(yaml_configs[consts.YAML_TARGET])

    source_client = clients.get_data_client(source_conn)
    target_client = clients.get_data_client(target_conn)

    for config in yaml_configs[consts.YAML_VALIDATIONS]:
        config[consts.CONFIG_SOURCE_CONN] = source_conn
        config[consts.CONFIG_TARGET_CONN] = target_conn
        config[consts.CONFIG_RESULT_HANDLER] = yaml_configs[consts.YAML_RESULT_HANDLER]
        config_manager = ConfigManager(
            config, source_client, target_client, verbose=args.verbose
        )

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
    score_cutoff = args.score_cutoff or 0.8

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
    """Return results of raw query for adhoc usage."""
    mgr = state_manager.StateManager()
    client = clients.get_data_client(mgr.get_connection_config(args.conn))

    with client.raw_sql(args.query, results=True) as cur:
        return cur.fetchall()


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


def run_validation(config_manager, verbose=False):
    """Run a single validation.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
        verbose (bool): Validation setting to log queries run.
    """
    validator = DataValidation(
        config_manager.config,
        validation_builder=None,
        result_handler=None,
        verbose=verbose,
    )
    validator.execute()


def run_validations(args, config_managers):
    """Run and manage a series of validations.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    # TODO(issue/31): Add parallel execution logic
    for config_manager in config_managers:
        run_validation(config_manager, verbose=args.verbose)


def store_yaml_config_file(args, config_managers):
    """Build a YAML config file from the supplied configs.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    config_file_path = _get_arg_config_file(args)
    yaml_configs = convert_config_to_yaml(args, config_managers)
    yaml_config_str = dump(yaml_configs, Dumper=Dumper)

    with open(config_file_path, "w") as yaml_file:
        yaml_file.write(yaml_config_str)


def run(args):
    """ """
    config_managers = build_config_managers_from_args(args)

    if args.config_file:
        store_yaml_config_file(args, config_managers)
    else:
        run_validations(args, config_managers)


def run_connections(args):
    """ Run commands related to connection management."""
    if args.connect_cmd == "list":
        cli_tools.list_connections()
    elif args.connect_cmd == "add":
        conn = cli_tools.get_connection_config_from_args(args)
        # Test getting a client to validate connection details
        _ = clients.get_data_client(conn)
        cli_tools.store_connection(args.connection_name, conn)
    else:
        raise ValueError(f"Connections Argument '{args.connect_cmd}' is not supported")


def validate(args):
    """ Run commands related to data validation."""
    if args.validate_cmd == "column" or args.validate_cmd == "schema":
        run(args)
    else:
        raise ValueError(f"Validation Argument '{args.validate_cmd}' is not supported")


def main():
    # Create Parser and Get Deployment Info
    args = cli_tools.get_parsed_args()

    if args.command == "run":
        run(args)
    elif args.command == "connections":
        run_connections(args)
    elif args.command == "run-config":
        config_managers = build_config_managers_from_yaml(args)
        run_validations(args, config_managers)
    elif args.command == "find-tables":
        print(find_tables_using_string_matching(args))
    elif args.command == "query":
        print(run_raw_query_against_connection(args))
    elif args.command == "validate":
        validate(args)
    else:
        raise ValueError(f"Positional Argument '{args.command}' is not supported")


if __name__ == "__main__":
    main()
