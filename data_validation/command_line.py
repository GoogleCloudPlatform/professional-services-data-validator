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

import argparse
import json
import logging

from data_validation import data_validation, consts


""" Example: python3 command_line.py can now be used as `data-validation`
data-validation run -t Column \
-sc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"},{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' \
--sum '*' --count '*'

data-validation run -t GroupedColumn \
-sc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' \
--grouped-columns '["starttime"]' \
--sum '["tripduration"]' --count '["tripduration"]'
"""


def configure_arg_parser():
    """Extract Args for Run."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Command to Run (run, build)")

    parser.add_argument("--type", "-t",
                        help="Type of Data Validation (Column, GroupedColumn)")
    parser.add_argument("--source-conn", "-sc",
                        help="Source connection details")
    parser.add_argument("--target-conn", "-tc",
                        help="Target connection details")
    parser.add_argument("--tables-list", "-tbls",
                        help="JSON List of dict {schema:schema_name, table:table_name}")
    parser.add_argument("--count", "-count",
                        help="JSON List of columns sum '[\"col_a\"]' or * for all columns")
    parser.add_argument("--sum", "-sum",
                        help="JSON List of columns sum '[\"col_a\"]' or * for all numeric")
    parser.add_argument("--grouped-columns", "-gc",
                        help="JSON List of columns to use in group by '[\"col_a\"]'")
    parser.add_argument("--verbose", "-v", action='store_true',
                        help="Verbose logging")

    return parser.parse_args()


def add_grouped_column_config(args, config, source_table, target_table):
    """Add formated grouped column objects to config."""
    if args.grouped_columns:
        grouped_columns = json.loads(args.grouped_columns)
        for column in grouped_columns:
            if column not in source_table.columns:
                raise Exception(f"GroupedColumn DNE: {source_table.op().name}.{column}")
            column_config = {
                consts.CONFIG_SOURCE_COLUMN: column,
                consts.CONFIG_TARGET_COLUMN: column,
                consts.CONFIG_FIELD_ALIAS: column,
                consts.CONFIG_CAST: None,
            }
            config[consts.CONFIG_GROUPED_COLUMNS].append(column_config)


def _add_aggregate_of_type(config, source_table, target_table, agg_type, arg_value, supported_types):
    """Add aggregate objects which align with the config and supported types."""
    whitelist_columns = source_table.columns if arg_value=="*" else json.loads(arg_value)
    for column in source_table.columns:
        if column not in whitelist_columns:
            continue
        elif column not in target_table.columns:
            logging.info(f"Skipping Agg {agg_type}: {source_table.op().name}.{column}")
            continue
        elif supported_types and str(source_table[column].type()) not in supported_types:
            continue

        aggregate_config = {
            consts.CONFIG_SOURCE_COLUMN: column,
            consts.CONFIG_TARGET_COLUMN: column,
            consts.CONFIG_FIELD_ALIAS: f"{agg_type}__{column}",
            consts.CONFIG_TYPE: agg_type,
        }
        config[consts.CONFIG_AGGREGATES].append(aggregate_config)


def add_aggregate_config(args, config, source_table, target_table):
    """Add formated aggregation objects to config."""
    if args.count:
        _add_aggregate_of_type(
            config, source_table, target_table, "count", args.count, None)
    if args.sum:
        _add_aggregate_of_type(
            config, source_table, target_table, "sum", args.sum, ["int64", "float64"])


def build_config_from_args(args, config_type, source_conn, target_conn, table_obj, source_client, target_client):
    """Return config object ready to execute."""
    config = {
        consts.CONFIG_TYPE: config_type,
        consts.CONFIG_SOURCE_CONN: source_conn,
        consts.CONFIG_TARGET_CONN: target_conn,
        consts.CONFIG_SCHEMA_NAME: table_obj[consts.CONFIG_SCHEMA_NAME],
        consts.CONFIG_TABLE_NAME: table_obj[consts.CONFIG_TABLE_NAME],
        consts.CONFIG_TARGET_SCHEMA_NAME: table_obj.get(consts.CONFIG_TARGET_SCHEMA_NAME) or table_obj[consts.CONFIG_SCHEMA_NAME],
        consts.CONFIG_TARGET_TABLE_NAME: table_obj.get(consts.CONFIG_TARGET_TABLE_NAME) or table_obj[consts.CONFIG_TABLE_NAME],
        consts.CONFIG_AGGREGATES: [],
        consts.CONFIG_FILTERS: [],
        consts.CONFIG_GROUPED_COLUMNS: [],
    }

    source_table = source_client.table(config[consts.CONFIG_TABLE_NAME], database=config[consts.CONFIG_SCHEMA_NAME])
    target_table = target_client.table(config[consts.CONFIG_TARGET_TABLE_NAME], database=config[consts.CONFIG_TARGET_SCHEMA_NAME])

    add_aggregate_config(args, config, source_table, target_table)
    # TODO(GH#18): Add query filter config logic
    if config_type == "GroupedColumn":
        add_grouped_column_config(args, config, source_table, target_table)

    return config


def build_configs_from_args(args):
    """Return a list of config dicts ready to execute."""
    configs = []

    config_type = args.type
    source_conn = json.loads(args.source_conn)
    target_conn = json.loads(args.target_conn)

    source_client = data_validation.DataValidation.get_data_client(source_conn)
    target_client = data_validation.DataValidation.get_data_client(target_conn)

    tables_list = json.loads(args.tables_list)
    for table_obj in tables_list:
        config = build_config_from_args(args, config_type, source_conn, target_conn, table_obj, source_client, target_client)
        configs.append(config)

    return configs


def run_validation(config, verbose=False):
    """Run a single validation."""
    validator = data_validation.DataValidation(
        config, validation_builder=None, result_handler=None, verbose=verbose)
    validator.execute()


def run_validations(args, configs):
    """Run and manage a series of validations."""
    for config in configs:
        run_validation(config, verbose=args.verbose)


def main():
    # Create Parser and Get Deployment Info
    args = configure_arg_parser()

    if args.command == "run":
        configs = build_configs_from_args(args)
        run_validations(args, configs)
    else:
        raise Exception(f"Positional Argument '{args.command}' is not supported")


if __name__=="__main__":
    main()
