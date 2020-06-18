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
data-validation run -t Column \
-sc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"},{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]' \
--sum '*' --count '*'

python -m data_validation run -t GroupedColumn \
-sc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]' \
--grouped-columns '["starttime"]' \
--sum '["tripduration"]' --count '["tripduration"]'
"""

import argparse
import json

from data_validation.config_manager import ConfigManager
from data_validation.data_validation import DataValidation


def configure_arg_parser():
    """Extract Args for Run."""
    parser = argparse.ArgumentParser(
        usage=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("command", help="Command to Run (run, build)")

    parser.add_argument(
        "--type", "-t", help="Type of Data Validation (Column, GroupedColumn)"
    )
    parser.add_argument("--source-conn", "-sc", help="Source connection details")
    parser.add_argument("--target-conn", "-tc", help="Target connection details")
    parser.add_argument(
        "--tables-list",
        "-tbls",
        help="JSON List of dict {schema:schema_name, table:table_name}",
    )
    parser.add_argument(
        "--count",
        "-count",
        help="JSON List of columns sum '[\"col_a\"]' or * for all columns",
    )
    parser.add_argument(
        "--sum",
        "-sum",
        help="JSON List of columns sum '[\"col_a\"]' or * for all numeric",
    )
    parser.add_argument(
        "--grouped-columns",
        "-gc",
        help="JSON List of columns to use in group by '[\"col_a\"]'",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    return parser.parse_args()


def get_aggregate_config(args, config_manager):
    """Return list of formated aggregation objects."""
    aggregate_configs = [
        config_manager.build_config_count_aggregate()
    ]

    if args.count:
        col_args = None if args.count == "*" else json.loads(args.count)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "count", col_args, None
        )
    if args.sum:
        col_args = None if args.sum == "*" else json.loads(args.sum)
        aggregate_configs += config_manager.build_config_column_aggregates(
            "sum", col_args, ["int64", "float64"]
        )

    return aggregate_configs


def build_config_from_args(args, config_manager):
    """Return config object ready to execute."""
    config_manager.append_aggregates(get_aggregate_config(args, config_manager))
    if config_manager.validation_type == "GroupedColumn":
        grouped_columns = json.loads(args.grouped_columns)
        config_manager.append_query_groups(
            config_manager.build_config_grouped_columns(grouped_columns)
        )
    # TODO(GH#18): Add query filter config logic

    return config_manager.config


def build_configs_from_args(args):
    """Return a list of config dicts ready to execute."""
    configs = []

    config_type = args.type
    source_conn = json.loads(args.source_conn)
    target_conn = json.loads(args.target_conn)

    source_client = DataValidation.get_data_client(source_conn)
    target_client = DataValidation.get_data_client(target_conn)

    tables_list = json.loads(args.tables_list)
    for table_obj in tables_list:
        config_manager = ConfigManager.build_config_manager(
            config_type,
            source_conn,
            target_conn,
            source_client,
            target_client,
            table_obj,
            verbose=False,
        )
        configs.append(build_config_from_args(args, config_manager))

    return configs


def run_validation(config, verbose=False):
    """Run a single validation."""
    validator = DataValidation(
        config, validation_builder=None, result_handler=None, verbose=verbose
    )
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
        raise ValueError(f"Positional Argument '{args.command}' is not supported")


if __name__ == "__main__":
    main()
