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
import copy
import json

from data_validation import data_validation, consts


""" Example: python3 command_line.py can now be used as `data-validation`
data-validation run -t Column \
-sc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tbls '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"},{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_stations"}]'
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
    parser.add_argument("--verbose", "-v", action='store_true',
                        help="Verbose logging")

    return parser.parse_args()


def build_configs_from_args(args):
    """Return a list of config dicts ready to execute."""
    configs = []

    glb_config = {}
    glb_config[consts.CONFIG_TYPE] = args.type
    glb_config[consts.CONFIG_SOURCE_CONN] = json.loads(args.source_conn)
    glb_config[consts.CONFIG_TARGET_CONN] = json.loads(args.target_conn)

    tables_list = json.loads(args.tables_list)
    for table_obj in tables_list:
        config = copy.deepcopy(glb_config)
        
        config[consts.CONFIG_SCHEMA_NAME] = table_obj[consts.CONFIG_SCHEMA_NAME]
        config[consts.CONFIG_TABLE_NAME] = table_obj[consts.CONFIG_TABLE_NAME]
        config[consts.CONFIG_TARGET_SCHEMA_NAME] = table_obj.get(consts.CONFIG_TARGET_SCHEMA_NAME) or table_obj[consts.CONFIG_SCHEMA_NAME]
        config[consts.CONFIG_TARGET_TABLE_NAME] = table_obj.get(consts.CONFIG_TARGET_TABLE_NAME) or table_obj[consts.CONFIG_TABLE_NAME]

        configs.append(config)

    return configs


def run_validations(args, configs):
    """Run a single validation."""
    for config in configs:
        validator = data_validation.DataValidation(
            config, validation_builder=None, result_handler=None, verbose=args.verbose)
        df = validator.execute()


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
