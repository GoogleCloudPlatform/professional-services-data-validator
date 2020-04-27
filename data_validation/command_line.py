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

from data_validation import data_validation, consts
from data_validation.query_builder import query_builder


""" Example: python3 command_line.py can now be used as `validate`
validate -t COLUMN \
-sc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tc '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}' \
-tbls 'bigquery-public-data.new_york_citibike.citibike_trips bigquery-public-data.new_york_citibike.citibike_stations'
"""

def create_builder(args):
    """ Initialize and return Query Builder """
    if args.type == "COLUMN":
        builder = query_builder.QueryBuilder.build_count_validator()
    elif args.type == "GROUPED_COLUMN":
        # TODO this is wrong
        builder = query_builder.QueryBuilder.build_partition_count_validator(
                    days_past=700, limit=10)

    return builder

def configure_arg_parser(parser):
    parser.add_argument("--type", "-t",
                        help="Type of Data Validation (COLUMN, GROUPED_COLUMN)")
    parser.add_argument("--source-conn", "-sc",
                        help="Source connection details")
    parser.add_argument("--target-conn", "-tc",
                        help="Target connection details")
    parser.add_argument("--tables-list", "-tbls",
                        help="List of schema.table tables separated by spaces")
    parser.add_argument("--verbose", "-v", action='store_true',
                        help="Verbose logging")

    return parser

def main():
    # Create Parser and Get Deployment
    parser = argparse.ArgumentParser()
    configure_arg_parser(parser)
    args = parser.parse_args()

    # Extract args into config
    config = {}
    config["source_config"] = json.loads(args.source_conn)
    config["target_config"] = json.loads(args.target_conn)
    config["tables_list"] = args.tables_list.split(" ")

    # '{"project_id":"pso-kokoro-resources","source_type":"BigQuery"}'

    # Create Builder
    builder = create_builder(args)

    # Execute Validation
    validator = data_validation.DataValidation.init_data_validation(builder, config,
                    result_handler=None, verbose=args.verbose)
    df = validator.execute()


if __name__=="__main__":
    main()
