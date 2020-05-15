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

from data_validation.query_builder import query_builder


class ValidationBuilder(object):
    def __init__(self, config, source_client, target_client, verbose=False):
        """ Initialize a ValidationBuilder client

            :param config: The Validation config supplied
            :param source_client: The Ibis client for the source DB
            :param target_client: The Ibis client for the target DB
            :param verbose: If verbose, the Data Validation client will print queries run
        """
        self.verbose = verbose
        self.config = config
        self.validation_type = config["Type"]

        self.source_client = source_client
        self.target_client = target_client

        self.source_builder = self.get_query_builder(self.validation_type)
        self.target_builder = self.get_query_builder(self.validation_type)

    @staticmethod
    def get_query_builder(validation_type):
        """ Return Query Builder object given validation type """
        if validation_type == "Column":
            builder = query_builder.QueryBuilder.build_count_validator()
        elif validation_type == "GroupedColumn":
            builder = query_builder.QueryBuilder.build_partition_count_validator(
                days_past=700, limit=10
            )
        else:
            msg = "Validation Builder supplied unknown type: %s" % validation_type
            raise Exception(msg)

        return builder

    def get_source_query(self):
        """ Return query for source validation """
        source_config = {
            "data_client": self.source_client,
            "schema_name": self.config["schema_name"],
            "table_name": self.config["table_name"],
            "partition_column": self.config["partition_column"],
        }
        query = self.source_builder.compile(**source_config)
        if self.verbose:
            print("-- ** Source Query ** --")
            print(query)

        return query

    def get_target_query(self):
        """ Return query for source validation """
        target_config = {
            "data_client": self.target_client,
            "schema_name": self.config.get(
                "target_schema_name", self.config["schema_name"]
            ),
            "table_name": self.config.get(
                "target_table_name", self.config["table_name"]
            ),
            "partition_column": self.config.get(
                "target_partition_column", self.config["partition_column"]
            ),
        }
        query = self.target_builder.compile(**target_config)
        if self.verbose:
            print("-- ** Target Query ** --")
            print(query)

        return query
