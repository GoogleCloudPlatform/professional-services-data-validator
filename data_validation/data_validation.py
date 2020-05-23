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

import copy

from ibis.bigquery.client import BigQueryClient
from ibis.sql.mysql.client import MySQLClient
from ibis.sql.postgres.client import PostgreSQLClient

from data_validation import consts, exceptions
from data_validation.validation_builder import ValidationBuilder
from data_validation.result_handlers.result_handler import ResultHandler

# If you have a Teradata License there is an optional teradatasql import
try:
    from ibis_teradata.client import TeradataClient
except Exception:
    TeradataClient = None


CLIENT_LOOKUP = {
    "BigQuery": BigQueryClient,
    "MySQL": MySQLClient,
    "Postgres": PostgreSQLClient,
    "Teradata": TeradataClient,
}


""" The DataValidation class is where the code becomes source/target aware

    The class builds specific source and target clients and is likely where someone would go to
    customize their validation process.

    data_validator = DataValidation(builder, source_config, target_config, result_handler=None, verbose=False)
"""


class DataValidation(object):
    def __init__(
        self, config, validation_builder=None, result_handler=None, verbose=False
    ):
        """ Initialize a DataValidation client

            :param builder: A QueryBuilder client with the structure of the desired validation
            :param source_config: The source config used for the comparison
            :param target_config: The target config used for the comparison
            :param result_handler: A ResultHandler client to be used when storing results (default is print)
            :param verbose: If verbose, the Data Validation client will print queries run
        """
        self.verbose = verbose

        # Data Client Management
        self.config = config

        self.source_client = DataValidation.get_data_client(self.config["source_conn"])
        self.target_client = DataValidation.get_data_client(self.config["target_conn"])

        # Initialize Validation Builder if None was supplied
        self.validation_builder = validation_builder or ValidationBuilder(
            config, self.source_client, self.target_client, verbose=self.verbose
        )

        # Initialize the default Result Handler if None was supplied
        self.result_handler = result_handler or ResultHandler()

    # TODO(dhercher) we planned on shifting this to use an Execution Handler.
    # Leaving to to swast on the design of how this should look.
    def execute(self):
        """ Execute Queries and Store Results """
        source_df = self.source_client.execute(
            self.validation_builder.get_source_query()
        )
        target_df = self.source_client.execute(
            self.validation_builder.get_target_query()
        )

        result_df = self.combine_data(source_df, target_df)

        # Call Result Handler to Manage Results
        return self.result_handler.execute(self.config, result_df)

    @staticmethod
    def get_data_client(connection_config):
        """ Return DataClient client from given configuration """
        connection_config = copy.deepcopy(connection_config)
        source_type = connection_config.pop(consts.SOURCE_TYPE)

        if source_type not in CLIENT_LOOKUP:
            msg = 'ConfigurationError: Source type "{source_type}" is not supported'.format(
                source_type=source_type
            )
            raise Exception(msg)

        try:
            data_client = CLIENT_LOOKUP[source_type](**connection_config)
        except Exception:
            msg = 'Connection Type "{source_type}" could not connect'.format(
                source_type=source_type
            )
            raise exceptions.DataClientConnectionFailure(msg)

        return data_client

    def combine_data(self, source_df, target_df):
        """ TODO: Return List of Dictionaries """
        # Clean Data to Standardize
        join_on_fields = self.validation_builder.get_group_aliases()

        if join_on_fields:
            df = source_df.merge(
                target_df,
                how="outer",
                on=join_on_fields,
                suffixes=(consts.INPUT_SUFFIX, consts.OUTPUT_SUFFIX),
            )
        else:
            df = source_df.join(
                target_df,
                how="outer",
                lsuffix=consts.INPUT_SUFFIX,
                rsuffix=consts.OUTPUT_SUFFIX,
            )
        return df
