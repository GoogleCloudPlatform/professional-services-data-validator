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
import datetime
import warnings

from ibis.bigquery.client import BigQueryClient
import ibis.pandas
from ibis.sql.mysql.client import MySQLClient
from ibis.sql.postgres.client import PostgreSQLClient

from data_validation import consts, combiner, exceptions, metadata
from data_validation.config_manager import ConfigManager
from data_validation.validation_builder import ValidationBuilder


# TODO(googleapis/google-auth-library-python#520): Remove after issue is resolved
warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

# If you have a Teradata License there is an optional teradatasql import
try:
    from third_party.ibis.ibis_teradata.client import TeradataClient
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

        Args:
            config (dict): The validation config used for the comparison
            validation_builder (ValidationBuilder): Optional instance of a ValidationBuilder
            result_handler (ResultHandler): Optional instance of as ResultHandler client
            verbose (bool): If verbose, the Data Validation client will print the queries run
        """
        self.verbose = verbose

        # Data Client Management
        self.config = config

        self.source_client = DataValidation.get_data_client(
            self.config[consts.CONFIG_SOURCE_CONN]
        )
        self.target_client = DataValidation.get_data_client(
            self.config[consts.CONFIG_TARGET_CONN]
        )

        self.config_manager = ConfigManager(
            config, self.source_client, self.target_client, verbose=self.verbose
        )

        # Initialize Validation Builder if None was supplied
        self.validation_builder = validation_builder or ValidationBuilder(
            self.config_manager
        )

        # Initialize the default Result Handler if None was supplied
        self.result_handler = result_handler or self.config_manager.get_result_handler()

    # TODO(dhercher) we planned on shifting this to use an Execution Handler.
    # Leaving to to swast on the design of how this should look.
    def execute(self):
        """ Execute Queries and Store Results """
        run_metadata = metadata.RunMetadata()

        source_df = self.source_client.execute(
            self.validation_builder.get_source_query()
        )
        target_df = self.source_client.execute(
            self.validation_builder.get_target_query()
        )
        join_on_fields = self.validation_builder.get_group_aliases()
        pandas_client = ibis.pandas.connect(
            {combiner.DEFAULT_SOURCE: source_df, combiner.DEFAULT_TARGET: target_df}
        )

        run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)
        run_metadata.validations = self.validation_builder.get_metadata()
        result_df = combiner.generate_report(
            pandas_client, run_metadata, join_on_fields=join_on_fields
        )

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

    def combine_data(self, source_df, target_df, join_on_fields):
        """ TODO: Return List of Dictionaries """
        # Clean Data to Standardize
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
