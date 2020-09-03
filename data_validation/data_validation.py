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
import json
import pandas
import warnings

import google.oauth2.service_account
from ibis.bigquery.client import BigQueryClient
import ibis.pandas
from ibis.sql.mysql.client import MySQLClient
from ibis.sql.postgres.client import PostgreSQLClient

from data_validation import consts, combiner, exceptions, metadata
from data_validation.config_manager import ConfigManager
from data_validation.validation_builder import ValidationBuilder

from third_party.ibis.ibis_impala.api import impala_connect

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
    "Impala": impala_connect,
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
        result_df = self._execute_validation(self.validation_builder)
        
        results_list = [] # [result_df]
        if self.config_manager.validation_type == "Row":
            for row in result_df.to_dict(orient="row"):
                if row["source_agg_value"] == row["target_agg_value"]:
                    continue
                elif not self.config_manager.primary_keys:
                    continue
                
                if self.verbose:
                    print(row)
                validation_builder = self._get_recursive_validation_builder(row)
                recurse_result_df = self._execute_validation(validation_builder)
                results_list.append(recurse_result_df)

        # Call Result Handler to Manage Results
        result_df = pandas.concat(results_list)
        return self.result_handler.execute(self.config, result_df)

    def _get_recursive_validation_builder(self, row):
        """ Return ValidationBuilder Configured for Next Recursive Search """
        validation_builder = ValidationBuilder(self.config_manager) # TODO should be self.validation.CLONE()
        validation_builder.add_config_query_groups(self.config_manager.primary_keys)
        
        # TODO: Group By is using the alias instead of source/target column name
        # TODO should add a separate filter field using Ibis native equals
        group_by_columns = json.loads(row["group_by_columns"])
        # group_by_columns_sql = " AND ".join([f"{key}='{value}'" for key,value in group_by_columns.items()])
        for alias, value in group_by_columns.items():
            filter_field = {
                consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,

                consts.CONFIG_FILTER_SOURCE_COLUMN: alias,
                consts.CONFIG_FILTER_SOURCE_VALUE: value,
                consts.CONFIG_FILTER_TARGET_COLUMN: alias,
                consts.CONFIG_FILTER_TARGET_VALUE: value,

                # consts.CONFIG_FILTER_SOURCE: group_by_columns_sql,
                # consts.CONFIG_FILTER_TARGET: group_by_columns_sql,
            }
            validation_builder.add_filter(filter_field)

        return validation_builder

    def _execute_validation(self, validation_builder):
        """ Execute Against a Supplied Validation Builder """
        run_metadata = metadata.RunMetadata()

        source_df = self.source_client.execute(
            validation_builder.get_source_query()
        )
        target_df = self.target_client.execute(
            validation_builder.get_target_query()
        )
        join_on_fields = validation_builder.get_group_aliases()
        pandas_client = ibis.pandas.connect(
            {combiner.DEFAULT_SOURCE: source_df, combiner.DEFAULT_TARGET: target_df}
        )

        run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)
        run_metadata.validations = validation_builder.get_metadata()
        result_df = combiner.generate_report(
            pandas_client, run_metadata, join_on_fields=join_on_fields
        )

        return result_df

    @staticmethod
    def get_data_client(connection_config):
        """ Return DataClient client from given configuration """
        connection_config = copy.deepcopy(connection_config)
        source_type = connection_config.pop(consts.SOURCE_TYPE)

        # The BigQueryClient expects a credentials object, not a string.
        if consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH in connection_config:
            key_path = connection_config.pop(consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH)
            if key_path:
                connection_config[
                    "credentials"
                ] = google.oauth2.service_account.Credentials.from_service_account_file(
                    key_path
                )

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
