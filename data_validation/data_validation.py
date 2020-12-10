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
import warnings

import google.oauth2.service_account
import ibis.pandas
import pandas
import numpy

from data_validation import consts, combiner, exceptions, metadata, clients
from data_validation.config_manager import ConfigManager
from data_validation.validation_builder import ValidationBuilder

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
        if self.config_manager.validation_type == "Row":
            grouped_fields = self.validation_builder.pop_grouped_fields()
            result_df = self.execute_recursive_validation(
                self.validation_builder, grouped_fields
            )
        else:
            result_df = self._execute_validation(
                self.validation_builder, process_in_memory=True
            )

        # Call Result Handler to Manage Results
        return self.result_handler.execute(self.config, result_df)

    def execute_recursive_validation(self, validation_builder, grouped_fields):
        """ Recursive execution for Row validations.

        This method executes aggregate queries, such as sum-of-hashes, on the
        source and target tables. Where they differ, add to the GROUP BY
        clause recursively until the individual row differences can be
        identified.
        """
        process_in_memory = self.config_manager.process_in_memory()
        past_results = []

        if len(grouped_fields) > 0:
            validation_builder.add_query_group(grouped_fields[0])
            result_df = self._execute_validation(
                validation_builder, process_in_memory=process_in_memory
            )

            for row in result_df.to_dict(orient="row"):
                if row["source_agg_value"] == row["target_agg_value"]:
                    past_results.append(pandas.DataFrame([row]))
                    continue
                else:
                    recursive_validation_builder = validation_builder.clone()
                    self._add_recursive_validation_filter(
                        recursive_validation_builder, row
                    )
                    past_results.append(
                        self.execute_recursive_validation(
                            recursive_validation_builder, grouped_fields[1:]
                        )
                    )
        elif self.config_manager.primary_keys:
            validation_builder.add_config_query_groups(self.config_manager.primary_keys)
            past_results.append(
                self._execute_validation(
                    validation_builder, process_in_memory=process_in_memory
                )
            )
        else:
            warnings.warn(
                "WARNING: No Primary Keys Suppplied in Row Validation", UserWarning
            )
            return None

        return pandas.concat(past_results)

    def _add_recursive_validation_filter(self, validation_builder, row):
        """ Return ValidationBuilder Configured for Next Recursive Search """
        group_by_columns = json.loads(row["group_by_columns"])
        for alias, value in group_by_columns.items():
            filter_field = {
                consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,
                consts.CONFIG_FILTER_SOURCE_COLUMN: validation_builder.get_grouped_alias_source_column(
                    alias
                ),
                consts.CONFIG_FILTER_SOURCE_VALUE: value,
                consts.CONFIG_FILTER_TARGET_COLUMN: validation_builder.get_grouped_alias_target_column(
                    alias
                ),
                consts.CONFIG_FILTER_TARGET_VALUE: value,
            }
            validation_builder.add_filter(filter_field)

    def _execute_validation(self, validation_builder, process_in_memory=True):
        """ Execute Against a Supplied Validation Builder """
        run_metadata = metadata.RunMetadata()
        run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)
        run_metadata.validations = validation_builder.get_metadata()

        source_query = validation_builder.get_source_query()
        target_query = validation_builder.get_target_query()

        join_on_fields = validation_builder.get_group_aliases()

        if process_in_memory:
            source_df = self.source_client.execute(source_query)
            target_df = self.target_client.execute(target_query)
            pd_schema = source_df.dtypes[
                [
                    i
                    for i, v in source_df.dtypes.iteritems()
                    if v not in [numpy.dtype("O")]
                ]
            ]
            if self.verbose:
                print("-- ** Pandas Schema ** --")
                print(pd_schema)

            pandas_client = ibis.pandas.connect(
                {combiner.DEFAULT_SOURCE: source_df, combiner.DEFAULT_TARGET: target_df}
            )

            result_df = combiner.generate_report(
                pandas_client,
                run_metadata,
                pandas_client.table(combiner.DEFAULT_SOURCE, schema=pd_schema),
                pandas_client.table(combiner.DEFAULT_TARGET, schema=pd_schema),
                join_on_fields=join_on_fields,
                verbose=self.verbose,
            )
        else:
            result_df = combiner.generate_report(
                self.source_client,
                run_metadata,
                source_query,
                target_query,
                join_on_fields=join_on_fields,
                verbose=self.verbose,
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

        if source_type not in clients.CLIENT_LOOKUP:
            msg = 'ConfigurationError: Source type "{source_type}" is not supported'.format(
                source_type=source_type
            )
            raise Exception(msg)

        try:
            data_client = clients.CLIENT_LOOKUP[source_type](**connection_config)
        except Exception as e:
            msg = 'Connection Type "{source_type}" could not connect: {error}'.format(
                source_type=source_type, error=str(e)
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
