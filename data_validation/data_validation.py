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

import pandas

from ibis.bigquery.client import BigQueryClient
from ibis.sql.mysql.client import MySQLClient
from ibis.sql.postgres.client import PostgreSQLClient

from data_validation import consts, exceptions

# If you have a Teradata License there is an optional teradatasql import
try:
    from data_validation.data_sources.teradata.client import TeradataClient
except Exception:
    TeradataClient = None

# TODO would it make sense to design the client to be registerable in some sense
# for easier extension?
# TODO for Airflow support we should wrap clients and create
# AIRFLOW_MYSQL which expects the connection name and uses airflow hooks
CLIENT_LOOKUP = {
    "BigQuery": BigQueryClient,
    "MySQL": MySQLClient,
    "Postgres": PostgreSQLClient,
    "Teradata": TeradataClient,
}


class ResultHandler(object):
    def execute(self, inp_config, out_config, result_df):
        print(result_df.to_string(index=False))

        return result_df


""" The DataValidation class is where the code becomes source/target aware

    The class builds specific source and target clients and is likely where someone would go to
    customize their validation process.

    data_validator = DataValidation(builder, source_config, target_config, result_handler=None, verbose=False)
"""


class DataValidation(object):
    def __init__(
        self, builder, source_config, target_config, result_handler=None, verbose=False
    ):
        """ Initialize a DataValidation client

            :param builder: A QueryBuilder client with the structure of the desired validation
            :param source_config: The source config used for the comparison
            :param target_config: The target config used for the comparison
            :param result_handler: A ResultHandler client to be used when storing results (default is print)
            :param verbose: If verbose, the Data Validation client will print queries run
        """
        self.verbose = verbose
        self.builder = builder

        # Data Client Management
        self.source_config = source_config
        self.target_config = target_config

        self.source_client = DataValidation.get_data_client(self.source_config)
        self.target_client = DataValidation.get_data_client(self.target_config)

        # Initialize the default Result Handler if None was supplied
        if result_handler is None:
            result_handler = ResultHandler()

        self.tables_list = self.build_tables_list()
        self.result_handler = result_handler

    @staticmethod
    def init_data_validation(builder, config, result_handler=None, verbose=False):
        """Return and initialized DataValidation object built from the config.

            :param builder: A QueryBuilder client with the structure of the desired validation
            :param config: The source and target config used for the comparison
            :param result_handler: A ResultHandler client to be used when storing results (default is print)
            :param verbose: If verbose, the Data Validation client will print queries run
        """
        # TODO: Improve the cleanup and section it out
        # TODO: Check for target [schema, table, column] lambda and apply
        source_config, target_config = DataValidation._build_config_details(config)

        return DataValidation(
            builder,
            source_config,
            target_config,
            result_handler=result_handler,
            verbose=verbose,
        )

    @staticmethod
    def _build_config_details(config):
        source_config = {consts.CONFIG: config.pop("source_config")}
        target_config = {consts.CONFIG: config.pop("target_config")}
        # Apply Source Types TODO: this can be built into the class
        source_config[consts.SOURCE_TYPE] = source_config[consts.CONFIG].pop(
            consts.SOURCE_TYPE
        )
        target_config[consts.SOURCE_TYPE] = target_config[consts.CONFIG].pop(
            consts.SOURCE_TYPE
        )
        # Clean all config attributes
        source_config.update(config)
        target_config.update(config)

        return source_config, target_config

    def build_tables_list(self):
        """ Return a List of Source and Target Tables to Validate """
        # TODO(dhercher): We should discuss a config design that makes sense
        tables_list = []

        # Lambda Funcations to convert source to target
        convert_schema = (lambda x: x)
        convert_table = (lambda x: x)

        # Handle Single Table Config Submission
        if consts.SCHEMA_NAME in self.source_config:
            table_obj = DataValidation._build_table_obj(
                            self.source_config[consts.SCHEMA_NAME],
                            self.source_config[consts.TABLE_NAME],
                            convert_schema, convert_table)
            tables_list.append(table_obj)

        if "tables_list" in self.source_config:
            for table_schema in self.source_config["tables_list"]:
                table_obj = DataValidation._build_table_obj(
                                ".".join(table_schema.split(".")[:-1]),
                                table_schema.split(".")[-1],
                                convert_schema, convert_table)
                tables_list.append(table_obj)

        return tables_list

    @staticmethod
    def _build_table_obj(schema_name, table_name, convert_schema, convert_table):
        """ Translate a given table into a table list dict """
        table_obj = {
            "source": {consts.SCHEMA_NAME: schema_name, consts.TABLE_NAME: table_name},
            "target": {consts.SCHEMA_NAME: convert_schema(schema_name),
                       consts.TABLE_NAME: convert_table(table_name)}}
        return table_obj

    @staticmethod
    def get_data_client(config):
        """ Return DataClient client from given configuration """
        source_type = config[consts.SOURCE_TYPE]

        if source_type not in CLIENT_LOOKUP:
            msg = 'ConfigurationError: Source type "{source_type}" is not supported'.format(
                source_type=source_type
            )
            raise Exception(msg)

        try:
            data_client = CLIENT_LOOKUP[source_type](**config[consts.CONFIG])
        except Exception:
            msg = 'Connection Type "{source_type}" could not connect'.format(
                source_type=source_type
            )
            raise exceptions.DataClientConnectionFailure(msg)

        return data_client

    def execute(self):
        """ Execute Queries and Store Results """
        results = []
        for table_obj in self.tables_list:
            source_df = self.execute_table(self.source_client,
                        table_obj["source"][consts.SCHEMA_NAME],
                        table_obj["source"][consts.TABLE_NAME],
                        partition_column=self.source_config.get(consts.PARTITION_COLUMN))
            target_df = self.execute_table(self.target_client,
                        table_obj["target"][consts.SCHEMA_NAME],
                        table_obj["target"][consts.TABLE_NAME],
                        partition_column=self.target_config.get(consts.PARTITION_COLUMN))

            df = self.combine_data(source_df, target_df)
            results.append(df)

        result_df = pandas.concat(results)

        # Call Result Handler to Manage Results
        return self.result_handler.execute(
            self.source_config, self.target_config, result_df
        )

    def execute_table(self, client, schema_name, table_name, partition_column=None):
        """ Execute validation on single table """
        # TODO(dhercher): This could possibly be a staticmethod for single table validation
        # this might be needed to validate trend data or recency
        query = self.builder.compile(client, schema_name, table_name,
                                     partition_column=partition_column)
        # Return Query Results in Dataframe from Ibis
        if self.verbose:
            print("-- ** Query ** --")
            print(query.compile())

        return client.execute(query)

    def combine_data(self, source_df, target_df):
        """ TODO: Return List of Dictionaries """
        # Clean Data to Standardize
        source_df = self._clean_raw_data(source_df)
        target_df = self._clean_raw_data(target_df)

        df = source_df.merge(
            target_df,
            how="outer",
            on=consts.DEFAULT_PARTITION_KEY,
            suffixes=(consts.INPUT_SUFFIX, consts.OUTPUT_SUFFIX),
        )
        return df

    def _clean_raw_data(self, result_df):
        """ TODO: Return Pandas DataFrame with standardized result data to be combined """
        # All data is joined via partition key
        if consts.DEFAULT_PARTITION_KEY not in result_df.columns:
            result_df[consts.DEFAULT_PARTITION_KEY] = consts.DEFAULT_PARTITION_KEY

        return result_df
