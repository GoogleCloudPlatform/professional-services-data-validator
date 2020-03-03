import os
import pandas

import ibis

from ibis.bigquery.client import BigQueryClient
from ibis.sql.mysql.client import MySQLClient

from data_validation import consts
from data_validation.query_builder import query_builder

CLIENT_LOOKUP = {
    "BigQuery": BigQueryClient,
    "MySQL": MySQLClient,
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

    def __init__(self, builder, source_config, target_config, result_handler=None, verbose=False):
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

        self.result_handler = result_handler

    @staticmethod
    def get_data_client(config):
        """ Return DataClient client from given configuration """
        source_type = config[consts.SOURCE_TYPE]

        if source_type not in CLIENT_LOOKUP:
            msg = 'ConfigurationError: Source type "{source_type}" is not supported'.format(source_type=source_type)
            raise Exception(msg)

        return CLIENT_LOOKUP[source_type](**config[consts.CONFIG])

    def execute(self):
        """ Execute Queries and Store Results """
        source_query = self.builder.compile(self.source_client,
                                self.source_config[consts.SCHEMA_NAME], self.source_config[consts.TABLE_NAME],
                                       partition_column=self.source_config.get(consts.PARTITION_COLUMN))
        target_query = self.builder.compile(self.target_client,
                                self.target_config[consts.SCHEMA_NAME], self.target_config[consts.TABLE_NAME],
                                partition_column=self.target_config.get(consts.PARTITION_COLUMN))

        # Return Query Results in Dataframe from Ibis
        if self.verbose:
            print("-- ** Input Query ** --")
            print(source_query.compile())
            print("-- ** Output Query ** --")
            print(target_query.compile())

        source_df = self.source_client.execute(source_query)
        target_df = self.target_client.execute(target_query)

        result_df = self.combine_data(source_df, target_df)

        # Call Result Handler to Manage Results
        return self.result_handler.execute(self.source_config, self.target_config, result_df)

    def combine_data(self, source_df, target_df):
        """ TODO: Return List of Dictionaries """
        # Clean Data to Standardize
        source_df = self._clean_raw_data(source_df)
        target_df = self._clean_raw_data(target_df)

        df = source_df.merge(target_df, how="outer", on=consts.DEFAULT_PARTITION_KEY,
                             suffixes=(consts.INPUT_SUFFIX, consts.OUTPUT_SUFFIX))
        return df

    def _clean_raw_data(self, result_df):
        """ TODO: Return Pandas DataFrame with standardized result data to be combined """
        # All data is joined via partition key
        if consts.DEFAULT_PARTITION_KEY not in result_df.columns:
            result_df[consts.DEFAULT_PARTITION_KEY] = consts.DEFAULT_PARTITION_KEY

        return result_df
