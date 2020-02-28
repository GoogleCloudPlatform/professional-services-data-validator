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
def process_data(builder, inp_config, out_config, verbose=False):
    inp_client = CLIENT_LOOKUP[inp_config[consts.SOURCE_TYPE]](**inp_config[consts.CONFIG])
    out_client = CLIENT_LOOKUP[out_config[consts.SOURCE_TYPE]](**out_config[consts.CONFIG])

    inp_query = builder.compile(inp_client, inp_config[consts.SCHEMA_NAME], inp_config[consts.TABLE_NAME],
                                     partition_column=inp_config.get(consts.PARTITION_COLUMN))
    out_query = builder.compile(out_client, out_config[consts.SCHEMA_NAME], out_config[consts.TABLE_NAME],
                                     partition_column=inp_config.get(consts.PARTITION_COLUMN))

    # Return Query Results in Dataframe from Ibis
    if verbose:
        print("-- ** Input Query ** --")
        print(inp_query.compile())
        print("-- ** Output Query ** --")
        print(out_query.compile())

    inp_df = inp_client.execute(inp_query)
    out_df = out_client.execute(out_query)

    df = combine_data(inp_df, out_df)
    store_results(df)

def combine_data(inp_df, out_df):
    """ Return List of Dictionaries """
    # Clean Data to Standardize
    inp_df = _clean_raw_data(inp_df)
    out_df = _clean_raw_data(out_df)

    df = inp_df.merge(out_df, how="outer", on=consts.DEFAULT_PARTITION_KEY,
                      suffixes=(consts.INPUT_SUFFIX, consts.OUTPUT_SUFFIX))
    return df

def _clean_raw_data(df_results):
    """ Return Pandas DataFrame with standardized result data to be combined """
    # All data is joined via partition key
    if consts.DEFAULT_PARTITION_KEY not in df_results.columns:
        df_results[consts.DEFAULT_PARTITION_KEY] = consts.DEFAULT_PARTITION_KEY

    return df_results

def store_results(df):
    """ Store Results of Data Validation """
    print(df.to_string(index=False))
