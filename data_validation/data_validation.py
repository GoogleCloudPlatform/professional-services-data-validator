import os
import pandas

from data_validation import consts
from data_validation.data_sources import data_client, example_client, bigquery
from data_validation.query_builder import query_builder

CLIENT_LOOKUP = {
    "BigQuery": bigquery.BigQueryClient,
    "Example": example_client.ExampleClient,
}
def process_data(builder, inp_config, out_config):

    inp_client = CLIENT_LOOKUP[inp_config[consts.SOURCE_TYPE]](**inp_config)
    out_client = CLIENT_LOOKUP[out_config[consts.SOURCE_TYPE]](**out_config)

    inp_query = builder.render_query(inp_client, inp_config[consts.SCHEMA_NAME], inp_config[consts.TABLE_NAME],
                                     partition_column=inp_config.get(consts.PARTITION_COLUMN),
                                     partition_column_type=inp_config.get(consts.PARTITION_COLUMN_TYPE))
    out_query = builder.render_query(out_client, out_config[consts.SCHEMA_NAME], out_config[consts.TABLE_NAME],
                                     partition_column=inp_config.get(consts.PARTITION_COLUMN),
                                     partition_column_type=inp_config.get(consts.PARTITION_COLUMN_TYPE))

    inp_results = inp_client.read_sql(inp_query)
    out_results = out_client.read_sql(out_query)

    df = combine_data(inp_results, out_results)
    store_results(df)

def combine_data(inp_results, out_results):
    """ Return List of Dictionaries """
    # Clean Data to Standardize
    inp_df = _clean_raw_data(inp_results)
    out_df = _clean_raw_data(out_results)

    df = inp_df.merge(out_df, how="outer", on=consts.DEFAULT_PARTITION_KEY,
                      suffixes=(consts.INPUT_SUFFIX, consts.OUTPUT_SUFFIX))
    return df

def _clean_raw_data(results):
    """ Return Pandas DataFrame with standardized result data to be combined """
    df_results = pandas.DataFrame(results)

    # All data is joined via partition key
    if consts.DEFAULT_PARTITION_KEY not in df_results.columns:
        df_results[consts.DEFAULT_PARTITION_KEY] = consts.DEFAULT_PARTITION_KEY

    return df_results

def store_results(df):
    """ Store Results of Data Validation """
    print(df.to_string(index=False))
