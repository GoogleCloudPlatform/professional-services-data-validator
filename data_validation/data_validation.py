import os

from data_validation.data_sources import data_client, example_client, bigquery
from data_validation.query_builder import query_builder

CLIENT_LOOKUP = {
    "BigQuery": bigquery.BigQueryClient,
    "Example": example_client.ExampleClient,
}
def process_data(builder, inp_config, out_config):

    inp_client = CLIENT_LOOKUP[inp_config["source_type"]](**inp_config)
    out_client = CLIENT_LOOKUP[out_config["source_type"]](**out_config)

    inp_query = builder.render_query(inp_client, inp_config["schema_name"], inp_config["table_name"],
                                     partition_column=None, partition_column_type=None)
    out_query = builder.render_query(out_client, out_config["schema_name"], out_config["table_name"],
                                     partition_column=None, partition_column_type=None)

    inp_results = inp_client.read_sql(inp_query)
    out_results = out_client.read_sql(out_query)

    print("Input Results: {}".format(inp_results))
    print("Output Results: {}".format(out_results))
