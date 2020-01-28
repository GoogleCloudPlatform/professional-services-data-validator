# import unittest

# from nose.tools import assert_equals, assert_true

from data_validation import data_validation
from data_validation.data_sources import data_client, example_client, bigquery
from data_validation.query_builder import query_builder

import os

# class TestHelper(unittest.TestCase):

#     # def setUp(self):
        

#     def test_(self):
#         state_id = helper.generate_state_id(8)

#         assert_true(len(state_id)==8)

def test_abstract_client():
    e = None
    try:
        client = data_client.DataClient()
    except Exception as e:
        pass
    # assert_true(e is not None)

def test_example_client_count_validator():
    client = example_client.ExampleClient()
    builder = query_builder.QueryBuilder.build_count_validator()

    query = builder.render_query(client, "schema_name", "table_name", partition_column=None, partition_column_type=None)
    print(query)


# def test_example_client_count_validator():
inp_config = {"source_type": "BigQuery",
              "project_id": os.environ["PROJECT_ID"],
              "schema_name": "bigquery-public-data.new_york_citibike",
              "table_name": "citibike_trips"}
out_config = {"source_type": "BigQuery",
              "project_id": os.environ["PROJECT_ID"],
              "schema_name": "bigquery-public-data.new_york_citibike",
              "table_name": "citibike_trips"}

builder = query_builder.QueryBuilder.build_count_validator()
data_validation.process_data(builder, inp_config, out_config)


