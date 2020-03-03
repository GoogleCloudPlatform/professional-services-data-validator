
# Ignore Warning regarding User Auth in Google
import warnings
warnings.simplefilter("ignore", UserWarning)

# import unittest
# from nose.tools import assert_equals, assert_true

import os

from data_validation import data_validation, consts
from data_validation.query_builder import query_builder

BQ_CONFIG_VALID = {
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "config": {"project_id": os.environ["PROJECT_ID"]},

    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    consts.PARTITION_COLUMN: "starttime",
}

# TODO: To use this code I would need to whitelist the MySQL instance
MYSQL_CONFIG_INVALID = {
    # Configuration Required for All Data Soures
    "source_type": "MySQL",

    # BigQuery Specific Connection Config
    "config": {"host": "35.227.139.75", "user": "root", "password": "password", "port": 3306, "database": 'guestbook', "driver": 'pymysql'},

    # Configuration Required Depending on Validator Type
    "schema_name": "guestbook",
    "table_name": "entries",
    consts.PARTITION_COLUMN: "starttime",
}

class TestHelper(unittest.TestCase):
    def setUp(self):
        self.is_setup = True

    def test_count_validator(self):
        print("****** BQ Simple Count Comparison ********")
        builder = query_builder.QueryBuilder.build_count_validator()
        data_validator = data_validation.DataValidation(builder, BQ_CONFIG_VALID, BQ_CONFIG_VALID,
                                                        result_handler=None, verbose=False)
        df = data_validator.execute()
        assert_true(True)

    def test_partitioned_count_validator(self):
        print("****** BQ Partitioned Count Comparison ********")
        builder = query_builder.QueryBuilder.build_partition_count_validator(days_past=700, limit=10)
        data_validator = data_validation.DataValidation(builder, BQ_CONFIG_VALID, BQ_CONFIG_VALID,
                                                        result_handler=None, verbose=False)
        df = data_validator.execute()
        assert_true(True)

    def test_mysql_count_invalid_host(self):
        print("****** BQ Simple Count Comparison ********")
        builder = query_builder.QueryBuilder.build_count_validator()
        data_validator = data_validation.DataValidation(builder, MYSQL_CONFIG_INVALID, MYSQL_CONFIG_INVALID,
                                                        result_handler=None, verbose=False)
        df = data_validator.execute()
        assert_true(True)



# import os
# from data_validation import data_validation, consts
# from data_validation.query_builder import query_builder

# source_config = {
#     # Configuration Required for All Data Soures
#     "source_type": "BigQuery",

#     # BigQuery Specific Connection Config
#     "config": {"project_id": os.environ["PROJECT_ID"]},

#     # Configuration Required Depending on Validator Type
#     "schema_name": "bigquery-public-data.new_york_citibike",
#     "table_name": "citibike_trips",
#     consts.PARTITION_COLUMN: "starttime",
# }
# target_config = {
#     # Configuration Required for All Data Soures
#     "source_type": "BigQuery",

#     # BigQuery Specific Connection Config
#     "config": {"project_id": os.environ["PROJECT_ID"]},

#     # Configuration Required Depending on Validator Type
#     "schema_name": "bigquery-public-data.new_york_citibike",
#     "table_name": "citibike_trips",
#     consts.PARTITION_COLUMN: "starttime",
# }

# print("****** BQ Simple Count Comparison ********")
# builder = query_builder.QueryBuilder.build_count_validator()
# data_validator = data_validation.DataValidation(builder, source_config, target_config,
#                                                 result_handler=None, verbose=False)
# data_validator.execute()

# print("****** BQ Partitioned Count Comparison ********")
# builder = query_builder.QueryBuilder.build_partition_count_validator(days_past=700, limit=10)
# data_validator = data_validation.DataValidation(builder, source_config, target_config,
#                                                 result_handler=None, verbose=False)
# data_validator.execute()
