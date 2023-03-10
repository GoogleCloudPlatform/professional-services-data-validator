# Copyright 2023 Google LLC
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

import os

from data_validation import data_validation, consts, exceptions


# TODO: To use this code I would need to use the Cloud SQL Proxy.
#       https://cloud.google.com/sql/docs/mysql/quickstart-proxy-test

CONN = {
    "source_type": "MySQL",
    "host": os.getenv("MYSQL_HOST"),
    "user": "root",
    "password": os.getenv("MYSQL_PASSWORD"),
    "port": 3306,
    "database": "guestbook",
    "driver": "pymysql",
}
CONFIG_COUNT_VALID = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "guestbook",
    consts.CONFIG_TABLE_NAME: "entries",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: None,
            consts.CONFIG_TARGET_COLUMN: None,
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}

CONFIG_SCHEMA_VALID = {
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    consts.CONFIG_TYPE: "Column",
    consts.CONFIG_SCHEMA_NAME: "guestbook",
    consts.CONFIG_TABLE_NAME: "entries",
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}


def test_mysql_count_invalid_host():
    try:
        data_validator = data_validation.DataValidation(
            CONFIG_COUNT_VALID,
            verbose=False,
        )
        df = data_validator.execute()
        assert df["source_agg_value"][0] == df["target_agg_value"][0]
    except exceptions.DataClientConnectionFailure:
        # Local Testing will not work for MySQL
        pass


def test_schema_validation():
    try:
        data_validator = data_validation.DataValidation(
            CONFIG_SCHEMA_VALID,
            verbose=False,
        )
        df = data_validator.execute()

        for validation in df.to_dict(orient="records"):
            assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS
    except exceptions.DataClientConnectionFailure:
        # Local Testing will not work for MySQL
        pass


def test_mssql_row():
    """Test row validaiton on mssql"""
    try:
        config_row_valid = {
            consts.CONFIG_SOURCE_CONN: CONN,
            consts.CONFIG_TARGET_CONN: CONN,
            # Validation Type
            consts.CONFIG_TYPE: "Row",
            # Configuration Required Depending on Validator Type
            consts.CONFIG_SCHEMA_NAME: "guestbook",
            consts.CONFIG_TABLE_NAME: "test_data_types_mysql_row",
            consts.CONFIG_COMPARISON_FIELDS: [
                {
                    "source_column": "hash__all",
                    "target_column": "hash__all",
                    "field_alias": "hash__all",
                    "cast": None,
                }
            ],
            consts.CONFIG_CALCULATED_FIELDS: [
                {
                    "source_calculated_columns": ["serial_col"],
                    "target_calculated_columns": ["serial_col"],
                    "field_alias": "cast__serial_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["int_col"],
                    "target_calculated_columns": ["int_col"],
                    "field_alias": "cast__int_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["text_col"],
                    "target_calculated_columns": ["text_col"],
                    "field_alias": "cast__text_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["char_col"],
                    "target_calculated_columns": ["char_col"],
                    "field_alias": "cast__char_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["varchar_col"],
                    "target_calculated_columns": ["varchar_col"],
                    "field_alias": "cast__varchar_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["float_col"],
                    "target_calculated_columns": ["float_col"],
                    "field_alias": "cast__float_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["decimal_col"],
                    "target_calculated_columns": ["decimal_col"],
                    "field_alias": "cast__decimal_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["datetime_col"],
                    "target_calculated_columns": ["datetime_col"],
                    "field_alias": "cast__datetime_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["date_col"],
                    "target_calculated_columns": ["date_col"],
                    "field_alias": "cast__date_col",
                    "type": "cast",
                    "depth": 0,
                },
                {
                    "source_calculated_columns": ["cast__serial_col"],
                    "target_calculated_columns": ["cast__serial_col"],
                    "field_alias": "ifnull__cast__serial_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__int_col"],
                    "target_calculated_columns": ["cast__int_col"],
                    "field_alias": "ifnull__cast__int_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__text_col"],
                    "target_calculated_columns": ["cast__text_col"],
                    "field_alias": "ifnull__cast__text_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__char_col"],
                    "target_calculated_columns": ["cast__char_col"],
                    "field_alias": "ifnull__cast__char_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__varchar_col"],
                    "target_calculated_columns": ["cast__varchar_col"],
                    "field_alias": "ifnull__cast__varchar_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__float_col"],
                    "target_calculated_columns": ["cast__float_col"],
                    "field_alias": "ifnull__cast__float_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__decimal_col"],
                    "target_calculated_columns": ["cast__decimal_col"],
                    "field_alias": "ifnull__cast__decimal_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__datetime_col"],
                    "target_calculated_columns": ["cast__datetime_col"],
                    "field_alias": "ifnull__cast__datetime_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["cast__date_col"],
                    "target_calculated_columns": ["cast__date_col"],
                    "field_alias": "ifnull__cast__date_col",
                    "type": "ifnull",
                    "depth": 1,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__serial_col"],
                    "target_calculated_columns": ["ifnull__cast__serial_col"],
                    "field_alias": "rstrip__ifnull__cast__serial_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__int_col"],
                    "target_calculated_columns": ["ifnull__cast__int_col"],
                    "field_alias": "rstrip__ifnull__cast__int_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__text_col"],
                    "target_calculated_columns": ["ifnull__cast__text_col"],
                    "field_alias": "rstrip__ifnull__cast__text_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__char_col"],
                    "target_calculated_columns": ["ifnull__cast__char_col"],
                    "field_alias": "rstrip__ifnull__cast__char_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__varchar_col"],
                    "target_calculated_columns": ["ifnull__cast__varchar_col"],
                    "field_alias": "rstrip__ifnull__cast__varchar_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__float_col"],
                    "target_calculated_columns": ["ifnull__cast__float_col"],
                    "field_alias": "rstrip__ifnull__cast__float_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__decimal_col"],
                    "target_calculated_columns": ["ifnull__cast__decimal_col"],
                    "field_alias": "rstrip__ifnull__cast__decimal_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__datetime_col"],
                    "target_calculated_columns": ["ifnull__cast__datetime_col"],
                    "field_alias": "rstrip__ifnull__cast__datetime_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["ifnull__cast__date_col"],
                    "target_calculated_columns": ["ifnull__cast__date_col"],
                    "field_alias": "rstrip__ifnull__cast__date_col",
                    "type": "rstrip",
                    "depth": 2,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__serial_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__int_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__text_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__char_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__varchar_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__float_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__decimal_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__decimal_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__decimal_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__datetime_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__datetime_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__datetime_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                    "target_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                    "field_alias": "upper__rstrip__ifnull__cast__date_col",
                    "type": "upper",
                    "depth": 3,
                },
                {
                    "source_calculated_columns": [
                        "upper__rstrip__ifnull__cast__serial_col",
                        "upper__rstrip__ifnull__cast__int_col",
                        "upper__rstrip__ifnull__cast__text_col",
                        "upper__rstrip__ifnull__cast__char_col",
                        "upper__rstrip__ifnull__cast__varchar_col",
                        "upper__rstrip__ifnull__cast__float_col",
                        "upper__rstrip__ifnull__cast__decimal_col",
                        "upper__rstrip__ifnull__cast__datetime_col",
                        "upper__rstrip__ifnull__cast__date_col",
                    ],
                    "target_calculated_columns": [
                        "upper__rstrip__ifnull__cast__serial_col",
                        "upper__rstrip__ifnull__cast__int_col",
                        "upper__rstrip__ifnull__cast__text_col",
                        "upper__rstrip__ifnull__cast__char_col",
                        "upper__rstrip__ifnull__cast__varchar_col",
                        "upper__rstrip__ifnull__cast__float_col",
                        "upper__rstrip__ifnull__cast__decimal_col",
                        "upper__rstrip__ifnull__cast__datetime_col",
                        "upper__rstrip__ifnull__cast__date_col",
                    ],
                    "field_alias": "concat__all",
                    "type": "concat",
                    "depth": 4,
                },
                {
                    "source_calculated_columns": ["concat__all"],
                    "target_calculated_columns": ["concat__all"],
                    "field_alias": "hash__all",
                    "type": "hash",
                    "depth": 5,
                },
            ],
            consts.CONFIG_PRIMARY_KEYS: [
                {
                    "source_column": "serial_col",
                    "target_column": "serial_col",
                    "field_alias": "serial_col",
                    "cast": None,
                }
            ],
            consts.CONFIG_FORMAT: "table",
            consts.CONFIG_FILTER_STATUS: None,
            consts.CONFIG_RANDOM_ROW_BATCH_SIZE: "5",
            consts.CONFIG_USE_RANDOM_ROWS: True,
        }

        data_validator = data_validation.DataValidation(
            config_row_valid,
            verbose=False,
        )
        df = data_validator.execute()

        assert df["source_agg_value"][0] == df["target_agg_value"][0]
    except exceptions.DataClientConnectionFailure:
        # Local Testing will not work for MySQL
        pass
