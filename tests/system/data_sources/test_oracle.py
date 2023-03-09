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

import os
from unittest import mock

from data_validation import cli_tools, data_validation, consts
from data_validation.__main__ import build_config_managers_from_args


ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DATABASE = os.getenv("ORACLE_DATABASE", "XEPDB1")

CONN = {
    "source_type": "Oracle",
    "host": ORACLE_HOST,
    "user": "SYSTEM",
    "password": ORACLE_PASSWORD,
    "port": 1521,
    "database": ORACLE_DATABASE,
}


ORACLE_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
    consts.CONFIG_TABLE_NAME: "items_price",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "price",
            consts.CONFIG_TARGET_COLUMN: "price",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}


def test_count_validator():
    validator = data_validation.DataValidation(ORACLE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    return_value=CONN,
)
def test_schema_validation_core_types(mock_conn):
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
        ]
    )
    config_managers = build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures only the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    return_value=CONN,
)
def test_column_validation_core_types(mock_conn):
    parser = cli_tools.configure_arg_parser()
    # TODO Add col_datetime,col_tstz to --sum string below when issue-XXX is complete. Or change whole string to * if YYY is also complete.
    # TODO Add col_dec_20,col_dec_38 to --sum string below when issue-YYY is complete. Or change whole string to * if XXX is also complete.
    # TODO Change --min string below to * when issue-YYY is complete.
    # TODO Change --max string below to * when issue-YYY is complete.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
        ]
    )
    config_managers = build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures only the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    return_value=CONN,
)
def test_row_validation_core_types(mock_conn):
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--concat=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
        ]
    )
    config_managers = build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures only the data frame should be empty
    assert len(df) == 0
