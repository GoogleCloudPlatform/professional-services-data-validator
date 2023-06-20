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

from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts
from tests.system.data_sources.test_bigquery import BQ_CONN


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


def mock_get_connection_config(*args):
    if args[1] in ("ora-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Oracle to Oracle dvt_core_types schema validation"""
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
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Oracle to BigQuery dvt_core_types schema validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=ora-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            (
                # Integral Oracle NUMBERS go to BigQuery INT64.
                "--allow-list=!decimal(8,0):int64,decimal(2,0):int64,decimal(4,0):int64,decimal(9,0):int64,decimal(18,0):int64,"
                # Oracle NUMBERS that map to BigQuery NUMERIC.
                "decimal(20,0):decimal(38,9),decimal(10,2):decimal(38,9),"
                # Oracle NUMBERS that map to BigQuery BIGNUMERIC.
                "decimal(38,0):decimal(76,38),"
                # BigQuery does not have a float32 type.
                "float32:float64"
            ),
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Oracle to Oracle dvt_core_types column validation"""
    parser = cli_tools.configure_arg_parser()
    # TODO Change --sum string below to * when issue-762 is complete.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38",
            "--min=*",
            "--max=*",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    # TODO Change --sum string below to include col_datetime and col_tstz when issue-762 is complete.
    # TODO Change --min/max strings below to include col_tstz when issue-706 is complete.
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=ora-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Oracle to Oracle dvt_core_types row validation"""
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
            "--hash=*",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    # TODO Change --hash string below to include col_tstz when issue-706 is complete.
    # TODO Change --hash string below to include col_float32,col_float64 when issue-841 is complete.
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=ora-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_varchar_30,col_char_2,col_string,col_date,col_datetime",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0
