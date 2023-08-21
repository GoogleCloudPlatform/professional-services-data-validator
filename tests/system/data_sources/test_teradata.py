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
from data_validation.partition_builder import PartitionBuilder
from tests.system.data_sources.common_functions import null_not_null_assertions
from tests.system.data_sources.test_bigquery import BQ_CONN


TERADATA_USER = os.getenv("TERADATA_USER", "udf")
TERADATA_PASSWORD = os.getenv("TERADATA_PASSWORD")
TERADATA_HOST = os.getenv("TERADATA_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")

CONN = {
    "source_type": "Teradata",
    "host": TERADATA_HOST,
    "user_name": TERADATA_USER,
    "password": TERADATA_PASSWORD,
    "port": 1025,
}


TERADATA_COLUMN_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TABLE_NAME: "CALENDAR",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "year_of_calendar",
            consts.CONFIG_TARGET_COLUMN: "year_of_calendar",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
    consts.CONFIG_FILTERS: [
        {
            consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,
            consts.CONFIG_FILTER_SOURCE_COLUMN: "year_of_calendar",
            consts.CONFIG_FILTER_SOURCE_VALUE: 2010,
            consts.CONFIG_FILTER_TARGET_COLUMN: "year_of_calendar",
            consts.CONFIG_FILTER_TARGET_VALUE: 2010,
        },
    ],
}

TERADATA_ROW_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Row",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TABLE_NAME: "CALENDAR",
    consts.CONFIG_TARGET_SCHEMA_NAME: "Sys_Calendar",
    consts.CONFIG_TARGET_TABLE_NAME: "CALENDAR",
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
    consts.CONFIG_FILTERS: [],
    consts.CONFIG_USE_RANDOM_ROWS: False,
    consts.CONFIG_COMPARISON_FIELDS: [
        {
            consts.CONFIG_SOURCE_COLUMN: "concat__all",
            consts.CONFIG_TARGET_COLUMN: "concat__all",
            consts.CONFIG_FIELD_ALIAS: "concat__all",
            consts.CONFIG_CAST: None,
        }
    ],
    consts.CONFIG_CALCULATED_FIELDS: [
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "cast__calendar_date",
            consts.CONFIG_TYPE: "custom",
            consts.CONFIG_DEPTH: 0,
            consts.CONFIG_CUSTOM_IBIS_EXPR: "ibis.expr.types.TemporalValue.strftime",
            consts.CONFIG_CUSTOM_PARAMS: [{"format_str": "%Y-%m-%d"}],
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "cast__day_of_week",
            consts.CONFIG_TYPE: "cast",
            consts.CONFIG_DEPTH: 0,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["cast__calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["cast__calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: "ifnull",
            consts.CONFIG_DEPTH: 1,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["cast__day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["cast__day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: "ifnull",
            consts.CONFIG_DEPTH: 1,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["ifnull__cast__calendar_date"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["ifnull__cast__calendar_date"],
            consts.CONFIG_FIELD_ALIAS: "rstrip__ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: "rstrip",
            consts.CONFIG_DEPTH: 2,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: ["ifnull__cast__day_of_week"],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: ["ifnull__cast__day_of_week"],
            consts.CONFIG_FIELD_ALIAS: "rstrip__ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: "rstrip",
            consts.CONFIG_DEPTH: 2,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [
                "rstrip__ifnull__cast__calendar_date"
            ],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [
                "rstrip__ifnull__cast__calendar_date"
            ],
            consts.CONFIG_FIELD_ALIAS: "upper__rstrip__ifnull__cast__calendar_date",
            consts.CONFIG_TYPE: "upper",
            consts.CONFIG_DEPTH: 3,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [
                "rstrip__ifnull__cast__day_of_week"
            ],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [
                "rstrip__ifnull__cast__day_of_week"
            ],
            consts.CONFIG_FIELD_ALIAS: "upper__rstrip__ifnull__cast__day_of_week",
            consts.CONFIG_TYPE: "upper",
            consts.CONFIG_DEPTH: 3,
        },
        {
            consts.CONFIG_CALCULATED_SOURCE_COLUMNS: [
                "upper__rstrip__ifnull__cast__calendar_date",
                "upper__rstrip__ifnull__cast__day_of_week",
            ],
            consts.CONFIG_CALCULATED_TARGET_COLUMNS: [
                "upper__rstrip__ifnull__cast__calendar_date",
                "upper__rstrip__ifnull__cast__day_of_week",
            ],
            consts.CONFIG_FIELD_ALIAS: "concat__all",
            consts.CONFIG_TYPE: "concat",
            consts.CONFIG_DEPTH: 4,
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_SOURCE_COLUMN: "calendar_date",
            consts.CONFIG_TARGET_COLUMN: "calendar_date",
            consts.CONFIG_FIELD_ALIAS: "calendar_date",
            consts.CONFIG_CAST: None,
        }
    ],
}


def test_count_validator():
    validator = data_validation.DataValidation(TERADATA_COLUMN_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def test_row_validator():
    validator = data_validation.DataValidation(TERADATA_ROW_CONFIG, verbose=True)
    df = validator.execute()
    assert df["validation_status"][0] == "success"


def mock_get_connection_config(*args):
    if args[1] in ("td-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=udf.dvt_core_types",
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
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--exclusion-columns=id",
            (
                # Teradata integrals go to BigQuery INT64.
                "--allow-list=int8:int64,int16:int64,int32:int64,"
                # Teradata NUMBERS that map to BigQuery NUMERIC.
                "decimal(20,0):decimal(38,9),decimal(10,2):decimal(38,9),"
                # Teradata NUMBERS that map to BigQuery BIGNUMERIC.
                "decimal(38,0):decimal(76,38)"
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
def test_schema_validation_not_null_vs_nullable():
    """Compares a source table with a BigQuery target and ensure we match/fail on nnot null/nullable correctly."""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_null_not_null=pso_data_validator.dvt_null_not_null",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    null_not_null_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=udf.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_datetime,col_tstz",
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
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38,col_dec_10_2,col_datetime",
            "--min=col_int8,col_int16,col_int32,col_int64,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38,col_dec_10_2,col_datetime",
            "--max=col_int8,col_int16,col_int32,col_int64,col_float32,col_float64,col_varchar_30,col_char_2,col_string,col_date,col_dec_20,col_dec_38,col_dec_10_2,col_datetime",
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
    parser = cli_tools.configure_arg_parser()
    # Excluded col_string because LONG VARCHAR column causes exception regardless of column contents:
    # [Error 3798] A column or character expression is larger than the max size.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=udf.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    "course_id < 'ALG001' OR course_id = 'ALG001' AND (quarter_id < 3 OR quarter_id = 3 AND (student_id < 1234))",
    "(course_id > 'ALG001' OR course_id = 'ALG001' AND (quarter_id > 3 OR quarter_id = 3 AND (student_id >= 1234)))"
    + " AND (course_id < 'GEO001' OR course_id = 'GEO001' AND (quarter_id < 2 OR quarter_id = 2 AND (student_id < 5678)))",
    "(course_id > 'GEO001' OR course_id = 'GEO001' AND (quarter_id > 2 OR quarter_id = 2 AND (student_id >= 5678)))"
    + " AND (course_id < 'TRI001' OR course_id = 'TRI001' AND (quarter_id < 1 OR quarter_id = 1 AND (student_id < 9012)))",
    "course_id > 'TRI001' OR course_id = 'TRI001' AND (quarter_id > 1 OR quarter_id = 1 AND (student_id >= 9012))",
]


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_teradata_generate_table_partitions():
    """Test generate table partitions on BigQuery
    The unit tests, specifically test_add_partition_filters_to_config and test_store_yaml_partitions_local
    check that yaml configurations are created and saved in local storage. Partitions can only be created with
    a database that can handle SQL with ntile, hence doing this as part of system testing.
    What we are checking
    1. the shape of the partition list is 1, number of partitions (only one table in the list)
    2. value of the partition list matches what we expect.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "generate-table-partitions",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=udf.test_generate_partitions=udf.test_generate_partitions",
            "-pk=course_id,quarter_id,student_id",
            "-hash=*",
            "-cdir=/home/users/yaml",
            "-pn=4",
        ]
    )
    config_managers = main.build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()

    assert len(partition_filters) == 1  # only one pair of tables
    assert (
        len(partition_filters[0]) == partition_builder.args.partition_num
    )  # assume no of table rows > partition_num
    assert partition_filters[0] == EXPECTED_PARTITION_FILTER


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    # Excluded col_string because LONG VARCHAR column causes exception regardless of column contents:
    # [Error 3798] A column or character expression is larger than the max size.
    # TODO Change --hash option to include col_tstz when issue-929 is complete.
    # TODO Change --hash option to include col_float32,col_float64 when issue-841 is complete.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=td-conn",
            "-tc=bq-conn",
            "-tbls=udf.dvt_core_types=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_varchar_30,col_char_2,col_date,col_datetime",
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
def test_custom_query_validation_core_types():
    """Teradata to Teradata dvt_core_types custom-query validation"""
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "custom-query",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "--source-query=select * from udf.dvt_core_types",
            "--target-query=select * from udf.dvt_core_types",
            "--filter-status=fail",
            "--count=*",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0
