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

import datetime
import json
import os
import pathlib
from unittest import mock

import pytest

from data_validation import cli_tools, consts, data_validation, find_tables
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    binary_key_assertions,
    column_validation_test,
    connections_add_test,
    custom_query_validation_test,
    find_tables_test,
    generate_and_run_table_partitions_test,
    raw_query_test,
    row_validation_many_columns_test,
    row_validation_test,
    run_test_from_cli_args,
    schema_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN

PROJECT_ID = os.environ["PROJECT_ID"]
SPANNER_INSTANCE = os.getenv("SPANNER_INSTANCE", "span1")
SPANNER_DATABASE = os.getenv("SPANNER_DATABASE", "pso_data_validator")
SPANNER_CONN_NAME = "spanner-integration-test"
CLI_FIND_TABLES_ARGS = [
    "find-tables",
    "--source-conn",
    "mock-conn",
    "--target-conn",
    "mock-conn",
]

SPANNER_CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_SPANNER,
    "project_id": PROJECT_ID,
    "instance_id": SPANNER_INSTANCE,
    "database_id": SPANNER_DATABASE,
}


def mock_get_connection_config(*args):
    if args[1] in ("spanner-conn", "mock-conn"):
        return SPANNER_CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@pytest.fixture
def count_config():
    return {
        # Connection Name
        consts.CONFIG_SOURCE_CONN: SPANNER_CONN,
        consts.CONFIG_TARGET_CONN: SPANNER_CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
        consts.CONFIG_TABLE_NAME: "functional_alltypes",
        consts.CONFIG_GROUPED_COLUMNS: [],
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: consts.CONFIG_TYPE_COUNT,
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: consts.CONFIG_TYPE_COUNT,
                consts.CONFIG_SOURCE_COLUMN: "string_col",
                consts.CONFIG_TARGET_COLUMN: "string_col",
                consts.CONFIG_FIELD_ALIAS: "count_string_col",
            },
            {
                consts.CONFIG_TYPE: consts.CONFIG_TYPE_AVG,
                consts.CONFIG_SOURCE_COLUMN: "float_col",
                consts.CONFIG_TARGET_COLUMN: "float_col",
                consts.CONFIG_FIELD_ALIAS: "avg_float_col",
            },
            {
                consts.CONFIG_TYPE: consts.CONFIG_TYPE_MAX,
                consts.CONFIG_SOURCE_COLUMN: "timestamp_col",
                consts.CONFIG_TARGET_COLUMN: "timestamp_col",
                consts.CONFIG_FIELD_ALIAS: "max_timestamp_col",
            },
            {
                consts.CONFIG_TYPE: consts.CONFIG_TYPE_MIN,
                consts.CONFIG_SOURCE_COLUMN: "int_col",
                consts.CONFIG_TARGET_COLUMN: "int_col",
                consts.CONFIG_FIELD_ALIAS: "min_int_col",
            },
        ],
        consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
        consts.CONFIG_FILTER_STATUS: None,
    }


@pytest.fixture
def grouped_config():
    return {
        # Connection Name
        consts.CONFIG_SOURCE_CONN: SPANNER_CONN,
        consts.CONFIG_TARGET_CONN: SPANNER_CONN,
        # Validation Type
        consts.CONFIG_TYPE: "GroupedColumn",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "pso_data_validator",
        consts.CONFIG_TABLE_NAME: "functional_alltypes",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: "sum",
                consts.CONFIG_SOURCE_COLUMN: "float_col",
                consts.CONFIG_TARGET_COLUMN: "float_col",
                consts.CONFIG_FIELD_ALIAS: "sum_float_col",
            },
        ],
        consts.CONFIG_GROUPED_COLUMNS: [
            {
                consts.CONFIG_FIELD_ALIAS: "timestamp_col",
                consts.CONFIG_SOURCE_COLUMN: "timestamp_col",
                consts.CONFIG_TARGET_COLUMN: "timestamp_col",
                consts.CONFIG_CAST: "date",
            },
        ],
        consts.CONFIG_FORMAT: consts.FORMAT_TYPE_TABLE,
        consts.CONFIG_FILTER_STATUS: None,
    }


CLI_CONFIG_FILE = "example_test.yaml"
CLI_RUN_CONFIG_ARGS = ["run-config", "--config-file", CLI_CONFIG_FILE]


STRING_MATCH_RESULT = '{"schema_name": "pso_data_validator", "table_name": "results", "target_schema_name": "pso_data_validator", "target_table_name": "results"}'


def test_count_validator(count_config):
    validator = data_validation.DataValidation(count_config, verbose=True)
    df = validator.execute()

    count_value = df[df[consts.VALIDATION_NAME] == "count"]["source_agg_value"].values[
        0
    ]
    count_string_value = df[df[consts.VALIDATION_NAME] == "count_string_col"][
        "source_agg_value"
    ].values[0]
    avg_float_value = df[df[consts.VALIDATION_NAME] == "avg_float_col"][
        "source_agg_value"
    ].values[0]
    max_timestamp_value = df[df[consts.VALIDATION_NAME] == "max_timestamp_col"][
        "source_agg_value"
    ].values[0]
    min_int_value = df[df[consts.VALIDATION_NAME] == "min_int_col"][
        "source_agg_value"
    ].values[0]

    assert float(count_value) > 0
    assert float(count_string_value) > 0
    assert float(avg_float_value) > 0
    assert datetime.datetime.strptime(
        max_timestamp_value,
        "%Y-%m-%d %H:%M:%S%z",
    ) > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    assert float(min_int_value) > 0


def test_grouped_count_validator(grouped_config):
    validator = data_validation.DataValidation(grouped_config, verbose=True)
    df = validator.execute()
    rows = list(df[df[consts.VALIDATION_NAME] == "count"].iterrows())

    # Check that all partitions are unique.
    partitions = frozenset(df[consts.GROUP_BY_COLUMNS])
    assert len(rows) == len(partitions)
    assert len(rows) > 1
    assert df["source_agg_value"].sum() == df[consts.TARGET_AGG_VALUE].sum()

    for _, row in rows:
        assert float(row["source_agg_value"]) > 0
        assert row["source_agg_value"] == row[consts.TARGET_AGG_VALUE]


###############################
# SCHEMA VALIDATION
###############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Spanner to Spanner dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # BigQuery does not have a float32 type.
            "float32:float64,"
            # Spanner does not have a timezone-naive datetime type.
            "timestamp('UTC'):timestamp"
        ),
    )


###############################
# COLUMN VALIDATION
###############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    """Spanner to Spanner dvt_core_types column validation"""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    # Excluded col_datetime because Spanner stores it as a timezone-aware TIMESTAMP
    # (interpreted in America/Los_Angeles) while BigQuery stores naive DATETIME
    # (see issue-1061).
    min_max_cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_datetime")
        ]
    )
    # TODO Include col_date and col_tstz in sum/avg/std when issue-1876 is complete.
    # Excluded date/timestamp columns from sum/avg/std because Spanner fails on
    # DVT's pre-agg CAST(... AS DATETIME) and UNIX_SECONDS(DATE) (see issue-1876).
    sum_avg_std_cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _
            not in (
                "id",
                "col_float32",
                "col_date",
                "col_datetime",
                "col_tstz",
            )
        ]
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        grouped_columns="col_varchar_30",
        sum_cols=sum_avg_std_cols,
        min_cols=min_max_cols,
        max_cols=min_max_cols,
        avg_cols=sum_avg_std_cols,
        std_cols=sum_avg_std_cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_many_columns():
    """dvt_many_cols column validation."""
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_many_cols",
        count_cols="*",
    )


###############################
# ROW VALIDATION
###############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Spanner to Spanner dvt_core_types row validation"""
    row_validation_test(
        tc="mock-conn",
        hash="*",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types column validation"""
    row_validation_test(
        tc="bq-conn",
        # TODO Change --hash to include col_date and col_datetime when issue-1061 is complete.
        hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_string,col_tstz",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types():
    """Spanner to Spanner dvt_core_types row validation with --comparison-fields"""
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types row validation with --comparison-fields"""
    # TODO Include col_datetime when issue-1061 is complete.
    # Excluded col_datetime because Spanner stores it as a timezone-aware TIMESTAMP
    # (interpreted in America/Los_Angeles) while BigQuery stores naive DATETIME
    # (see issue-1061).
    cols = ",".join(
        [_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id", "col_datetime")]
    )
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        comp_fields=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_binary_pk_to_bigquery():
    """Spanner to BigQuery dvt_binary row validation.
    This is testing binary primary key join columns.
    Includes random row filter test.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_binary",
            "--primary-keys=binary_id",
            "--hash=int_id,other_data",
            "--use-random-row",
            "--random-row-batch-size=5",
        ]
    )
    df = run_test_from_cli_args(args)
    binary_key_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_binary_values_to_bigquery():
    """dvt_binary row validation with comparison fields."""
    row_validation_test(
        tables="pso_data_validator.dvt_binary",
        tc="bq-conn",
        primary_keys="int_id",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_many_columns():
    """Spanner dvt_many_cols row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_strings_to_bigquery():
    """Test with string values containing special characters."""
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_strings",
        tc="bq-conn",
        hash="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_composite_pk_to_bigquery():
    """Test composite primary key (integer, varchar, char) row validation with random row sampling."""
    df = row_validation_test(
        tables="pso_data_validator.dvt_composite_pk",
        tc="bq-conn",
        hash="*",
        primary_keys="key1,key2,key3",
        use_random_row=True,
        random_row_batch_size=5,
        filter_status=None,
    )
    assert len(df) == 5
    assert (df["validation_status"] == consts.VALIDATION_STATUS_SUCCESS).all()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_vol_composite_pk_to_bigquery():
    """Test composite primary key high volume row sampling validation."""
    df = row_validation_test(
        tables="pso_data_validator.dvt_vol_composite_pk",
        tc="bq-conn",
        hash="*",
        primary_keys="key1,key2,key3",
        use_random_row=True,
        random_row_batch_size=5000,
    )
    assert len(df) == 0


###############################
# CUSTOM QUERY VALIDATIONS
###############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_validation_core_types():
    """Spanner to Spanner dvt_core_types custom-query validation"""
    custom_query_validation_test(
        tc="mock-conn",
        source_query="select * from dvt_core_types",
        target_query="select * from dvt_core_types",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_column_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types custom-query column validation"""
    # Spanner queries do not use a schema prefix, BigQuery requires the dataset.
    custom_query_validation_test(
        tc="bq-conn",
        source_query="select * from dvt_core_types",
        target_query="select * from pso_data_validator.dvt_core_types",
        count_cols="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_core_types_to_bigquery():
    """Spanner to BigQuery dvt_core_types custom-query row comparison-fields validation"""
    custom_query_validation_test(
        validation_type="row",
        tc="bq-conn",
        source_query="select id,col_int64,COL_VARCHAR_30,col_date from dvt_core_types",
        target_query="select id,col_int64,col_varchar_30,COL_DATE from pso_data_validator.dvt_core_types",
        comp_fields="col_int64,col_varchar_30,col_date",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_row_validation_many_columns():
    """Spanner dvt_many_cols custom-query row validation.
    This is testing many columns logic for --hash, there's a Teradata test for --concat.
    """
    row_validation_many_columns_test(validation_type="custom-query", schema=None)


############################
# GENERATE PARTITIONS TESTS
############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_and_run_partitions(tmp_path: pathlib.Path):
    """Test generate and execute partition configs."""
    pytest.skip("Skipping test_generate_and_run_partitions due to issue-1613.")
    generate_and_run_table_partitions_test(
        tmp_path,
        sc="mock-conn",
        tc="bq-conn",
    )


##################
# RAW QUERY TESTS
##################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys, table="dvt_core_types")


####################
# CONNECTIONS TESTS
####################
def test_connections_add(caplog, tmp_path, monkeypatch):
    """Test data-validation connections add command."""
    conn_args = [
        "--project-id",
        PROJECT_ID,
        "--instance-id",
        SPANNER_INSTANCE,
        "--database-id",
        SPANNER_DATABASE,
    ]
    connections_add_test(
        caplog, consts.SOURCE_TYPE_SPANNER, conn_args, tmp_path, monkeypatch
    )


##############################
# FIND-TABLE VALIDATION TESTS
##############################
@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Spanner test of find-tables command."""
    find_tables_test()


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_views_and_tables():
    """Spanner to BigQuery test of find-tables command including views."""
    pytest.skip("Skipping test_find_views_and_tables due to issue-1888.")
    find_tables_test(include_views=True)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_cli_find_tables():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(CLI_FIND_TABLES_ARGS)
    tables_json = find_tables.find_tables_using_string_matching(args)
    tables = json.loads(tables_json)
    assert isinstance(tables_json, str)
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "array_table",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "array_table",
    } in tables
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "functional_alltypes",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "functional_alltypes",
    } in tables
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "students_pointer",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "students_pointer",
    } in tables
    assert {
        "schema_name": "pso_data_validator",
        "table_name": "dvt_core_types_vw",
        "target_schema_name": "pso_data_validator",
        "target_table_name": "dvt_core_types_vw",
    } not in tables
