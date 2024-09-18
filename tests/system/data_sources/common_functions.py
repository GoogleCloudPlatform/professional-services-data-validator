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

import json
import string
from typing import TYPE_CHECKING

from data_validation import __main__ as main
from data_validation import consts, data_validation

from data_validation import (
    cli_tools,
)
from data_validation.partition_builder import PartitionBuilder

if TYPE_CHECKING:
    from argparse import Namespace
    from pandas import DataFrame


def id_type_test_assertions(df, expected_rows=5):
    """Standard assertions for assorted primary key type integration tests."""
    # Should be expected_rows rows in the df all with status success.
    assert (
        len(df) == expected_rows
    ), f"We expect {expected_rows} rows with status success from this validation"
    assert all(
        _ == consts.VALIDATION_STATUS_SUCCESS for _ in df["validation_status"].to_list()
    ), "Not all rows have status 'success'"


def binary_key_assertions(df):
    """Standard assertions for binary key integration test.
    These tests use BigQuery as a fixed target and execute against all other engines."""
    id_type_test_assertions(df)
    # Validate a sample primary key value is hex(ish).
    sample_gbc = df["group_by_columns"].to_list().pop()
    sample_gbc = json.loads(sample_gbc)
    sample_key = [v for _, v in sample_gbc.items()].pop()
    assert all(_ in string.hexdigits for _ in sample_key)


def null_not_null_assertions(df):
    """Standard assertions for null_not_null integration test.
    These tests use BigQuery as a fixed target with a mismatch of not null/nullable settings.
    All other engines are validated against BigQuery to check we get the correct status.
    """
    # Should be 4 columns in the Dataframe.
    assert len(df) == 4
    match_columns = ["col_nn", "col_nullable"]
    mismatch_columns = ["col_src_nn_trg_n", "col_src_n_trg_nn"]
    for column_name, status in zip(df["source_column_name"], df["validation_status"]):
        assert column_name in (match_columns + mismatch_columns)
        if column_name in match_columns:
            # These columns are the same for all engines and should succeed.
            assert (
                status == consts.VALIDATION_STATUS_SUCCESS
            ), f"Column: {column_name}, status: {status} != {consts.VALIDATION_STATUS_SUCCESS}"
        elif column_name in mismatch_columns:
            # These columns are the different for source and target engines and should fail.
            assert (
                status == consts.VALIDATION_STATUS_FAIL
            ), f"Column: {column_name}, status: {status} != {consts.VALIDATION_STATUS_FAIL}"


def run_test_from_cli_args(args: "Namespace") -> "DataFrame":
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    return validator.execute()


def row_validation_many_columns_test(
    schema: str = "pso_data_validator",
    table: str = "dvt_many_cols",
    validation_type: str = "row",
    concat_arg: str = "hash",
    expected_config_managers: int = 1,
    target_conn: str = "mock-conn",
):
    """Runs a dvt_many_cols validation (standard or custom-query) based on input parameters and tests results."""
    parser = cli_tools.configure_arg_parser()
    schema_prefix = f"{schema}." if schema else ""
    if validation_type == "row":
        args = parser.parse_args(
            [
                "validate",
                "row",
                "-sc=mock-conn",
                f"-tc={target_conn}",
                f"-tbls={schema_prefix}{table}",
                "--primary-keys=id",
                f"--{concat_arg}=*",
                "--filter-status=fail",
            ]
        )
    else:
        query = f"SELECT * FROM {schema_prefix}{table}"
        args = parser.parse_args(
            [
                "validate",
                "custom-query",
                "row",
                "-sc=mock-conn",
                "-tc=mock-conn",
                f"--source-query={query}",
                f"--target-query={query}",
                "--primary-keys=id",
                f"--{concat_arg}=*",
                "--filter-status=fail",
            ]
        )
    config_managers = main.build_config_managers_from_args(args)
    # We expect the validation to be split into multiple config managers.
    assert (
        len(config_managers) == expected_config_managers
    ), f"Expected {expected_config_managers} but found: {len(config_managers)}"
    for config_manager in config_managers:
        validator = data_validation.DataValidation(config_manager.config, verbose=False)
        df = validator.execute()
        # With filter on failures the data frame should be empty.
        assert len(df) == 0


def find_tables_assertions(command_output: str):
    assert isinstance(command_output, str)
    assert command_output
    output_dict = json.loads(command_output)
    assert output_dict
    assert isinstance(output_dict, list)
    assert isinstance(output_dict[0], dict)
    assert len(output_dict) > 1
    assert all(_["schema_name"] == "pso_data_validator" for _ in output_dict)
    assert all(_["target_schema_name"] == "pso_data_validator" for _ in output_dict)
    # Assert that a couple of known tables are in the map.
    assert "dvt_core_types" in [_["table_name"] for _ in output_dict]
    assert "dvt_core_types" in [_["target_table_name"] for _ in output_dict]


def row_validation_test(
    # pk="id",
    tables="pso_data_validator.test_generate_partitions",
    tc="bq-conn",
    hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz",
    filters="1=1",
):
    """Generic row validation test. All row validation tests expect an empty dataframe as the assertion
    1.
    """
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            f"-tc={tc}",
            f"-tbls={tables}",
            f"--filters={filters}",
            "--primary-keys=id",
            "--filter-status=fail",
            f"--hash={hash}",
        ]
    )
    df = run_test_from_cli_args(args)
    # With filter on failures the data frame should be empty
    assert len(df) == 0


def generate_partitions_test(
    expected_filter: str,
    pk="course_id,quarter_id,student_id",
    tables="pso_data_validator.test_generate_partitions",
    filters="quarter_id != 1111",
):
    """Test generate table partitions for a database. Usually only the partition_filter is different
    because of the differences in SQL between the databases. Some databases have different table names,
    Teradata has a different syntax for inequality and Postgres has different column names/types for primary keys.
    The unit tests in tests/unit/test_partition_builder.py check if the filters are split into configs and
    stored in the filesystem correctly.
    """

    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "generate-table-partitions",
            "-sc=mock-conn",
            "-tc=mock-conn",
            f"-tbls={tables}",
            f"-pk={pk}",
            "-hash=*",
            "-cdir=/home/users/yaml",
            "-pn=9",
            "-ppf=5",
            f"-filters={filters}",
        ]
    )
    config_managers = main.build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()
    yaml_configs_list = partition_builder._add_partition_filters(partition_filters)

    assert len(partition_filters) == 1  # only one pair of tables
    # Number of partitions is as requested - assume table rows > partitions requested
    assert len(partition_filters[0][0]) == partition_builder.args.partition_num
    assert partition_filters[0] == expected_filter
