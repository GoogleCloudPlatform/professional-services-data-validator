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
from typing import TYPE_CHECKING, Tuple  # Build is still on Python 3.8
import pathlib

from data_validation import __main__ as main
from data_validation import (
    cli_tools,
    consts,
    data_validation,
    find_tables,
    raw_query,
)

from data_validation.partition_builder import PartitionBuilder

if TYPE_CHECKING:
    from argparse import Namespace
    from pandas import DataFrame


DVT_CORE_TYPES_COLUMNS = [
    "id",
    "col_int8",
    "col_int16",
    "col_int32",
    "col_int64",
    "col_dec_20",
    "col_dec_38",
    "col_dec_10_2",
    "col_float32",
    "col_float64",
    "col_varchar_30",
    "col_char_2",
    "col_string",
    "col_date",
    "col_datetime",
    "col_tstz",
]


def id_type_test_assertions(df, expected_rows=5):
    """Standard assertions for assorted primary key type integration tests."""
    # Should be expected_rows rows in the df all with status success.
    assert (
        len(df) == expected_rows
    ), f"We expect {expected_rows} rows with status success from this validation"
    assert all(
        _ == consts.VALIDATION_STATUS_SUCCESS
        for _ in df[consts.VALIDATION_STATUS].to_list()
    ), "Not all rows have status 'success'"


def binary_key_assertions(df):
    """Standard assertions for binary key integration test.
    These tests use BigQuery as a fixed target and execute against all other engines."""
    id_type_test_assertions(df)
    # Validate a sample primary key value is hex(ish).
    sample_gbc = df[consts.GROUP_BY_COLUMNS].to_list().pop()
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
    for column_name, status in zip(
        df[consts.SOURCE_COLUMN_NAME], df[consts.VALIDATION_STATUS]
    ):
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


def run_tests_from_cli_args(
    args: "Namespace",
    expected_config_managers: int = 1,
) -> list:
    """Multi test version of run_test_from_cli_args()"""
    config_managers = main.build_config_managers_from_args(args)
    assert (
        len(config_managers) == expected_config_managers
    ), f"Expected {expected_config_managers} but found: {len(config_managers)}"
    dfs = []
    for config_manager in config_managers:
        validator = data_validation.DataValidation(config_manager.config, verbose=False)
        dfs.append(validator.execute())
    return dfs


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
    # We expect the validation to be split into multiple config managers.
    for df in run_tests_from_cli_args(
        args, expected_config_managers=expected_config_managers
    ):
        # With filter on failures the data frame should be empty.
        assert len(df) == 0


def find_tables_assertions(
    command_output: str,
    expected_source_schema: str = "pso_data_validator",
    expected_target_schema: str = "pso_data_validator",
    include_views: bool = False,
    check_for_view: bool = True,
):
    assert isinstance(command_output, str)
    assert command_output
    output_dict = json.loads(command_output)
    assert output_dict
    assert isinstance(output_dict, list)
    assert isinstance(output_dict[0], dict)
    assert len(output_dict) > 1
    assert all(_["schema_name"] == expected_source_schema for _ in output_dict)
    assert all(_["target_schema_name"] == expected_target_schema for _ in output_dict)
    # Assert that a couple of known tables are in the map.
    assert "dvt_core_types" in [_["table_name"] for _ in output_dict]
    assert "dvt_core_types" in [_["target_table_name"] for _ in output_dict]
    if check_for_view:
        source_names = [_["table_name"] for _ in output_dict]
        target_names = [_["target_table_name"] for _ in output_dict]
        if include_views:
            # Assert that known view is IN the map.
            assert (
                "dvt_core_types_vw" in source_names
            ), f"dvt_core_types_vw should be in command source output: {source_names}"
            assert (
                "dvt_core_types_vw" in target_names
            ), f"dvt_core_types_vw should be in command target output: {target_names}"
        else:
            # Assert that known view is NOT IN the map.
            assert (
                "dvt_core_types_vw" not in source_names
            ), f"dvt_core_types_vw should NOT be in command source output: {source_names}"
            assert (
                "dvt_core_types_vw" not in target_names
            ), f"dvt_core_types_vw should NOT be in command target output: {target_names}"


def find_tables_test(
    tc: str = "bq-conn",
    allowed_schema: str = "pso_data_validator",
    include_views: bool = False,
    check_for_view: bool = True,
):
    """Generic find-tables test"""
    parser = cli_tools.configure_arg_parser()
    cli_arg_list = [
        "find-tables",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"--allowed-schemas={allowed_schema}",
        "--include-views" if include_views else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    args = parser.parse_args(cli_arg_list)
    output = find_tables.find_tables_using_string_matching(args)
    find_tables_assertions(
        output,
        include_views=include_views,
        check_for_view=check_for_view,
        expected_source_schema=allowed_schema,
        expected_target_schema=allowed_schema,
    )


def schema_validation_test(
    tables: str = "pso_data_validator.dvt_core_types",
    tc: str = "bq-conn",
    filter_status: str = "fail",
    exclusion_columns: str = "id",
    allow_list: str = None,
    allow_list_file: str = None,
    result_handler: str = None,
    labels: str = None,
) -> "DataFrame":
    """Generic schema validation test.

    All tests expect an empty dataframe as the assertion.
    """
    parser = cli_tools.configure_arg_parser()
    cli_arg_list = [
        "validate",
        "schema",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"-tbls={tables}",
        f"--exclusion-columns={exclusion_columns}",
        f"--filter-status={filter_status}" if filter_status else None,
        f"--allow-list={allow_list}" if allow_list else None,
        f"--allow-list-file={allow_list_file}" if allow_list_file else None,
        f"--result-handler={result_handler}" if result_handler else None,
        f"--labels={labels}" if labels else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    args = parser.parse_args(cli_arg_list)
    df = run_test_from_cli_args(args)
    if filter_status == "fail":
        # With filter on failures the data frame should be empty
        assert len(df) == 0
    return df


def column_validation_test_args(
    tables: str = "pso_data_validator.dvt_core_types",
    tc: str = "bq-conn",
    count_cols: str = None,
    sum_cols: str = None,
    min_cols: str = None,
    max_cols: str = None,
    filters: str = None,
    grouped_columns: str = None,
    filter_status: str = "fail",
    wildcard_include_timestamp: bool = False,
    result_handler: str = None,
):
    parser = cli_tools.configure_arg_parser()
    cli_arg_list = [
        "validate",
        "column",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"-tbls={tables}",
        f"--filter-status={filter_status}" if filter_status else None,
        f"--count={count_cols}" if count_cols else None,
        f"--sum={sum_cols}" if sum_cols else None,
        f"--min={min_cols}" if min_cols else None,
        f"--max={max_cols}" if max_cols else None,
        f"--filters={filters}" if filters else None,
        f"--grouped-columns={grouped_columns}" if grouped_columns else None,
        "--wildcard-include-timestamp" if wildcard_include_timestamp else None,
        f"--result-handler={result_handler}" if result_handler else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    return parser.parse_args(cli_arg_list)


def column_validation_test(
    tables="pso_data_validator.dvt_core_types",
    tc="bq-conn",
    count_cols=None,
    sum_cols=None,
    min_cols=None,
    max_cols=None,
    filters=None,
    grouped_columns=None,
    wildcard_include_timestamp: bool = False,
    filter_status: str = "fail",
    expected_rows=0,
    result_handler: str = None,
):
    """Generic column validation test.

    Standard test expects an empty dataframe as the assertion but has override.
    """
    args = column_validation_test_args(
        tables=tables,
        tc=tc,
        count_cols=count_cols,
        sum_cols=sum_cols,
        min_cols=min_cols,
        max_cols=max_cols,
        filters=filters,
        grouped_columns=grouped_columns,
        wildcard_include_timestamp=wildcard_include_timestamp,
        filter_status=filter_status,
        result_handler=result_handler,
    )
    df = run_test_from_cli_args(args)
    assert len(df) == expected_rows
    return df


def column_validation_test_config_managers(
    tables="pso_data_validator.dvt_core_types",
    tc="bq-conn",
    count_cols=None,
    sum_cols=None,
    min_cols=None,
    max_cols=None,
    filters=None,
    grouped_columns=None,
) -> list:
    args = column_validation_test_args(
        tables=tables,
        tc=tc,
        count_cols=count_cols,
        sum_cols=sum_cols,
        min_cols=min_cols,
        max_cols=max_cols,
        filters=filters,
        grouped_columns=grouped_columns,
    )
    return main.build_config_managers_from_args(args)


def row_validation_test(
    tables="pso_data_validator.dvt_core_types",
    tc="bq-conn",
    hash="col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float32,col_float64,col_varchar_30,col_char_2,col_date,col_datetime,col_tstz",
    filters="1=1",
    filter_status: str = "fail",
    primary_keys="id",
    comp_fields=None,
    concat=None,
    use_randow_row=False,
    random_row_batch_size=None,
    result_handler: str = None,
):
    """Generic row validation test. All row validation tests expect an empty dataframe as the assertion"""
    parser = cli_tools.configure_arg_parser()
    if comp_fields:
        col_option = f"--comparison-fields={comp_fields}"
    elif concat:
        col_option = f"--concat={concat}"
    else:
        col_option = f"--hash={hash}"
    cli_arg_list = [
        "validate",
        "row",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"-tbls={tables}",
        f"--filters={filters}",
        f"--primary-keys={primary_keys}" if primary_keys else None,
        col_option,
        f"--filter-status={filter_status}" if filter_status else None,
        "--use-random-row" if use_randow_row else None,
        (
            f"--random-row-batch-size={random_row_batch_size}"
            if random_row_batch_size
            else None
        ),
        f"--result-handler={result_handler}" if result_handler else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    args = parser.parse_args(cli_arg_list)
    df = run_test_from_cli_args(args)
    if filter_status == "fail":
        # With filter on failures the data frame should be empty
        assert len(df) == 0
    return df


def id_column_row_validation_test(
    tables: str,
    tc: str = "bq-conn",
    hash: str = "id,other_data",
    comp_fields: str = None,
    use_randow_row: bool = True,
):
    """Specific row validation test for primary key data type tests"""
    parser = cli_tools.configure_arg_parser()
    if comp_fields:
        col_option = f"--comparison-fields={comp_fields}"
    else:
        col_option = f"--hash={hash}"
    cli_arg_list = [
        "validate",
        "row",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"-tbls={tables}",
        "--primary-keys=id",
        col_option,
        "--use-random-row" if use_randow_row else None,
        "--random-row-batch-size=5" if use_randow_row else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    args = parser.parse_args(cli_arg_list)
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


def id_column_query_row_validation_test(
    tables: str,
    tc: str = "bq-conn",
    hash: str = "id,other_data",
    comp_fields: str = None,
):
    """Specific custom query row validation test for primary key data type tests"""
    parser = cli_tools.configure_arg_parser()
    if comp_fields:
        col_option = f"--comparison-fields={comp_fields}"
    else:
        col_option = f"--hash={hash}"
    cli_arg_list = [
        "validate",
        "custom-query",
        "row",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"-sq=SELECT * FROM {tables.split('=')[0]}",
        f"-tq=SELECT * FROM {tables.split('=')[1] if '=' in tables else tables.split('=')[0]}",
        "--primary-keys=id",
        col_option,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    args = parser.parse_args(cli_arg_list)
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


def fixed_char_varchar_test(
    tables: Tuple[str, str] = (
        "pso_data_validator.dvt_fixed_char_id",
        "pso_data_validator.dvt_varchar_id",
    ),
):
    """Row validation test for fixed and variable length character types as primary keys"""
    id_column_row_validation_test(
        tables[0],
    )
    id_column_row_validation_test(
        tables[1],
    )
    id_column_query_row_validation_test(
        tables[0],
    )
    id_column_query_row_validation_test(
        tables[1],
    )


def partition_table_test(
    expected_filter: str,
    pk="course_id,quarter_id,recd_timestamp,registration_date,approved",
    tables="pso_data_validator.test_generate_partitions_v2",
    filters="quarter_id != 1111",
    partition_num=9,
    parts_per_file=5,
):
    """Test generate table partitions for a database. Usually only the partition_filter is different
    because of the differences in SQL between the databases. Some databases have different table names,
    Teradata has a different syntax for inequality and Postgres has different column names/types for primary keys.
    The unit tests in tests/unit/test_partition_builder.py check if the filters are split into configs and
    stored in the filesystem correctly.
    """

    parser = cli_tools.configure_arg_parser()
    cli_arg_list = [
        "generate-table-partitions",
        "-sc=mock-conn",
        "-tc=mock-conn",
        f"-tbls={tables}",
        f"-pk={pk}",
        "-hash=*",
        "-cdir=/home/users/yaml",
        f"-pn={partition_num}",
        f"-ppf={parts_per_file}",
        f"-filters={filters}" if filters else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]
    args = parser.parse_args(cli_arg_list)
    config_managers = main.build_config_managers_from_args(args, consts.ROW_VALIDATION)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()

    assert len(partition_filters) == 1  # only one pair of tables
    # Number of partitions is as requested - assume table rows > partitions requested
    assert len(partition_filters[0][0]) == partition_builder.args.partition_num
    assert partition_filters[0] == expected_filter


def partition_query_test(
    expected_filter: str,
    tmp_path: pathlib.Path,
    pk="course_id,quarter_id,recd_timestamp,registration_date,approved",
    tables="pso_data_validator.test_generate_partitions_v2",
    filters="quarter_id != 1111",
):
    """Test generate table partitions for custom queries. Usually only the partition_filter is different
    because of the differences in SQL between the databases. Some databases have different table names,
    Teradata has a different syntax for inequality and Postgres has different column names/types for primary keys.
    The unit tests in tests/unit/test_partition_builder.py check if the filters are split into configs and
    stored in the filesystem correctly.
    """
    tables_list = tables.split("=")
    source_table_name = tables_list[0]
    target_table_name = tables_list[1] if len(tables_list) == 2 else tables_list[0]
    target_query = f"select * from {target_table_name}"
    source_query = f"select * from {source_table_name}"
    source_query_file = tmp_path / "source_query.sql"
    source_query_file.write_text(source_query)

    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "generate-table-partitions",
            "-sc=mock-conn",
            "-tc=mock-conn",
            f"-sqf={str(source_query_file)}",
            f"-tq={target_query}",
            f"-pk={pk}",
            "-hash=*",
            "-cdir=/home/users/yaml",
            "-pn=9",
            "-ppf=5",
            f"-filters={filters}",
        ]
    )
    setattr(args, "custom_query_type", "row")
    config_managers = main.build_config_managers_from_args(args, consts.CUSTOM_QUERY)
    partition_builder = PartitionBuilder(config_managers, args)
    partition_filters = partition_builder._get_partition_key_filters()

    assert len(partition_filters) == 1  # only one pair of tables
    # Number of partitions is as requested - assume table rows > partitions requested
    assert len(partition_filters[0][0]) == partition_builder.args.partition_num
    assert partition_filters[0] == expected_filter


def custom_query_validation_test(
    validation_type="column",
    tc="bq-conn",
    source_query="select * from pso_data_validator.dvt_core_types",
    target_query="select * from pso_data_validator.dvt_core_types",
    filters=None,
    count_cols=None,
    min_cols=None,
    max_cols=None,
    sum_cols=None,
    grouped_columns=None,
    comp_fields=None,
    hash="*",
    assert_df_not_empty=False,
):
    """Generic custom-query validation test.

    All tests expect an empty dataframe as the assertion.
    """
    parser = cli_tools.configure_arg_parser()
    cli_arg_list = [
        "validate",
        "custom-query",
        f"{validation_type}",
        "-sc=mock-conn",
        f"-tc={tc}",
        f"--source-query={source_query}",
        f"--target-query={target_query}",
        "--filter-status=fail",
        f"--filters={filters}" if filters else None,
        # Column validation parameters
        f"--count={count_cols}" if count_cols else None,
        f"--sum={sum_cols}" if sum_cols else None,
        f"--min={min_cols}" if min_cols else None,
        f"--max={max_cols}" if max_cols else None,
        f"--grouped-columns={grouped_columns}" if grouped_columns else None,
    ]
    cli_arg_list = [_ for _ in cli_arg_list if _]

    # Row validation parameters
    if validation_type == "row":
        cli_arg_list.append("--primary-keys=id")

        if comp_fields:
            cli_arg_list.append(f"--comparison-fields={comp_fields}")
        else:
            cli_arg_list.append(f"--hash={hash}")

    args = parser.parse_args(cli_arg_list)
    df = run_test_from_cli_args(args)
    if assert_df_not_empty:
        # With filter on failures the data frame should be populated
        assert len(df) > 0
    else:
        # With filter on failures the data frame should be empty
        assert len(df) == 0


def raw_query_rows(
    query: str,
    conn: str = "mock-conn",
) -> list:
    """Get rows for a raw query test."""
    parser = cli_tools.configure_arg_parser()
    cli_arg_list = [
        "query",
        f"--conn={conn}",
        f"--query={query}",
    ]
    args = parser.parse_args(cli_arg_list)
    return raw_query.run_raw_query_against_connection(args)


def raw_query_test(
    capsys,
    conn: str = "mock-conn",
    query: str = "select * from pso_data_validator.dvt_core_types",
    table: str = None,
    expected_rows: int = 3,
):
    """Raw query test."""
    if table:
        query = f"select * from {table}"
    rows = raw_query_rows(query, conn=conn)
    assert len(rows) == expected_rows
    assert len(rows[0]) > 0
    raw_query.print_raw_query_output(rows)
    captured = capsys.readouterr()
    assert "characters truncated" not in captured.out
