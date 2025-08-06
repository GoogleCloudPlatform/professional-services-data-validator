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
import pathlib
from unittest import mock

import pytest

from data_validation import cli_tools, consts
from tests.system.data_sources.common_functions import (
    DVT_TRICKY_DATES_COLUMNS,
    column_validation_test,
    find_tables_test,
    id_type_test_assertions,
    raw_query_test,
    row_validation_test,
    run_test_from_cli_args,
    schema_validation_test,
    custom_query_validation_test,
)
from tests.system.data_sources.test_bigquery import BQ_CONN
from tests.system.data_sources.common_functions import (
    DVT_CORE_TYPES_COLUMNS,
    partition_table_test,
    partition_query_test,
)


IMPALA_HOST = os.getenv("IMPALA_HOST", "localhost")
IMPALA_PORT = os.getenv("IMPALA_PORT", "21050")
IMPALA_DATABASE = os.getenv("IMPALA_DATABASE", "default")
IMPALA_AUTH_MECH = os.getenv("IMPALA_AUTH_MECH", "NOSASL")


CONN = {
    consts.SOURCE_TYPE: consts.SOURCE_TYPE_IMPALA,
    "host": IMPALA_HOST,
    "port": IMPALA_PORT,
    "database": IMPALA_DATABASE,
    "auth_mechanism": IMPALA_AUTH_MECH,
}

# Expected result from partitioning table on 3 keys
EXPECTED_PARTITION_FILTER = [
    [
        "(quarter_id != 1111) AND ((`course_id` < 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Edward') OR ((`course_id` = 'St. Edward') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Edward') OR ((`course_id` = 'St. Edward') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. John') OR ((`course_id` = 'St. John') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. John') OR ((`course_id` = 'St. John') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. Jude') OR ((`course_id` = 'St. Jude') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Jude') OR ((`course_id` = 'St. Jude') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Paul') OR ((`course_id` = 'St. Paul') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Paul') OR ((`course_id` = 'St. Paul') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE)))))))))",
    ],
    [
        "(quarter_id != 1111) AND ((`course_id` < 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG001') OR ((`course_id` = 'ALG001') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG002  t0.') OR ((`course_id` = 'ALG002  t0.') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG003') OR ((`course_id` = 'ALG003') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` < 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'ALG004') OR ((`course_id` = 'ALG004') AND ((`quarter_id` > 5678) OR ((`quarter_id` = 5678) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '2023-08-23') OR ((`registration_date` = '2023-08-23') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Edward') OR ((`course_id` = 'St. Edward') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Edward') OR ((`course_id` = 'St. Edward') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. John') OR ((`course_id` = 'St. John') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < TRUE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. John') OR ((`course_id` = 'St. John') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= TRUE))))))))) AND ((`course_id` < 'St. Jude') OR ((`course_id` = 'St. Jude') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Jude') OR ((`course_id` = 'St. Jude') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE))))))))) AND ((`course_id` < 'St. Paul') OR ((`course_id` = 'St. Paul') AND ((`quarter_id` < 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` < 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` < '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` < FALSE)))))))))",
        "(quarter_id != 1111) AND ((`course_id` > 'St. Paul') OR ((`course_id` = 'St. Paul') AND ((`quarter_id` > 1234) OR ((`quarter_id` = 1234) AND ((`recd_timestamp` > 'NaT') OR ((`recd_timestamp` = 'NaT') AND ((`registration_date` > '1969-07-20') OR ((`registration_date` = '1969-07-20') AND (`approved` >= FALSE)))))))))",
    ],
]


def mock_get_connection_config(*args):
    if args[1] in ("impala-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    """Impala to Impala dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    """Impala to BigQuery dvt_core_types schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="bq-conn",
        allow_list=(
            # Impala integers go to BigQuery INT64.
            "int8:int64,int16:int64,int32:int64,"
            # Hive does not have a time zoned timestamp.
            "timestamp:timestamp('UTC'),"
            # BigQuery does not have a float32 type.
            "float32:float64"
        ),
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_view_core_types_vw():
    """Impala to Impala view dvt_core_types_vw schema validation"""
    schema_validation_test(
        tables="pso_data_validator.dvt_core_types_vw",
        tc="mock-conn",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_bool():
    """Impala to Impala dvt_bool schema validation"""
    schema_validation_test(tables="pso_data_validator.dvt_bool", tc="mock-conn")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_generate_partitions(tmp_path: pathlib.Path):
    """Test generate partitions on Impala, first on table, then on custom query"""
    partition_table_test(
        EXPECTED_PARTITION_FILTER,
    )
    partition_query_test(
        EXPECTED_PARTITION_FILTER,
        tmp_path,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    column_validation_test(
        tc="mock-conn",
        count_cols="*",
        sum_cols="*",
        min_cols="*",
        max_cols="*",
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    """Impala to BigQuery dvt_core_types column validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    # TODO Change cols to include col_char_2 when issue-1514 is complete.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_char_2")
        ]
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_core_types",
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        avg_cols=cols,
        std_cols=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_view_core_types_vw():
    """Impala to Impala view dvt_core_types_vw column validation"""
    cols = ",".join([_ for _ in DVT_CORE_TYPES_COLUMNS if _ not in ("id")])
    column_validation_test(
        tc="mock-conn",
        tables="pso_data_validator.dvt_core_types_vw",
        count_cols=cols,
        sum_cols=cols,
        min_cols=cols,
        max_cols=cols,
        filters="id>0 AND col_int8>0",
        grouped_columns="col_varchar_30",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    """Impala to Impala dvt_core_types row validation"""
    row_validation_test(
        tc="mock-conn",
        concat="*",
        filters="id>0 AND col_int8>0",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types_to_bigquery():
    """Impala to BigQuery dvt_core_types row validation"""
    # Excluded col_float32 because BigQuery does not have an exact same type and
    # float32/64 are lossy and cannot be compared.
    # col_float64 is excluded below because there is no way to control the format
    # when casting to string.
    # TODO Change cols to include col_dec_10_2 when issue-1379 is complete.
    cols = ",".join(
        [
            _
            for _ in DVT_CORE_TYPES_COLUMNS
            if _ not in ("id", "col_float32", "col_float64", "col_dec_10_2")
        ]
    )
    # Impala does not have sha2() until Impala v4.1.
    # Our test infrastructiure is Impala v3 therefore we use --concat below.
    row_validation_test(tc="bq-conn", concat=cols)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_core_types():
    """Impala to Impala dvt_core_types row validation with --comp-fields"""
    row_validation_test(
        tables="pso_data_validator.dvt_core_types",
        tc="mock-conn",
        comp_fields="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_pangrams_to_bigquery():
    """Impala to BigQuery dvt_pangrams row validation.
    This is testing comparisons across a wider set of characters than standard test data.
    """
    parser = cli_tools.configure_arg_parser()
    # Impala does not have sha2() until Impala v4.1.
    # Our test infrastructiure is Impala v3 therefore we use --concat below.
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_pangrams",
            "--primary-keys=id",
            "--concat=*",
        ]
    )
    df = run_test_from_cli_args(args)
    id_type_test_assertions(df)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_custom_query_validation_core_types_to_bigquery():
    """Impala to BigQuery dvt_core_types custom-query validation
    Using BigQuery target because Hive queries are really slow."""
    custom_query_validation_test(tc="bq-conn", count_cols="*")


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_bool_to_bigquery():
    """Test row validation on a table with bool data types."""
    pytest.skip("Skipping test_row_validation_bool_to_bigquery due to issue-1380.")
    # Impala does not have sha2() until Impala v4.1.
    # Our test infrastructiure is Impala v3 therefore we use --concat below.
    row_validation_test(
        tables="pso_data_validator.dvt_bool",
        tc="bq-conn",
        concat="*",
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    # We cannot test col_dt_low and col_ts_low because the low value of Impala timestamps
    # is incompatible with other engines.
    cols = ",".join(
        _ for _ in DVT_TRICKY_DATES_COLUMNS if _ not in ("col_dt_low", "col_ts_low")
    )
    column_validation_test(
        tc="bq-conn",
        tables="pso_data_validator.dvt_tricky_dates",
        min_cols=cols,
        max_cols=cols,
        sum_cols=cols,
        grouped_columns="id",
        wildcard_include_timestamp=True,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    # We cannot test col_dt_low and col_ts_low because the low value of Impala timestamps
    # is incompatible with other engines.
    cols = ",".join(
        _ for _ in DVT_TRICKY_DATES_COLUMNS if _ not in ("col_dt_low", "col_ts_low")
    )
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        concat=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_comp_fields_tricky_dates_to_bigquery():
    """Test with date values that are at the extremes, e.g. 9999-12-31."""
    # We cannot test col_dt_low and col_ts_low because the low value of Impala timestamps
    # is incompatible with other engines.
    cols = ",".join(
        _ for _ in DVT_TRICKY_DATES_COLUMNS if _ not in ("col_dt_low", "col_ts_low")
    )
    row_validation_test(
        tables="pso_data_validator.dvt_tricky_dates",
        tc="bq-conn",
        comp_fields=cols,
    )


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_find_tables():
    """Impala to BigQuery test of find-tables command."""
    # check_for_view=False because there is no practical way to exclude views on Impala.
    find_tables_test(check_for_view=False)


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_raw_query_dvt_row_types(capsys):
    """Test data-validation query command."""
    raw_query_test(capsys)
