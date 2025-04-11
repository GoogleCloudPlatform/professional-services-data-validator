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

import pandas
import pandas.testing
import pytest
import logging
import json

from freezegun import freeze_time
from data_validation import metadata, consts

_NAN = float("nan")

EXAMPLE_RUN_METADATA = metadata.RunMetadata(
    validations={
        "count": metadata.ValidationMetadata(
            source_table_name="test_source",
            source_table_schema="bq-public.source_dataset",
            source_column_name="timecol",
            target_table_name="test_target",
            target_table_schema="bq-public.target_dataset",
            target_column_name="timecol",
            validation_type="Column",
            aggregation_type="count",
            primary_keys=[],
            num_random_rows=None,
            threshold=0.0,
        ),
    },
    start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
    end_time=datetime.datetime(1998, 9, 4, 7, 31, 42),
    labels=[],
    run_id="test-run",
)


@pytest.fixture
def module_under_test():
    from data_validation import combiner

    return combiner


def pandas_df(cols: int, rows: int):
    data = {"count": [1]}
    for i in range(cols):
        col_name = f"count__col{i + 1}"
        data[col_name] = range(rows)
    return pandas.DataFrame(data)


def test_generate_report_with_different_columns(module_under_test):
    source = pandas.DataFrame({"count": [1], "sum": [3]})
    target = pandas.DataFrame({"count": [2]})
    with pytest.raises(
        ValueError, match="Expected source and target to have same schema"
    ):
        module_under_test.generate_report(
            # Schema validation occurs before run_metadata is needed.
            None,
            source,
            target,
        )


def test_generate_report_with_too_many_rows(module_under_test):
    source = pandas.DataFrame({"count": [1, 1]})
    target = pandas.DataFrame({"count": [2, 2]})
    report = module_under_test.generate_report(
        # Validation occurs before run_metadata is needed.
        EXAMPLE_RUN_METADATA,
        source,
        target,
    )

    # TODO: how do we want to handle this going forward?
    assert len(report) == 16


@freeze_time("1998-09-04 07:31:42")
@pytest.mark.parametrize(
    ("input_df"),
    [
        pandas_df(100, 1),
        pandas_df(250, 1),
        pandas_df(500, 1),
    ],
)
def test_generate_report_with_many_columns(module_under_test, input_df):
    """Test that combiner works for tables with many validations (no RecursionError)."""
    validations = {
        _: metadata.ValidationMetadata(
            source_table_name="test_source",
            source_table_schema="source_dataset",
            source_column_name=None if _ == "count" else _,
            target_table_name="test_target",
            target_table_schema="target_dataset",
            target_column_name=None if _ == "count" else _,
            validation_type="Column",
            aggregation_type="count",
            primary_keys=[],
            num_random_rows=None,
            threshold=0.0,
        )
        for _ in input_df.columns
    }
    run_metadata = metadata.RunMetadata(
        validations=validations,
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=None,
        labels=[],
        run_id="test-run",
    )

    report = module_under_test.generate_report(
        run_metadata,
        input_df,  # mock for source_df
        input_df,  # mock for target_df
    )
    # There are no filters so the resulting Dataframe should have one row per validation.
    assert len(report) == len(validations)


@freeze_time("1998-09-04 07:31:42")
@pytest.mark.parametrize(
    ("source_df", "target_df", "run_metadata", "expected"),
    (
        (
            pandas.DataFrame({"count": [1]}),
            pandas.DataFrame({"count": [2]}),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: [None],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: [None],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["count"],
                    consts.VALIDATION_NAME: ["count"],
                    consts.SOURCE_AGG_VALUE: ["1"],
                    consts.TARGET_AGG_VALUE: ["2"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [1.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [100.0],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_FAIL],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame(
                {"timecol__max": [pandas.Timestamp("2020-07-01T16:00:00Z")]}
            ),
            pandas.DataFrame(
                {"timecol__max": [pandas.Timestamp("2020-07-01T16:00:00Z")]}
            ),
            metadata.RunMetadata(
                validations={
                    "timecol__max": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="timecol",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="timecol",
                        validation_type="Column",
                        aggregation_type="max",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: ["timecol"],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: ["timecol"],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["max"],
                    consts.VALIDATION_NAME: ["timecol__max"],
                    consts.SOURCE_AGG_VALUE: ["2020-07-01 16:00:00+00:00"],
                    consts.TARGET_AGG_VALUE: ["2020-07-01 16:00:00+00:00"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [0.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [0.0],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame(
                {
                    "timecol__max": [
                        pandas.Timestamp(1600000000, unit="s", tz=datetime.timezone.utc)
                    ]
                }
            ),
            pandas.DataFrame(
                {
                    "timecol__max": [
                        pandas.Timestamp(2000000000, unit="s", tz=datetime.timezone.utc)
                    ]
                }
            ),
            metadata.RunMetadata(
                validations={
                    "timecol__max": metadata.ValidationMetadata(
                        source_column_name="timecol",
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        target_column_name="timecol",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        validation_type="Column",
                        aggregation_type="max",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: ["timecol"],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: ["timecol"],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["max"],
                    consts.VALIDATION_NAME: ["timecol__max"],
                    consts.SOURCE_AGG_VALUE: ["2020-09-13 12:26:40+00:00"],
                    consts.TARGET_AGG_VALUE: ["2033-05-18 03:33:20+00:00"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [400000000.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [25.0],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_FAIL],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame({"count": [8], "sum__ttteeesssttt": [-1]}),
            pandas.DataFrame({"count": [9], "sum__ttteeesssttt": [1]}),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                    "sum__ttteeesssttt": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="test_col",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="ttteeesssttt_col",
                        validation_type="Column",
                        aggregation_type="sum",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"] * 2,
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)]
                    * 2,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 2,
                    consts.SOURCE_TABLE_NAME: [
                        "bq-public.source_dataset.test_source",
                        "bq-public.source_dataset.test_source",
                    ],
                    consts.SOURCE_COLUMN_NAME: [None, "test_col"],
                    consts.TARGET_TABLE_NAME: [
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                    ],
                    consts.TARGET_COLUMN_NAME: [None, "ttteeesssttt_col"],
                    consts.VALIDATION_TYPE: ["Column", "Column"],
                    consts.AGGREGATION_TYPE: ["count", "sum"],
                    consts.VALIDATION_NAME: ["count", "sum__ttteeesssttt"],
                    consts.SOURCE_AGG_VALUE: ["8", "-1"],
                    consts.TARGET_AGG_VALUE: ["9", "1"],
                    consts.GROUP_BY_COLUMNS: [None, None],
                    consts.CONFIG_PRIMARY_KEYS: [None, None],
                    consts.NUM_RANDOM_ROWS: [None, None],
                    consts.VALIDATION_DIFFERENCE: [1.0, 2.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [12.5, -200.0],
                    consts.VALIDATION_PCT_THRESHOLD: [30.0, 0.0],
                    consts.VALIDATION_STATUS: [
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                    ],
                    consts.CONFIG_LABELS: [[]] * 2,
                }
            ),
        ),
    ),
)
def test_generate_report_without_group_by(
    module_under_test, source_df, target_df, run_metadata, expected
):
    report = module_under_test.generate_report(
        run_metadata,
        source_df,
        target_df,
    )
    # Sort columns by name to order in the comparison.
    # https://stackoverflow.com/a/11067072/101923
    # Sort rows by name to order in the comparison.
    report = (
        report.sort_values(consts.VALIDATION_NAME)
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values(consts.VALIDATION_NAME)
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    pandas.testing.assert_frame_equal(report, expected)


@freeze_time("1998-09-04 07:31:42")
@pytest.mark.parametrize(
    ("source_df", "target_df", "join_on_fields", "run_metadata", "expected"),
    (
        (
            pandas.DataFrame(
                {
                    "count": [2, 4, 8, 16],
                    "grp_a": ["a", "a", "b", "b"],
                    "grp_i": [0, 1, 0, 1],
                }
            ),
            pandas.DataFrame(
                {
                    "count": [1, 3, 7, 17],
                    "grp_a": ["a", "a", "b", "b"],
                    "grp_i": [0, 1, 0, 1],
                }
            ),
            ("grp_a", "grp_i"),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=7.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="grouped-test",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["grouped-test"] * 4,
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)]
                    * 4,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 4,
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"]
                    * 4,
                    consts.SOURCE_COLUMN_NAME: [None] * 4,
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"]
                    * 4,
                    consts.TARGET_COLUMN_NAME: [None] * 4,
                    consts.VALIDATION_TYPE: ["Column"] * 4,
                    consts.AGGREGATION_TYPE: ["count"] * 4,
                    consts.VALIDATION_NAME: ["count"] * 4,
                    consts.SOURCE_AGG_VALUE: ["2", "4", "8", "16"],
                    consts.TARGET_AGG_VALUE: ["1", "3", "7", "17"],
                    consts.GROUP_BY_COLUMNS: [
                        '{"grp_a": "a", "grp_i": "0"}',
                        '{"grp_a": "a", "grp_i": "1"}',
                        '{"grp_a": "b", "grp_i": "0"}',
                        '{"grp_a": "b", "grp_i": "1"}',
                    ],
                    consts.CONFIG_PRIMARY_KEYS: [None] * 4,
                    consts.NUM_RANDOM_ROWS: [None] * 4,
                    consts.VALIDATION_DIFFERENCE: [-1.0, -1.0, -1.0, 1.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [-50.0, -25.0, -12.5, 6.25],
                    consts.VALIDATION_PCT_THRESHOLD: [7.0, 7.0, 7.0, 7.0],
                    consts.VALIDATION_STATUS: [
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                    ],
                    consts.CONFIG_LABELS: [[]] * 4,
                }
            ),
        ),
        (
            pandas.DataFrame({"count": [1, 2], "grp": ['"', "\\"]}),
            pandas.DataFrame({"count": [3, 4], "grp": ['"', "\\"]}),
            ("grp",),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=100.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="grouped-test",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["grouped-test"] * 2,
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)]
                    * 2,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 2,
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"]
                    * 2,
                    consts.SOURCE_COLUMN_NAME: [None] * 2,
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"]
                    * 2,
                    consts.TARGET_COLUMN_NAME: [None] * 2,
                    consts.VALIDATION_TYPE: ["Column"] * 2,
                    consts.AGGREGATION_TYPE: ["count"] * 2,
                    consts.VALIDATION_NAME: ["count"] * 2,
                    consts.SOURCE_AGG_VALUE: ["1", "2"],
                    consts.TARGET_AGG_VALUE: ["3", "4"],
                    consts.GROUP_BY_COLUMNS: ['{"grp": "\\""}', '{"grp": "\\\\"}'],
                    consts.CONFIG_PRIMARY_KEYS: [None] * 2,
                    consts.NUM_RANDOM_ROWS: [None] * 2,
                    consts.VALIDATION_DIFFERENCE: [2.0, 2.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [200.0, 100.0],
                    consts.VALIDATION_PCT_THRESHOLD: [100.0, 100.0],
                    consts.VALIDATION_STATUS: [
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                    ],
                    consts.CONFIG_LABELS: [[]] * 2,
                }
            ),
        ),
        (
            pandas.DataFrame(
                {
                    "count": [2, 4, 6, 8],
                    "grp_a": ["a", "a", "c", "c"],
                    "grp_i": [0, 1, 0, 1],
                }
            ),
            pandas.DataFrame(
                {
                    "count": [1, 3, 5, 7],
                    "grp_a": ["a", "a", "b", "b"],
                    "grp_i": [0, 1, 0, 1],
                }
            ),
            ("grp_a", "grp_i"),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=25.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="grouped-test",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["grouped-test"] * 6,
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)]
                    * 6,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 6,
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"]
                    * 6,
                    consts.SOURCE_COLUMN_NAME: [None] * 6,
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"]
                    * 6,
                    consts.TARGET_COLUMN_NAME: [None] * 6,
                    consts.VALIDATION_TYPE: ["Column"] * 6,
                    consts.AGGREGATION_TYPE: ["count"] * 6,
                    consts.VALIDATION_NAME: ["count"] * 6,
                    consts.SOURCE_AGG_VALUE: ["2", "4", _NAN, _NAN, "6", "8"],
                    consts.TARGET_AGG_VALUE: ["1", "3", "5", "7", _NAN, _NAN],
                    consts.GROUP_BY_COLUMNS: [
                        '{"grp_a": "a", "grp_i": "0"}',
                        '{"grp_a": "a", "grp_i": "1"}',
                        '{"grp_a": "b", "grp_i": "0"}',
                        '{"grp_a": "b", "grp_i": "1"}',
                        '{"grp_a": "c", "grp_i": "0"}',
                        '{"grp_a": "c", "grp_i": "1"}',
                    ],
                    consts.CONFIG_PRIMARY_KEYS: [None] * 6,
                    consts.NUM_RANDOM_ROWS: [None] * 6,
                    consts.VALIDATION_DIFFERENCE: [-1.0, -1.0, _NAN, _NAN, _NAN, _NAN],
                    consts.VALIDATION_PCT_DIFFERENCE: [
                        -50.0,
                        -25.0,
                        _NAN,
                        _NAN,
                        _NAN,
                        _NAN,
                    ],
                    consts.VALIDATION_PCT_THRESHOLD: [
                        25.0,
                        25.0,
                        _NAN,
                        _NAN,
                        _NAN,
                        _NAN,
                    ],
                    consts.VALIDATION_STATUS: [
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                    ],
                    consts.CONFIG_LABELS: [[]] * 6,
                }
            ),
        ),
    ),
)
def test_generate_report_with_group_by(
    module_under_test,
    source_df,
    target_df,
    join_on_fields,
    run_metadata,
    expected,
):
    report = module_under_test.generate_report(
        run_metadata,
        source_df,
        target_df,
        join_on_fields=join_on_fields,
    )
    # Sort columns by name to order in the comparison.
    # https://stackoverflow.com/a/11067072/101923
    # Sort rows by name to order in the comparison.
    report = (
        report.sort_values(consts.VALIDATION_NAME)
        .sort_values(consts.GROUP_BY_COLUMNS)
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values(consts.VALIDATION_NAME)
        .sort_values(consts.GROUP_BY_COLUMNS)
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    pandas.testing.assert_frame_equal(report, expected)


@freeze_time("1998-09-04 07:31:42")
@pytest.mark.parametrize(
    ("source_df", "target_df", "run_metadata", "expected"),
    (
        (
            pandas.DataFrame({"sum": [8093]}),
            pandas.DataFrame({"sum": [_NAN]}),
            metadata.RunMetadata(
                validations={
                    "sum": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="test_col",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="test_col",
                        validation_type="Column",
                        aggregation_type="sum",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: ["test_col"],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: ["test_col"],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["sum"],
                    consts.VALIDATION_NAME: ["sum"],
                    consts.SOURCE_AGG_VALUE: ["8093"],
                    consts.TARGET_AGG_VALUE: ["nan"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_FAIL],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame({"sum": [_NAN]}),
            pandas.DataFrame({"sum": [8093]}),
            metadata.RunMetadata(
                validations={
                    "sum": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="test_col",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="test_col",
                        validation_type="Column",
                        aggregation_type="sum",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: ["test_col"],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: ["test_col"],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["sum"],
                    consts.VALIDATION_NAME: ["sum"],
                    consts.SOURCE_AGG_VALUE: ["nan"],
                    consts.TARGET_AGG_VALUE: ["8093"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_FAIL],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame({"sum": [_NAN]}),
            pandas.DataFrame({"sum": [_NAN]}),
            metadata.RunMetadata(
                validations={
                    "sum": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="test_col",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="test_col",
                        validation_type="Column",
                        aggregation_type="sum",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: ["test_col"],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: ["test_col"],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["sum"],
                    consts.VALIDATION_NAME: ["sum"],
                    consts.SOURCE_AGG_VALUE: ["nan"],
                    consts.TARGET_AGG_VALUE: ["nan"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame({"count": [1]}),
            pandas.DataFrame({"count": [_NAN]}),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"],
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ],
                    consts.SOURCE_TABLE_NAME: ["bq-public.source_dataset.test_source"],
                    consts.SOURCE_COLUMN_NAME: [None],
                    consts.TARGET_TABLE_NAME: ["bq-public.target_dataset.test_target"],
                    consts.TARGET_COLUMN_NAME: [None],
                    consts.VALIDATION_TYPE: ["Column"],
                    consts.AGGREGATION_TYPE: ["count"],
                    consts.VALIDATION_NAME: ["count"],
                    consts.SOURCE_AGG_VALUE: ["1"],
                    consts.TARGET_AGG_VALUE: ["nan"],
                    consts.GROUP_BY_COLUMNS: [None],
                    consts.CONFIG_PRIMARY_KEYS: [None],
                    consts.NUM_RANDOM_ROWS: [None],
                    consts.VALIDATION_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_FAIL],
                    consts.CONFIG_LABELS: [[]],
                }
            ),
        ),
        (
            pandas.DataFrame({"count": [8], "sum__ttteeesssttt": [-1]}),
            pandas.DataFrame({"count": [9], "sum__ttteeesssttt": [_NAN]}),
            metadata.RunMetadata(
                validations={
                    "count": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name=None,
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name=None,
                        validation_type="Column",
                        aggregation_type="count",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                    "sum__ttteeesssttt": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="test_col",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="ttteeesssttt_col",
                        validation_type="Column",
                        aggregation_type="sum",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
                end_time=None,
                labels=[],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    consts.CONFIG_RUN_ID: ["test-run"] * 2,
                    consts.CONFIG_START_TIME: [datetime.datetime(1998, 9, 4, 7, 30, 1)]
                    * 2,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            1998, 9, 4, 7, 31, 42, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 2,
                    consts.SOURCE_TABLE_NAME: [
                        "bq-public.source_dataset.test_source",
                        "bq-public.source_dataset.test_source",
                    ],
                    consts.SOURCE_COLUMN_NAME: [None, "test_col"],
                    consts.TARGET_TABLE_NAME: [
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                    ],
                    consts.TARGET_COLUMN_NAME: [None, "ttteeesssttt_col"],
                    consts.VALIDATION_TYPE: ["Column", "Column"],
                    consts.AGGREGATION_TYPE: ["count", "sum"],
                    consts.VALIDATION_NAME: ["count", "sum__ttteeesssttt"],
                    consts.SOURCE_AGG_VALUE: ["8", "-1"],
                    consts.TARGET_AGG_VALUE: ["9", "nan"],
                    consts.GROUP_BY_COLUMNS: [None, None],
                    consts.CONFIG_PRIMARY_KEYS: [None, None],
                    consts.NUM_RANDOM_ROWS: [None, None],
                    consts.VALIDATION_DIFFERENCE: [1.0, _NAN],
                    consts.VALIDATION_PCT_DIFFERENCE: [12.5, _NAN],
                    consts.VALIDATION_PCT_THRESHOLD: [30.0, 0.0],
                    consts.VALIDATION_STATUS: [
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                    ],
                    consts.CONFIG_LABELS: [[]] * 2,
                }
            ),
        ),
    ),
)
def test_generate_report_with_nan_agg_value(
    module_under_test, source_df, target_df, run_metadata, expected
):
    report = module_under_test.generate_report(
        run_metadata,
        source_df,
        target_df,
    )
    # Sort columns by name to order in the comparison.
    # https://stackoverflow.com/a/11067072/101923
    # Sort rows by name to order in the comparison.
    report = (
        report.sort_values(consts.VALIDATION_NAME)
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values(consts.VALIDATION_NAME)
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    pandas.testing.assert_frame_equal(report, expected)


@pytest.mark.parametrize(
    ("run_metadata", "result_df", "source_df", "target_df", "expected"),
    (
        (
            metadata.RunMetadata(
                run_id="test1-run",
                start_time=datetime.datetime(
                    2025, 2, 12, 7, 30, 10, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2025, 2, 12, 7, 32, 15, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_TYPE: [consts.ROW_VALIDATION] * 5,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 2
                    + [consts.VALIDATION_STATUS_FAIL] * 3,
                    consts.GROUP_BY_COLUMNS: [
                        {"id": "1"},
                        {"id": "2"},
                        {"id": "3"},
                        {"id": "4"},
                        {"id": "8"},
                    ],
                    consts.SOURCE_AGG_VALUE: [10, 20, 30, 40, None],
                    consts.TARGET_AGG_VALUE: [10, 20, 60, None, 80],
                }
            ),
            pandas.DataFrame({"id": [1, 2, 3, 4], "value": [10, 20, 30, 40]}),
            pandas.DataFrame({"id": [1, 2, 3, 8], "value": [10, 20, 60, 80]}),
            {
                consts.CONFIG_RUN_ID: "test1-run",
                consts.CONFIG_START_TIME: "2025-02-12T07:30:10+00:00",
                consts.CONFIG_END_TIME: "2025-02-12T07:32:15+00:00",
                consts.TOTAL_SOURCE_ROWS: 4,
                consts.TOTAL_TARGET_ROWS: 4,
                consts.TOTAL_ROWS_VALIDATED: 5,
                consts.TOTAL_ROWS_SUCCESS: 2,
                # ids 3, 4, 8 got failed validation status
                consts.TOTAL_ROWS_FAIL: 3,
                # id 4 present only in source
                consts.FAILED_SOURCE_NOT_IN_TARGET: 1,
                # id 8 present only in target
                consts.FAILED_TARGET_NOT_IN_SOURCE: 1,
                # id 3 present in both source and target but value is different
                consts.FAILED_PRESENT_IN_BOTH_TABLES: 1,
            },
        ),
        (
            metadata.RunMetadata(
                run_id="test2-run",
                start_time=datetime.datetime(
                    2025, 3, 6, 5, 30, 10, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2025, 3, 6, 5, 32, 15, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_TYPE: [consts.CUSTOM_QUERY] * 2,
                    consts.CONFIG_PRIMARY_KEYS: ["{id}"] * 2,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 2,
                    consts.GROUP_BY_COLUMNS: [
                        {"id": "5"},
                        {"id": "6"},
                    ],
                    consts.SOURCE_AGG_VALUE: [50, 60],
                    consts.TARGET_AGG_VALUE: [50, 60],
                }
            ),
            pandas.DataFrame({"id": [5, 6], "value": [50, 60]}),
            pandas.DataFrame({"id": [5, 6], "value": [50, 60]}),
            {
                consts.CONFIG_RUN_ID: "test2-run",
                consts.CONFIG_START_TIME: "2025-03-06T05:30:10+00:00",
                consts.CONFIG_END_TIME: "2025-03-06T05:32:15+00:00",
                consts.TOTAL_SOURCE_ROWS: 2,
                consts.TOTAL_TARGET_ROWS: 2,
                consts.TOTAL_ROWS_VALIDATED: 2,
                consts.TOTAL_ROWS_SUCCESS: 2,
                consts.TOTAL_ROWS_FAIL: 0,
                consts.FAILED_SOURCE_NOT_IN_TARGET: 0,
                consts.FAILED_TARGET_NOT_IN_SOURCE: 0,
                consts.FAILED_PRESENT_IN_BOTH_TABLES: 0,
            },
        ),
    ),
)
def test_get_summary_with_values_for_all_stats(
    module_under_test, caplog, run_metadata, result_df, source_df, target_df, expected
):
    caplog.set_level(logging.INFO)
    module_under_test._get_summary(run_metadata, result_df, source_df, target_df)

    logged = caplog.records[0]  # assuming only one log message
    assert logged.levelname == "INFO"
    assert logged.message == json.dumps(expected)
    assert all(
        module_under_test.COMBINER_GET_SUMMARY_EXC_TEXT not in _.message
        for _ in caplog.records
    )


@pytest.mark.parametrize(
    ("run_metadata", "result_df", "source_df", "target_df"),
    (
        (
            metadata.RunMetadata(
                run_id="test-run",
                start_time=datetime.datetime(
                    2025, 2, 12, 7, 30, 10, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2025, 2, 12, 7, 32, 15, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_TYPE: [],
                    consts.VALIDATION_STATUS: [],
                    consts.GROUP_BY_COLUMNS: [],
                    consts.SOURCE_AGG_VALUE: [],
                    consts.TARGET_AGG_VALUE: [],
                }
            ),
            pandas.DataFrame({"id": [], "value": []}),
            pandas.DataFrame({"id": [], "value": []}),
        ),
    ),
)
def test_get_summary_with_empty_inputs(
    module_under_test, caplog, run_metadata, result_df, source_df, target_df
):
    caplog.set_level(logging.INFO)
    module_under_test._get_summary(run_metadata, result_df, source_df, target_df)
    assert all(
        module_under_test.COMBINER_GET_SUMMARY_EXC_TEXT not in _.message
        for _ in caplog.records
    )
