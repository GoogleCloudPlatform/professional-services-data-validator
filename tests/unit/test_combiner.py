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

import ibis.backends.pandas
import pandas
import pandas.testing
import pytest

from data_validation import metadata, consts


_NAN = float("nan")
FAKE_TIME = datetime.datetime(1998, 9, 4, 7, 31, 42)

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
    labels=[("name", "test_label")],
    run_id="test-run",
)


@pytest.fixture
def module_under_test():
    from data_validation import combiner

    return combiner


@pytest.fixture
def patch_datetime_now(monkeypatch):
    class mydatetime:
        @classmethod
        def now(cls, utc):
            return FAKE_TIME

    monkeypatch.setattr(datetime, "datetime", mydatetime)


def test_generate_report_with_different_columns(module_under_test):
    source = pandas.DataFrame({"count": [1], "sum": [3]})
    target = pandas.DataFrame({"count": [2]})
    pandas_client = ibis.backends.pandas.connect(
        {
            module_under_test.DEFAULT_SOURCE: source,
            module_under_test.DEFAULT_TARGET: target,
        }
    )
    with pytest.raises(
        ValueError, match="Expected source and target to have same schema"
    ):
        module_under_test.generate_report(
            pandas_client,
            # Schema validation occurs before run_metadata is needed.
            None,
            source=pandas_client.table(module_under_test.DEFAULT_SOURCE),
            target=pandas_client.table(module_under_test.DEFAULT_TARGET),
        )


def test_generate_report_with_too_many_rows(module_under_test):
    source = pandas.DataFrame({"count": [1, 1]})
    target = pandas.DataFrame({"count": [2, 2]})
    pandas_client = ibis.backends.pandas.connect(
        {
            module_under_test.DEFAULT_SOURCE: source,
            module_under_test.DEFAULT_TARGET: target,
        }
    )

    report = module_under_test.generate_report(
        pandas_client,
        # Validation occurs before run_metadata is needed.
        EXAMPLE_RUN_METADATA,
        source=pandas_client.table(module_under_test.DEFAULT_SOURCE),
        target=pandas_client.table(module_under_test.DEFAULT_TARGET),
    )

    # TODO: how do we want to handle this going forward?
    assert len(report) == 16


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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": [None],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": [None],
                    "validation_type": ["Column"],
                    "aggregation_type": ["count"],
                    "validation_name": ["count"],
                    "source_agg_value": ["1"],
                    "target_agg_value": ["2"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [1.0],
                    "pct_difference": [100.0],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_FAIL],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": ["timecol"],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": ["timecol"],
                    "validation_type": ["Column"],
                    "aggregation_type": ["max"],
                    "validation_name": ["timecol__max"],
                    "source_agg_value": ["2020-07-01 16:00:00+00:00"],
                    "target_agg_value": ["2020-07-01 16:00:00+00:00"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [0.0],
                    "pct_difference": [0.0],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_SUCCESS],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": ["timecol"],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": ["timecol"],
                    "validation_type": ["Column"],
                    "aggregation_type": ["max"],
                    "validation_name": ["timecol__max"],
                    "source_agg_value": ["2020-09-13 12:26:40+00:00"],
                    "target_agg_value": ["2033-05-18 03:33:20+00:00"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [400000000.0],
                    "pct_difference": [25.0],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_FAIL],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"] * 2,
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)] * 2,
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)] * 2,
                    "source_table_name": [
                        "bq-public.source_dataset.test_source",
                        "bq-public.source_dataset.test_source",
                    ],
                    "source_column_name": [None, "test_col"],
                    "target_table_name": [
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                    ],
                    "target_column_name": [None, "ttteeesssttt_col"],
                    "validation_type": ["Column", "Column"],
                    "aggregation_type": ["count", "sum"],
                    "validation_name": ["count", "sum__ttteeesssttt"],
                    "source_agg_value": ["8", "-1"],
                    "target_agg_value": ["9", "1"],
                    "group_by_columns": [None, None],
                    "primary_keys": [None, None],
                    "num_random_rows": [None, None],
                    "difference": [1.0, 2.0],
                    "pct_difference": [12.5, -200.0],
                    "pct_threshold": [30.0, 0.0],
                    "validation_status": [
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                    ],
                    "labels": [[("name", "test_label")]] * 2,
                }
            ),
        ),
    ),
)
def test_generate_report_without_group_by(
    module_under_test, patch_datetime_now, source_df, target_df, run_metadata, expected
):
    pandas_client = ibis.backends.pandas.connect(
        {"test_source": source_df, "test_target": target_df}
    )
    report = module_under_test.generate_report(
        pandas_client,
        run_metadata,
        source=pandas_client.table("test_source"),
        target=pandas_client.table("test_target"),
    )
    # Sort columns by name to order in the comparison.
    # https://stackoverflow.com/a/11067072/101923
    # Sort rows by name to order in the comparison.
    report = (
        report.sort_values("validation_name")
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values("validation_name")
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    pandas.testing.assert_frame_equal(report, expected)


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
                labels=[("name", "group_label")],
                run_id="grouped-test",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["grouped-test"] * 4,
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)] * 4,
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)] * 4,
                    "source_table_name": ["bq-public.source_dataset.test_source"] * 4,
                    "source_column_name": [None] * 4,
                    "target_table_name": ["bq-public.target_dataset.test_target"] * 4,
                    "target_column_name": [None] * 4,
                    "validation_type": ["Column"] * 4,
                    "aggregation_type": ["count"] * 4,
                    "validation_name": ["count"] * 4,
                    "source_agg_value": ["2", "4", "8", "16"],
                    "target_agg_value": ["1", "3", "7", "17"],
                    "group_by_columns": [
                        '{"grp_a": "a", "grp_i": "0"}',
                        '{"grp_a": "a", "grp_i": "1"}',
                        '{"grp_a": "b", "grp_i": "0"}',
                        '{"grp_a": "b", "grp_i": "1"}',
                    ],
                    "primary_keys": [None] * 4,
                    "num_random_rows": [None] * 4,
                    "difference": [-1.0, -1.0, -1.0, 1.0],
                    "pct_difference": [-50.0, -25.0, -12.5, 6.25],
                    "pct_threshold": [7.0, 7.0, 7.0, 7.0],
                    "validation_status": [
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                    ],
                    "labels": [[("name", "group_label")]] * 4,
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
                labels=[("name", "group_label")],
                run_id="grouped-test",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["grouped-test"] * 2,
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)] * 2,
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)] * 2,
                    "source_table_name": ["bq-public.source_dataset.test_source"] * 2,
                    "source_column_name": [None] * 2,
                    "target_table_name": ["bq-public.target_dataset.test_target"] * 2,
                    "target_column_name": [None] * 2,
                    "validation_type": ["Column"] * 2,
                    "aggregation_type": ["count"] * 2,
                    "validation_name": ["count"] * 2,
                    "source_agg_value": ["1", "2"],
                    "target_agg_value": ["3", "4"],
                    "group_by_columns": ['{"grp": "\\""}', '{"grp": "\\\\"}'],
                    "primary_keys": [None] * 2,
                    "num_random_rows": [None] * 2,
                    "difference": [2.0, 2.0],
                    "pct_difference": [200.0, 100.0],
                    "pct_threshold": [100.0, 100.0],
                    "validation_status": [
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                    ],
                    "labels": [[("name", "group_label")]] * 2,
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
                labels=[("name", "group_label")],
                run_id="grouped-test",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["grouped-test"] * 6,
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)] * 6,
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)] * 6,
                    "source_table_name": [
                        "bq-public.source_dataset.test_source",
                        "bq-public.source_dataset.test_source",
                        _NAN,
                        _NAN,
                        "bq-public.source_dataset.test_source",
                        "bq-public.source_dataset.test_source",
                    ],
                    "source_column_name": [None] * 6,
                    "target_table_name": [
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                        _NAN,
                        _NAN,
                    ],
                    "target_column_name": [None] * 6,
                    "validation_type": ["Column"] * 6,
                    "aggregation_type": ["count"] * 6,
                    "validation_name": ["count"] * 6,
                    "source_agg_value": ["2", "4", _NAN, _NAN, "6", "8"],
                    "target_agg_value": ["1", "3", "5", "7", _NAN, _NAN],
                    "group_by_columns": [
                        '{"grp_a": "a", "grp_i": "0"}',
                        '{"grp_a": "a", "grp_i": "1"}',
                        '{"grp_a": "b", "grp_i": "0"}',
                        '{"grp_a": "b", "grp_i": "1"}',
                        '{"grp_a": "c", "grp_i": "0"}',
                        '{"grp_a": "c", "grp_i": "1"}',
                    ],
                    "primary_keys": [None] * 6,
                    "num_random_rows": [None] * 6,
                    "difference": [-1.0, -1.0, _NAN, _NAN, _NAN, _NAN],
                    "pct_difference": [-50.0, -25.0, _NAN, _NAN, _NAN, _NAN],
                    "pct_threshold": [25.0, 25.0, _NAN, _NAN, _NAN, _NAN],
                    "validation_status": [
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                    ],
                    "labels": [[("name", "group_label")]] * 6,
                }
            ),
        ),
    ),
)
def test_generate_report_with_group_by(
    module_under_test,
    patch_datetime_now,
    source_df,
    target_df,
    join_on_fields,
    run_metadata,
    expected,
):
    pandas_client = ibis.backends.pandas.connect(
        {"test_source": source_df, "test_target": target_df}
    )
    report = module_under_test.generate_report(
        pandas_client,
        run_metadata,
        join_on_fields=join_on_fields,
        source=pandas_client.table("test_source"),
        target=pandas_client.table("test_target"),
    )
    # Sort columns by name to order in the comparison.
    # https://stackoverflow.com/a/11067072/101923
    # Sort rows by name to order in the comparison.
    report = (
        report.sort_values("validation_name")
        .sort_values("group_by_columns")
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values("validation_name")
        .sort_values("group_by_columns")
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    pandas.testing.assert_frame_equal(report, expected)


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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": ["test_col"],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": ["test_col"],
                    "validation_type": ["Column"],
                    "aggregation_type": ["sum"],
                    "validation_name": ["sum"],
                    "source_agg_value": ["8093"],
                    "target_agg_value": ["nan"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [_NAN],
                    "pct_difference": [_NAN],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_FAIL],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": ["test_col"],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": ["test_col"],
                    "validation_type": ["Column"],
                    "aggregation_type": ["sum"],
                    "validation_name": ["sum"],
                    "source_agg_value": ["nan"],
                    "target_agg_value": ["8093"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [_NAN],
                    "pct_difference": [_NAN],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_FAIL],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": ["test_col"],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": ["test_col"],
                    "validation_type": ["Column"],
                    "aggregation_type": ["sum"],
                    "validation_name": ["sum"],
                    "source_agg_value": ["nan"],
                    "target_agg_value": ["nan"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [_NAN],
                    "pct_difference": [_NAN],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_SUCCESS],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"],
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)],
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)],
                    "source_table_name": ["bq-public.source_dataset.test_source"],
                    "source_column_name": [None],
                    "target_table_name": ["bq-public.target_dataset.test_target"],
                    "target_column_name": [None],
                    "validation_type": ["Column"],
                    "aggregation_type": ["count"],
                    "validation_name": ["count"],
                    "source_agg_value": ["1"],
                    "target_agg_value": ["nan"],
                    "group_by_columns": [None],
                    "primary_keys": [None],
                    "num_random_rows": [None],
                    "difference": [_NAN],
                    "pct_difference": [_NAN],
                    "pct_threshold": [0.0],
                    "validation_status": [consts.VALIDATION_STATUS_FAIL],
                    "labels": [[("name", "test_label")]],
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
                labels=[("name", "test_label")],
                run_id="test-run",
            ),
            pandas.DataFrame(
                {
                    "run_id": ["test-run"] * 2,
                    "start_time": [datetime.datetime(1998, 9, 4, 7, 30, 1)] * 2,
                    "end_time": [datetime.datetime(1998, 9, 4, 7, 31, 42)] * 2,
                    "source_table_name": [
                        "bq-public.source_dataset.test_source",
                        "bq-public.source_dataset.test_source",
                    ],
                    "source_column_name": [None, "test_col"],
                    "target_table_name": [
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                    ],
                    "target_column_name": [None, "ttteeesssttt_col"],
                    "validation_type": ["Column", "Column"],
                    "aggregation_type": ["count", "sum"],
                    "validation_name": ["count", "sum__ttteeesssttt"],
                    "source_agg_value": ["8", "-1"],
                    "target_agg_value": ["9", "nan"],
                    "group_by_columns": [None, None],
                    "primary_keys": [None, None],
                    "num_random_rows": [None, None],
                    "difference": [1.0, _NAN],
                    "pct_difference": [12.5, _NAN],
                    "pct_threshold": [30.0, 0.0],
                    "validation_status": [
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                    ],
                    "labels": [[("name", "test_label")]] * 2,
                }
            ),
        ),
    ),
)
def test_generate_report_with_nan_agg_value(
    module_under_test, patch_datetime_now, source_df, target_df, run_metadata, expected
):
    pandas_client = ibis.backends.pandas.connect(
        {"test_source": source_df, "test_target": target_df}
    )
    report = module_under_test.generate_report(
        pandas_client,
        run_metadata,
        source=pandas_client.table("test_source"),
        target=pandas_client.table("test_target"),
    )
    # Sort columns by name to order in the comparison.
    # https://stackoverflow.com/a/11067072/101923
    # Sort rows by name to order in the comparison.
    report = (
        report.sort_values("validation_name")
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values("validation_name")
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    pandas.testing.assert_frame_equal(report, expected)
