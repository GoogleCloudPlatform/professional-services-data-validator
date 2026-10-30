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
import decimal
import json
import logging

import pandas
import pandas.testing
import pyarrow
import pytest

from freezegun import freeze_time
from data_validation import metadata, consts

# Ensure Pandas is patched as it would be in a true DVT execution.
import third_party.ibis.ibis_addon.operations  # noqa: F401

_NAN = float("nan")


def _assert_frame_equal(report, expected):
    with pandas.option_context("future.no_silent_downcasting", True):
        report_filled = report.fillna(value=_NAN)
        expected_filled = expected.fillna(value=_NAN)
    pandas.testing.assert_frame_equal(report_filled, expected_filled)


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
    return pyarrow.table(data)


def test_generate_report_with_different_columns(module_under_test):
    source = pyarrow.table({"count": [1], "sum": [3]})
    target = pyarrow.table({"count": [2]})
    with pytest.raises(
        ValueError, match="Expected source and target to have same schema"
    ):
        module_under_test.generate_report(
            # Schema validation occurs before run_metadata is needed.
            None,
            source,
            target,
        )


def test_generate_report_with_arrow_decimal(module_under_test):
    decimal_type = pyarrow.decimal128(38, 0)
    source = pyarrow.table(
        {
            "count": pyarrow.array(
                [decimal.Decimal("100000000000000000000")], type=decimal_type
            )
        }
    )
    target = pyarrow.table(
        {
            "count": pyarrow.array(
                [decimal.Decimal("100000000000000000000")], type=decimal_type
            )
        }
    )

    report = module_under_test.generate_report(EXAMPLE_RUN_METADATA, source, target)

    assert report.loc[0, consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_SUCCESS


@pytest.mark.parametrize(
    ("source_value", "target_value", "arrow_type", "threshold", "expected_status"),
    [
        # Regression test for issue-1007, values differ beyond float64 precision.
        (
            decimal.Decimal("4938271605061728382"),
            decimal.Decimal("4938271605061728390"),
            pyarrow.decimal128(38, 9),
            0.0,
            consts.VALIDATION_STATUS_FAIL,
        ),
        (
            4938271605061728382,
            4938271605061728390,
            pyarrow.int64(),
            0.0,
            consts.VALIDATION_STATUS_FAIL,
        ),
        (
            decimal.Decimal("61728394506172839450617283945061728390"),
            decimal.Decimal("61728394506172839450617283945061728391"),
            pyarrow.decimal256(76, 38),
            0.0,
            consts.VALIDATION_STATUS_FAIL,
        ),
        (
            decimal.Decimal("61728394506172839450617283945061728390"),
            decimal.Decimal("61728394506172839450617283945061728390"),
            pyarrow.decimal256(76, 38),
            0.0,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        # A non-zero threshold uses the percentage difference, these values are well within it.
        (
            decimal.Decimal("4938271605061728382"),
            decimal.Decimal("4938271605061728390"),
            pyarrow.decimal128(38, 9),
            1.0,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        # Difference > float32 max (~3.40e38) must not overflow pct_difference to inf.
        (
            decimal.Decimal("5" + "0" * 39),
            decimal.Decimal("51" + "0" * 38),
            pyarrow.decimal256(45, 0),
            5.0,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            1e307,
            1.02e307,
            pyarrow.float64(),
            5.0,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        # Tiny float64 difference (< float32 subnormal min ~1.4e-45) must not underflow to 0.0.
        (
            1e-46,
            9e-46,
            pyarrow.float64(),
            0.0,
            consts.VALIDATION_STATUS_FAIL,
        ),
    ],
)
def test_generate_report_with_high_precision_aggs(
    module_under_test,
    source_value,
    target_value,
    arrow_type,
    threshold,
    expected_status,
):
    run_metadata = metadata.RunMetadata(
        validations={
            "sum__col": metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="source_dataset",
                source_column_name="col",
                target_table_name="test_target",
                target_table_schema="target_dataset",
                target_column_name="col",
                validation_type="Column",
                aggregation_type="sum",
                primary_keys=[],
                num_random_rows=None,
                threshold=threshold,
            ),
        },
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=None,
        labels=[],
        run_id="test-run",
    )
    source = pyarrow.table({"sum__col": pyarrow.array([source_value], type=arrow_type)})
    target = pyarrow.table({"sum__col": pyarrow.array([target_value], type=arrow_type)})

    report = module_under_test.generate_report(run_metadata, source, target)

    assert report.loc[0, consts.VALIDATION_STATUS] == expected_status
    if pyarrow.types.is_integer(arrow_type) or pyarrow.types.is_decimal(arrow_type):
        assert report.loc[0, consts.SOURCE_AGG_VALUE].startswith(str(source_value))
        assert report.loc[0, consts.TARGET_AGG_VALUE].startswith(str(target_value))


@pytest.mark.parametrize(
    ("source_array", "target_array", "expected_status"),
    [
        # Oracle -> BigQuery issue-1007 regression, common type needs precision 45.
        (
            pyarrow.array(
                [decimal.Decimal("4938271605061728383.00")],
                type=pyarrow.decimal128(38, 2),
            ),
            pyarrow.array(
                [decimal.Decimal("4938271605061728382.000000000")],
                type=pyarrow.decimal128(38, 9),
            ),
            consts.VALIDATION_STATUS_FAIL,
        ),
        (
            pyarrow.array(
                [decimal.Decimal("4938271605061728382.00")],
                type=pyarrow.decimal128(38, 2),
            ),
            pyarrow.array(
                [decimal.Decimal("4938271605061728382.000000000")],
                type=pyarrow.decimal128(38, 9),
            ),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            pyarrow.array([4938271605061728382], type=pyarrow.int64()),
            pyarrow.array(
                [decimal.Decimal("4938271605061728382")],
                type=pyarrow.decimal128(38, 9),
            ),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            pyarrow.array(
                [decimal.Decimal("12345678901234567890.5")],
                type=pyarrow.decimal128(38, 9),
            ),
            pyarrow.array(
                [decimal.Decimal("12345678901234567890.5")],
                type=pyarrow.decimal256(76, 38),
            ),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        # >76 combined precision: Oracle SUM(NUMBER(38, 0)) decimal256(39, 0) vs
        # BigQuery BIGNUMERIC decimal256(76, 38) needs 39 + 38 = 77 digits.
        (
            pyarrow.array(
                [decimal.Decimal("12345678901234567890123456789012345678")],
                type=pyarrow.decimal256(39, 0),
            ),
            pyarrow.array(
                [decimal.Decimal("12345678901234567890123456789012345678")],
                type=pyarrow.decimal256(76, 38),
            ),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            pyarrow.array(
                [decimal.Decimal("123456789012345678901234567890123456789")],
                type=pyarrow.decimal256(39, 0),
            ),
            pyarrow.array(
                [decimal.Decimal("99999999999999999999999999999999999999")],
                type=pyarrow.decimal256(76, 38),
            ),
            consts.VALIDATION_STATUS_FAIL,
        ),
        # >76 combined precision: decimal256(76, 0) vs decimal128(38, 9) needs 85 digits.
        (
            pyarrow.array(
                [decimal.Decimal("42")],
                type=pyarrow.decimal256(76, 0),
            ),
            pyarrow.array(
                [decimal.Decimal("42.000000000")],
                type=pyarrow.decimal128(38, 9),
            ),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        # >76 fallback preserves Arrow 3-valued NULL semantics (None vs value -> fail).
        (
            pyarrow.array(
                [None],
                type=pyarrow.decimal256(39, 0),
            ),
            pyarrow.array(
                [decimal.Decimal("1")],
                type=pyarrow.decimal256(76, 38),
            ),
            consts.VALIDATION_STATUS_FAIL,
        ),
        # uint64 > 2^63 - 1 vs int64 / decimal without ArrowInvalid overflow.
        (
            pyarrow.array([2**63 + 5], type=pyarrow.uint64()),
            pyarrow.array([5], type=pyarrow.int64()),
            consts.VALIDATION_STATUS_FAIL,
        ),
        (
            pyarrow.array([2**63 + 5], type=pyarrow.uint64()),
            pyarrow.array(
                [decimal.Decimal(2**63 + 5)],
                type=pyarrow.decimal128(20, 0),
            ),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            pyarrow.array([42], type=pyarrow.uint64()),
            pyarrow.array([42], type=pyarrow.int64()),
            consts.VALIDATION_STATUS_SUCCESS,
        ),
    ],
)
def test_generate_report_with_mixed_exact_types(
    module_under_test, source_array, target_array, expected_status
):
    source = pyarrow.table({"count": source_array})
    target = pyarrow.table({"count": target_array})

    report = module_under_test.generate_report(EXAMPLE_RUN_METADATA, source, target)

    assert report.loc[0, consts.VALIDATION_STATUS] == expected_status


def test_generate_report_row_validation_with_mixed_exact_types(module_under_test):
    """Row validation (--comp-fields) should use _exact_equal for exact types."""
    source_table = pyarrow.table(
        {
            "id": [1, 2, 3, 4],
            "dec_scale_mix": pyarrow.array(
                [
                    decimal.Decimal("4938271605061728382.00"),
                    decimal.Decimal("4938271605061728383.00"),
                    None,
                    decimal.Decimal("1.00"),
                ],
                type=pyarrow.decimal128(38, 2),
            ),
            "dec_over_76": pyarrow.array(
                [
                    decimal.Decimal("12345678901234567890123456789012345678"),
                    decimal.Decimal("123456789012345678901234567890123456789"),
                    None,
                    None,
                ],
                type=pyarrow.decimal256(39, 0),
            ),
            "uint_vs_int": pyarrow.array(
                [42, 2**63 + 5, 10, None], type=pyarrow.uint64()
            ),
        }
    )
    target_table = pyarrow.table(
        {
            "id": [1, 2, 3, 4],
            "dec_scale_mix": pyarrow.array(
                [
                    decimal.Decimal("4938271605061728382.000000000"),
                    decimal.Decimal("4938271605061728382.000000000"),
                    decimal.Decimal("1.000000000"),
                    decimal.Decimal("1.000000000"),
                ],
                type=pyarrow.decimal128(38, 9),
            ),
            "dec_over_76": pyarrow.array(
                [
                    decimal.Decimal("12345678901234567890123456789012345678"),
                    decimal.Decimal("99999999999999999999999999999999999999"),
                    decimal.Decimal("1"),
                    None,
                ],
                type=pyarrow.decimal256(76, 38),
            ),
            "uint_vs_int": pyarrow.array([42, 5, 10, None], type=pyarrow.int64()),
        }
    )
    run_metadata = metadata.RunMetadata(
        validations={
            col: metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="source_schema",
                source_column_name=col,
                target_table_name="test_target",
                target_table_schema="target_schema",
                target_column_name=col,
                validation_type="Row",
                aggregation_type=None,
                primary_keys=["id"],
                num_random_rows=None,
                threshold=0.0,
            )
            for col in ("dec_scale_mix", "dec_over_76", "uint_vs_int")
        },
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=datetime.datetime(1998, 9, 4, 7, 31, 42),
        labels=[],
        run_id="test-run",
    )

    report = module_under_test.generate_report(
        run_metadata,
        source_table,
        target_table,
        join_on_fields=("id",),
        is_value_comparison=True,
    )

    by_col_and_id = {
        (
            row[consts.VALIDATION_NAME],
            json.loads(row[consts.GROUP_BY_COLUMNS])["id"],
        ): row[consts.VALIDATION_STATUS]
        for _, row in report.iterrows()
    }
    assert by_col_and_id[("dec_scale_mix", "1")] == consts.VALIDATION_STATUS_SUCCESS
    assert by_col_and_id[("dec_scale_mix", "2")] == consts.VALIDATION_STATUS_FAIL
    assert by_col_and_id[("dec_scale_mix", "3")] == consts.VALIDATION_STATUS_FAIL
    assert by_col_and_id[("dec_scale_mix", "4")] == consts.VALIDATION_STATUS_SUCCESS

    assert by_col_and_id[("dec_over_76", "1")] == consts.VALIDATION_STATUS_SUCCESS
    assert by_col_and_id[("dec_over_76", "2")] == consts.VALIDATION_STATUS_FAIL
    assert by_col_and_id[("dec_over_76", "3")] == consts.VALIDATION_STATUS_FAIL
    # Note: NULL vs NULL currently evaluates to fail until Open Problem #10 is resolved;
    # this asserts the >76 Python fallback matches pc.equal's 3-valued NULL behavior.
    assert by_col_and_id[("dec_over_76", "4")] == consts.VALIDATION_STATUS_FAIL

    assert by_col_and_id[("uint_vs_int", "1")] == consts.VALIDATION_STATUS_SUCCESS
    assert by_col_and_id[("uint_vs_int", "2")] == consts.VALIDATION_STATUS_FAIL
    assert by_col_and_id[("uint_vs_int", "3")] == consts.VALIDATION_STATUS_SUCCESS
    assert by_col_and_id[("uint_vs_int", "4")] == consts.VALIDATION_STATUS_FAIL


@pytest.mark.parametrize("aggregation_type", ["avg", "std"])
def test_generate_report_with_approximate_decimal_aggs(
    module_under_test, aggregation_type
):
    """avg/std decimals returned with different scales should not be compared exactly.

    e.g. Oracle returns STDDEV as decimal256(40, 40), BigQuery as decimal128(38, 9).
    """
    run_metadata = metadata.RunMetadata(
        validations={
            f"{aggregation_type}__col": metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="source_dataset",
                source_column_name="col",
                target_table_name="test_target",
                target_table_schema="target_dataset",
                target_column_name="col",
                validation_type="Column",
                aggregation_type=aggregation_type,
                primary_keys=[],
                num_random_rows=None,
                threshold=0.0,
            ),
        },
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=None,
        labels=[],
        run_id="test-run",
    )
    source = pyarrow.table(
        {
            f"{aggregation_type}__col": pyarrow.array(
                [decimal.Decimal("0.0953939201416945649152621586023226540255")],
                type=pyarrow.decimal256(40, 40),
            )
        }
    )
    target = pyarrow.table(
        {
            f"{aggregation_type}__col": pyarrow.array(
                [decimal.Decimal("0.095393920")], type=pyarrow.decimal128(38, 9)
            )
        }
    )

    report = module_under_test.generate_report(run_metadata, source, target)

    assert report.loc[0, consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_SUCCESS


def _grouped_count_run_metadata():
    return metadata.RunMetadata(
        validations={
            "count": metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="source_dataset",
                source_column_name=None,
                target_table_name="test_target",
                target_table_schema="target_dataset",
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
    )


@pytest.mark.parametrize(
    ("source_table", "target_table", "join_on_fields", "expected"),
    [
        # NULL group present in both source and target must match.
        (
            pyarrow.table({"grp": [1, None, 2], "count": [5, 3, 7]}),
            pyarrow.table({"grp": [None, 2, 1], "count": [3, 7, 5]}),
            ("grp",),
            {
                '{"grp": "1"}': ("5", "5", consts.VALIDATION_STATUS_SUCCESS),
                '{"grp": "2"}': ("7", "7", consts.VALIDATION_STATUS_SUCCESS),
                '{"grp": null}': ("3", "3", consts.VALIDATION_STATUS_SUCCESS),
            },
        ),
        # NULL group only in source, sentinel fill value (1) must not match the real group 1.
        (
            pyarrow.table({"grp": [1, None], "count": [5, 3]}),
            pyarrow.table({"grp": [1], "count": [5]}),
            ("grp",),
            {
                '{"grp": "1"}': ("5", "5", consts.VALIDATION_STATUS_SUCCESS),
                '{"grp": null}': ("3", None, consts.VALIDATION_STATUS_FAIL),
            },
        ),
        # Composite key with a NULL in one part only.
        (
            pyarrow.table(
                {"a": ["x", "x", None], "b": [1, None, None], "count": [1, 2, 3]}
            ),
            pyarrow.table(
                {"a": [None, "x", "x"], "b": [None, None, 1], "count": [3, 2, 1]}
            ),
            ("a", "b"),
            {
                '{"a": "x", "b": "1"}': ("1", "1", consts.VALIDATION_STATUS_SUCCESS),
                '{"a": "x", "b": null}': ("2", "2", consts.VALIDATION_STATUS_SUCCESS),
                '{"a": null, "b": null}': ("3", "3", consts.VALIDATION_STATUS_SUCCESS),
            },
        ),
        # Key entirely NULL on both sides.
        (
            pyarrow.table(
                {"grp": pyarrow.array([None], type=pyarrow.date32()), "count": [4]}
            ),
            pyarrow.table(
                {"grp": pyarrow.array([None], type=pyarrow.date32()), "count": [4]}
            ),
            ("grp",),
            {'{"grp": null}': ("4", "4", consts.VALIDATION_STATUS_SUCCESS)},
        ),
    ],
)
def test_generate_report_with_null_group_keys(
    module_under_test, source_table, target_table, join_on_fields, expected
):
    report = module_under_test.generate_report(
        _grouped_count_run_metadata(),
        source_table,
        target_table,
        join_on_fields=join_on_fields,
    )
    actual = {
        row[consts.GROUP_BY_COLUMNS]: (
            row[consts.SOURCE_AGG_VALUE],
            row[consts.TARGET_AGG_VALUE],
            row[consts.VALIDATION_STATUS],
        )
        for row in report.to_dict(orient="records")
    }
    assert len(report) == len(expected)
    for group, (src, tgt, status) in expected.items():
        assert actual[group][2] == status
        assert actual[group][0] == src
        if tgt is None:
            assert actual[group][1] in (None, "nan")
        else:
            assert actual[group][1] == tgt


@pytest.mark.parametrize(
    ("source_key", "target_key"),
    [
        # Oracle NUMBER(5) vs BigQuery INT64.
        (
            pyarrow.array(
                [decimal.Decimal(1), decimal.Decimal(2), None], pyarrow.decimal128(5, 0)
            ),
            pyarrow.array([2, 1, None], pyarrow.int64()),
        ),
        (
            pyarrow.array([1, 2, None], pyarrow.int64()),
            pyarrow.array(
                [decimal.Decimal(2), decimal.Decimal(1), None], pyarrow.decimal128(5, 0)
            ),
        ),
        (
            pyarrow.array([1, 2, None], pyarrow.int32()),
            pyarrow.array([2, 1, None], pyarrow.int64()),
        ),
    ],
)
def test_generate_report_with_mismatched_key_types(
    module_under_test, source_key, target_key
):
    source = pyarrow.table({"id": source_key, "count": [5, 7, 3]})
    target = pyarrow.table({"id": target_key, "count": [7, 5, 3]})
    report = module_under_test.generate_report(
        _grouped_count_run_metadata(), source, target, join_on_fields=("id",)
    )
    assert len(report) == 3
    assert set(report[consts.VALIDATION_STATUS]) == {consts.VALIDATION_STATUS_SUCCESS}


def test_generate_report_with_incompatible_key_types(module_under_test):
    source = pyarrow.table({"id": ["a", "b"], "count": [1, 2]})
    target = pyarrow.table({"id": [1, 2], "count": [1, 2]})
    with pytest.raises(ValueError, match="Unable to join on key id"):
        module_under_test.generate_report(
            _grouped_count_run_metadata(), source, target, join_on_fields=("id",)
        )


def test_generate_report_with_too_many_rows(module_under_test):
    source = pyarrow.table({"count": [1, 1]})
    target = pyarrow.table({"count": [2, 2]})
    with pytest.raises(ValueError, match="Expected at most one row per side"):
        module_under_test.generate_report(
            EXAMPLE_RUN_METADATA,
            source,
            target,
        )


def test_generate_report_with_empty_target(module_under_test):
    """A column validation where the target returns no rows should fail, not error."""
    source = pyarrow.table({"count": [1]})
    target = pyarrow.table({"count": pyarrow.array([], type=pyarrow.int64())})
    report = module_under_test.generate_report(EXAMPLE_RUN_METADATA, source, target)

    assert len(report) == 1
    assert report.loc[0, consts.SOURCE_AGG_VALUE] == "1"
    assert report.loc[0, consts.VALIDATION_STATUS] == consts.VALIDATION_STATUS_FAIL


@pytest.mark.parametrize(
    ("source_array", "target_array", "threshold", "expected_status"),
    [
        (
            pyarrow.array(["6240b18a-88c2-410e-8aa4-e937cf073835"]),
            pyarrow.array(["6240b18a-88c2-410e-8aa4-e937cf073835"]),
            0.0,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            pyarrow.array(["6240b18a-88c2-410e-8aa4-e937cf073835"]),
            pyarrow.array(["ffffffff-ffff-ffff-ffff-ffffffffffff"]),
            50.0,
            consts.VALIDATION_STATUS_FAIL,
        ),
        (
            pyarrow.array(
                ["6240b18a-88c2-410e-8aa4-e937cf073835"],
                type=pyarrow.large_string(),
            ),
            pyarrow.array(["6240b18a-88c2-410e-8aa4-e937cf073835"]),
            0.0,
            consts.VALIDATION_STATUS_SUCCESS,
        ),
        (
            pyarrow.array([None], type=pyarrow.string()),
            pyarrow.array(["6240b18a-88c2-410e-8aa4-e937cf073835"]),
            0.0,
            consts.VALIDATION_STATUS_FAIL,
        ),
    ],
)
def test_generate_report_with_string_aggregates(
    module_under_test, source_array, target_array, threshold, expected_status
):
    """UUID min/max and legacy cast: string aggregates compare by equality with NULL diff."""
    col_name = "min__cast_uuid_string__col_uuid"
    run_metadata = metadata.RunMetadata(
        validations={
            col_name: metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="source_dataset",
                source_column_name="col_uuid",
                target_table_name="test_target",
                target_table_schema="target_dataset",
                target_column_name="col_uuid",
                validation_type="Column",
                aggregation_type="min",
                primary_keys=[],
                num_random_rows=None,
                threshold=threshold,
            ),
        },
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=None,
        labels=[],
        run_id="test-run",
    )
    source = pyarrow.table({col_name: source_array})
    target = pyarrow.table({col_name: target_array})

    report = module_under_test.generate_report(run_metadata, source, target)

    assert len(report) == 1
    assert report.loc[0, consts.VALIDATION_STATUS] == expected_status
    assert pandas.isna(report.loc[0, consts.VALIDATION_DIFFERENCE])
    assert pandas.isna(report.loc[0, consts.VALIDATION_PCT_DIFFERENCE])


def test_generate_report_with_mixed_numeric_and_string_aggregates(module_under_test):
    """Numeric differences must not bleed into adjacent string aggregate slices."""
    uuid_col = "min__cast_uuid_string__col_uuid"
    legacy_sum_col = "sum__col_dec_38"
    run_metadata = metadata.RunMetadata(
        validations={
            col: metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="source_dataset",
                source_column_name=col,
                target_table_name="test_target",
                target_table_schema="target_dataset",
                target_column_name=col,
                validation_type="Column",
                aggregation_type=agg_type,
                primary_keys=[],
                num_random_rows=None,
                threshold=0.0,
            )
            for col, agg_type in [
                ("count", "count"),
                (uuid_col, "min"),
                (legacy_sum_col, "sum"),
                ("sum__col_int", "sum"),
            ]
        },
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=None,
        labels=[],
        run_id="test-run",
    )
    source = pyarrow.table(
        {
            "grp": ["a", "b"],
            "count": [10, 20],
            uuid_col: [
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
            ],
            legacy_sum_col: [
                "61728394506172839450617283945061728390",
                "99999999999999999999999999999999999999",
            ],
            "sum__col_int": [100, 200],
        }
    )
    target = pyarrow.table(
        {
            "grp": ["a", "b"],
            "count": [12, 20],
            uuid_col: [
                "11111111-1111-1111-1111-111111111111",
                "99999999-9999-9999-9999-999999999999",
            ],
            legacy_sum_col: [
                "61728394506172839450617283945061728390",
                "99999999999999999999999999999999999999",
            ],
            "sum__col_int": [100, 250],
        }
    )

    report = module_under_test.generate_report(
        run_metadata, source, target, join_on_fields=("grp",)
    )

    by_col = {
        name: group.sort_values(consts.GROUP_BY_COLUMNS).reset_index(drop=True)
        for name, group in report.groupby(consts.VALIDATION_NAME)
    }

    assert list(by_col["count"][consts.VALIDATION_DIFFERENCE]) == [2.0, 0.0]
    assert list(by_col["count"][consts.VALIDATION_STATUS]) == [
        consts.VALIDATION_STATUS_FAIL,
        consts.VALIDATION_STATUS_SUCCESS,
    ]

    assert by_col[uuid_col][consts.VALIDATION_DIFFERENCE].isna().all()
    assert by_col[uuid_col][consts.VALIDATION_PCT_DIFFERENCE].isna().all()
    assert list(by_col[uuid_col][consts.VALIDATION_STATUS]) == [
        consts.VALIDATION_STATUS_SUCCESS,
        consts.VALIDATION_STATUS_FAIL,
    ]

    assert by_col[legacy_sum_col][consts.VALIDATION_DIFFERENCE].isna().all()
    assert list(by_col[legacy_sum_col][consts.VALIDATION_STATUS]) == [
        consts.VALIDATION_STATUS_SUCCESS,
        consts.VALIDATION_STATUS_SUCCESS,
    ]

    assert list(by_col["sum__col_int"][consts.VALIDATION_DIFFERENCE]) == [0.0, 50.0]
    assert list(by_col["sum__col_int"][consts.VALIDATION_STATUS]) == [
        consts.VALIDATION_STATUS_SUCCESS,
        consts.VALIDATION_STATUS_FAIL,
    ]


@pytest.mark.parametrize(
    ("source_array", "target_array"),
    [
        (pyarrow.array(["10"]), pyarrow.array([10])),
        (pyarrow.array([b"abc"]), pyarrow.array([b"abc"])),
    ],
)
def test_generate_report_with_invalid_aggregate_types(
    module_under_test, source_array, target_array
):
    source = pyarrow.table({"count": source_array})
    target = pyarrow.table({"count": target_array})
    with pytest.raises(
        ValueError, match="Aggregate value of count must be numeric or string"
    ):
        module_under_test.generate_report(EXAMPLE_RUN_METADATA, source, target)


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
        for _ in input_df.column_names
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
        # Source aggregate value is 0:
        # - count: source=0, target=0 -> difference=0.0, pct_difference=0.0, status=success
        # - sum__a: source=0, target=5 -> difference=5.0, denominator falls back to target (5),
        #   so pct_difference=100.0, status=fail
        (
            pandas.DataFrame({"count": [0], "sum__a": [0]}),
            pandas.DataFrame({"count": [0], "sum__a": [5]}),
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
                    "sum__a": metadata.ValidationMetadata(
                        source_table_name="test_source",
                        source_table_schema="bq-public.source_dataset",
                        source_column_name="a",
                        target_table_name="test_target",
                        target_table_schema="bq-public.target_dataset",
                        target_column_name="a",
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
                    consts.SOURCE_COLUMN_NAME: [None, "a"],
                    consts.TARGET_TABLE_NAME: [
                        "bq-public.target_dataset.test_target",
                        "bq-public.target_dataset.test_target",
                    ],
                    consts.TARGET_COLUMN_NAME: [None, "a"],
                    consts.VALIDATION_TYPE: ["Column", "Column"],
                    consts.AGGREGATION_TYPE: ["count", "sum"],
                    consts.VALIDATION_NAME: ["count", "sum__a"],
                    consts.SOURCE_AGG_VALUE: ["0", "0"],
                    consts.TARGET_AGG_VALUE: ["0", "5"],
                    consts.GROUP_BY_COLUMNS: [None, None],
                    consts.CONFIG_PRIMARY_KEYS: [None, None],
                    consts.NUM_RANDOM_ROWS: [None, None],
                    consts.VALIDATION_DIFFERENCE: [0.0, 5.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [0.0, 100.0],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0, 0.0],
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
        pyarrow.Table.from_pandas(source_df, preserve_index=False),
        pyarrow.Table.from_pandas(target_df, preserve_index=False),
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
    _assert_frame_equal(report, expected)


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
                        25.0,
                        25.0,
                        25.0,
                        25.0,
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
        pyarrow.Table.from_pandas(source_df, preserve_index=False),
        pyarrow.Table.from_pandas(target_df, preserve_index=False),
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
    _assert_frame_equal(report, expected)


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
        pyarrow.Table.from_pandas(source_df, preserve_index=False),
        pyarrow.Table.from_pandas(target_df, preserve_index=False),
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
    _assert_frame_equal(report, expected)


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
                    consts.CONFIG_PRIMARY_KEYS: ["{id}"] * 5,
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
        # Test Case 3: Column Validation Summary
        # Verifies that a standard column validation correctly logs
        # total, success, and fail validation counts.
        (
            metadata.RunMetadata(
                run_id="col-test-run",
                start_time=datetime.datetime(
                    2025, 2, 12, 7, 30, 10, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2025, 2, 12, 7, 32, 15, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_TYPE: [consts.COLUMN_VALIDATION] * 5,
                    consts.CONFIG_PRIMARY_KEYS: [None] * 5,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 2
                    + [consts.VALIDATION_STATUS_FAIL] * 3,
                    consts.SOURCE_COLUMN_NAME: ["col1", "col2", "col3", "col4", "col5"],
                    consts.TARGET_COLUMN_NAME: ["col1", "col2", "col3", "col4", "col5"],
                    consts.SOURCE_AGG_VALUE: [10, 20, 30, 40, None],
                    consts.TARGET_AGG_VALUE: [10, 20, 60, None, 80],
                }
            ),
            pandas.DataFrame(),
            pandas.DataFrame(),
            {
                consts.CONFIG_RUN_ID: "col-test-run",
                consts.CONFIG_START_TIME: "2025-02-12T07:30:10+00:00",
                consts.CONFIG_END_TIME: "2025-02-12T07:32:15+00:00",
                consts.TOTAL_VALIDATIONS: 5,
                consts.TOTAL_VALIDATIONS_SUCCESS: 2,
                consts.TOTAL_VALIDATIONS_FAIL: 3,
            },
        ),
        # Test Case 4: Custom Query (Column-like) Validation Summary
        # Verifies that a custom query validation without primary keys
        # (which behaves like a column validation) correctly logs
        # total, success, and fail validation counts.
        (
            metadata.RunMetadata(
                run_id="custom-col-test-run",
                start_time=datetime.datetime(
                    2025, 2, 12, 7, 30, 10, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2025, 2, 12, 7, 32, 15, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_TYPE: [consts.CUSTOM_QUERY] * 5,
                    consts.CONFIG_PRIMARY_KEYS: [None] * 5,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 2
                    + [consts.VALIDATION_STATUS_FAIL] * 3,
                    consts.SOURCE_COLUMN_NAME: [None] * 5,
                    consts.TARGET_COLUMN_NAME: [None] * 5,
                    consts.SOURCE_AGG_VALUE: [10, 20, 30, 40, None],
                    consts.TARGET_AGG_VALUE: [10, 20, 60, None, 80],
                }
            ),
            pandas.DataFrame(),
            pandas.DataFrame(),
            {
                consts.CONFIG_RUN_ID: "custom-col-test-run",
                consts.CONFIG_START_TIME: "2025-02-12T07:30:10+00:00",
                consts.CONFIG_END_TIME: "2025-02-12T07:32:15+00:00",
                consts.TOTAL_VALIDATIONS: 5,
                consts.TOTAL_VALIDATIONS_SUCCESS: 2,
                consts.TOTAL_VALIDATIONS_FAIL: 3,
            },
        ),
    ),
)
def test_get_summary_with_values_for_all_stats(
    module_under_test, caplog, run_metadata, result_df, source_df, target_df, expected
):
    caplog.set_level(logging.INFO)
    module_under_test._get_summary(
        run_metadata,
        pyarrow.Table.from_pandas(result_df, preserve_index=False),
        pyarrow.Table.from_pandas(source_df, preserve_index=False),
        pyarrow.Table.from_pandas(target_df, preserve_index=False),
    )

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
    module_under_test._get_summary(
        run_metadata,
        pyarrow.Table.from_pandas(result_df, preserve_index=False),
        pyarrow.Table.from_pandas(source_df, preserve_index=False),
        pyarrow.Table.from_pandas(target_df, preserve_index=False),
    )
    assert all(
        module_under_test.COMBINER_GET_SUMMARY_EXC_TEXT not in _.message
        for _ in caplog.records
    )


def test_generate_report_with_binary_data(module_under_test):
    """Test report generation with binary data containing non-UTF-8 characters.

    This ensures that DVT does not crash with a UnicodeDecodeError when
    casting binary columns to string in the Pandas combiner phase. Instead,
    the binary data should be formatted as a hex string in the report.
    """
    # Binary data with non-UTF-8 bytes (e.g. 0xac)
    binary_data = b"\xac\xed\x00\x05"
    source_table = pyarrow.table({"id": [1], "bin_col": [binary_data]})
    target_table = pyarrow.table({"id": [1], "bin_col": [binary_data]})

    run_metadata = metadata.RunMetadata(
        validations={
            "bin_col": metadata.ValidationMetadata(
                source_table_name="test_source",
                source_table_schema="bq-public.source_dataset",
                source_column_name="bin_col",
                target_table_name="test_target",
                target_table_schema="bq-public.target_dataset",
                target_column_name="bin_col",
                validation_type="Row",
                aggregation_type="hash",
                primary_keys=["id"],
                num_random_rows=None,
                threshold=0.0,
            ),
        },
        start_time=datetime.datetime(1998, 9, 4, 7, 30, 1),
        end_time=datetime.datetime(1998, 9, 4, 7, 31, 42),
        labels=[],
        run_id="test-run",
    )

    report = module_under_test.generate_report(
        run_metadata,
        source_table,
        target_table,
        join_on_fields=("id",),
        is_value_comparison=True,
    )

    assert consts.SOURCE_AGG_VALUE in report.columns
    assert consts.TARGET_AGG_VALUE in report.columns
    assert report[consts.SOURCE_AGG_VALUE].iloc[0] == "aced0005"
    assert report[consts.TARGET_AGG_VALUE].iloc[0] == "aced0005"


@pytest.mark.parametrize(
    (
        "source_table",
        "target_table",
        "join_on_fields",
        "is_value_comparison",
        "run_metadata",
        "expected",
    ),
    [
        pytest.param(
            pyarrow.table(
                {
                    "id": [2, 1, 3],
                    "count": [1, 1, 1],
                    "count__a": [1, 1, 1],
                    "count__b": [1, 1, 1],
                    "sum__a": pyarrow.array(
                        [
                            decimal.Decimal("123.220000000"),
                            decimal.Decimal("123.110000000"),
                            decimal.Decimal("123.300000000"),
                        ],
                        type=pyarrow.decimal128(38, 9),
                    ),
                    "avg__b": [123456.2, 123456.1, 123456.3],
                }
            ),
            pyarrow.table(
                {
                    "id": [2, 1, 3],
                    "count": [1, 1, 1],
                    "count__a": [1, 1, 1],
                    "count__b": [1, 1, 1],
                    "sum__a": [123456.2, 123456.1, 123456.3],
                    "avg__b": pyarrow.array(
                        [
                            decimal.Decimal("123456.100000000"),
                            decimal.Decimal("123456.100000000"),
                            decimal.Decimal("123456.100000000"),
                        ],
                        type=pyarrow.decimal128(38, 9),
                    ),
                }
            ),
            ("id",),
            False,
            metadata.RunMetadata(
                run_id="d48471f6-baf4-436f-9cdd-878f7d57ef3d",
                validations={
                    "avg__b": metadata.ValidationMetadata(
                        validation_type="Custom-query",
                        aggregation_type="avg",
                        source_table_schema=None,
                        source_table_name=None,
                        target_table_schema=None,
                        target_table_name=None,
                        source_column_name="b",
                        target_column_name="b",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                    "count": metadata.ValidationMetadata(
                        validation_type="Custom-query",
                        aggregation_type="count",
                        source_table_schema=None,
                        source_table_name=None,
                        target_table_schema=None,
                        target_table_name=None,
                        source_column_name=None,
                        target_column_name=None,
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                    "count__a": metadata.ValidationMetadata(
                        validation_type="Custom-query",
                        aggregation_type="count",
                        source_table_schema=None,
                        source_table_name=None,
                        target_table_schema=None,
                        target_table_name=None,
                        source_column_name="a",
                        target_column_name="a",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                    "count__b": metadata.ValidationMetadata(
                        validation_type="Custom-query",
                        aggregation_type="count",
                        source_table_schema=None,
                        source_table_name=None,
                        target_table_schema=None,
                        target_table_name=None,
                        source_column_name="b",
                        target_column_name="b",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                    "sum__a": metadata.ValidationMetadata(
                        validation_type="Custom-query",
                        aggregation_type="sum",
                        source_table_schema=None,
                        source_table_name=None,
                        target_table_schema=None,
                        target_table_name=None,
                        source_column_name="a",
                        target_column_name="a",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=30.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 28, 17, 45, 49, 338186, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 28, 17, 45, 52, 947991, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: [
                        "count",
                        "count",
                        "count",
                        "count__a",
                        "count__a",
                        "count__a",
                        "count__b",
                        "count__b",
                        "count__b",
                        "sum__a",
                        "sum__a",
                        "sum__a",
                        "avg__b",
                        "avg__b",
                        "avg__b",
                    ],
                    consts.VALIDATION_TYPE: ["Custom-query"] * 15,
                    consts.AGGREGATION_TYPE: [
                        "count",
                        "count",
                        "count",
                        "count",
                        "count",
                        "count",
                        "count",
                        "count",
                        "count",
                        "sum",
                        "sum",
                        "sum",
                        "avg",
                        "avg",
                        "avg",
                    ],
                    consts.SOURCE_TABLE_NAME: [None] * 15,
                    consts.SOURCE_COLUMN_NAME: [
                        None,
                        None,
                        None,
                        "a",
                        "a",
                        "a",
                        "b",
                        "b",
                        "b",
                        "a",
                        "a",
                        "a",
                        "b",
                        "b",
                        "b",
                    ],
                    consts.SOURCE_AGG_VALUE: [
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "123.220000000",
                        "123.110000000",
                        "123.300000000",
                        "123456.2",
                        "123456.1",
                        "123456.3",
                    ],
                    consts.TARGET_TABLE_NAME: [None] * 15,
                    consts.TARGET_COLUMN_NAME: [
                        None,
                        None,
                        None,
                        "a",
                        "a",
                        "a",
                        "b",
                        "b",
                        "b",
                        "a",
                        "a",
                        "a",
                        "b",
                        "b",
                        "b",
                    ],
                    consts.TARGET_AGG_VALUE: [
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        "123456.2",
                        "123456.1",
                        "123456.3",
                        "123456.100000000",
                        "123456.100000000",
                        "123456.100000000",
                    ],
                    consts.GROUP_BY_COLUMNS: [
                        '{"id": "2"}',
                        '{"id": "1"}',
                        '{"id": "3"}',
                    ]
                    * 5,
                    consts.CONFIG_PRIMARY_KEYS: [None] * 15,
                    consts.NUM_RANDOM_ROWS: [None] * 15,
                    consts.VALIDATION_DIFFERENCE: [
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        123332.98,
                        123332.99,
                        123333.0,
                        -0.09999999999126885,
                        0.0,
                        -0.19999999999708962,
                    ],
                    consts.VALIDATION_PCT_DIFFERENCE: [
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        100091.6869,
                        100181.1325,
                        100026.764,
                        -0.0001,
                        0.0,
                        -0.0002,
                    ],
                    consts.VALIDATION_PCT_THRESHOLD: [30.0] * 15,
                    consts.VALIDATION_STATUS: [
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_FAIL,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                        consts.VALIDATION_STATUS_SUCCESS,
                    ],
                    consts.CONFIG_RUN_ID: ["d48471f6-baf4-436f-9cdd-878f7d57ef3d"] * 15,
                    consts.CONFIG_LABELS: [[]] * 15,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            28,
                            17,
                            45,
                            49,
                            338186,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 15,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            28,
                            17,
                            45,
                            52,
                            947991,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 15,
                }
            ),
            id="column_custom_query_with_threshold",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "count": [3],
                    "count__col_int8": [3],
                    "count__col_int16": [3],
                }
            ),
            pyarrow.table(
                {
                    "count": [3],
                    "count__col_int8": [3],
                    "count__col_int16": [3],
                }
            ),
            (),
            False,
            metadata.RunMetadata(
                run_id="186f78e3-0b76-42ff-a02f-64bc5f328d7c",
                validations={
                    "count": metadata.ValidationMetadata(
                        validation_type="Column",
                        aggregation_type="count",
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name=None,
                        target_column_name=None,
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "count__col_int16": metadata.ValidationMetadata(
                        validation_type="Column",
                        aggregation_type="count",
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int16",
                        target_column_name="col_int16",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "count__col_int8": metadata.ValidationMetadata(
                        validation_type="Column",
                        aggregation_type="count",
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int8",
                        target_column_name="col_int8",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 15, 17, 41, 49, 885792, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 15, 17, 41, 53, 610613, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: [
                        "count",
                        "count__col_int8",
                        "count__col_int16",
                    ],
                    consts.VALIDATION_TYPE: ["Column"] * 3,
                    consts.AGGREGATION_TYPE: ["count"] * 3,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 3,
                    consts.SOURCE_COLUMN_NAME: [None, "col_int8", "col_int16"],
                    consts.SOURCE_AGG_VALUE: ["3", "3", "3"],
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 3,
                    consts.TARGET_COLUMN_NAME: [None, "col_int8", "col_int16"],
                    consts.TARGET_AGG_VALUE: ["3", "3", "3"],
                    consts.GROUP_BY_COLUMNS: [None] * 3,
                    consts.CONFIG_PRIMARY_KEYS: [None] * 3,
                    consts.NUM_RANDOM_ROWS: [None] * 3,
                    consts.VALIDATION_DIFFERENCE: [0.0, 0.0, 0.0],
                    consts.VALIDATION_PCT_DIFFERENCE: [0.0, 0.0, 0.0],
                    consts.VALIDATION_PCT_THRESHOLD: [0.0, 0.0, 0.0],
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 3,
                    consts.CONFIG_RUN_ID: ["186f78e3-0b76-42ff-a02f-64bc5f328d7c"] * 3,
                    consts.CONFIG_LABELS: [[]] * 3,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            15,
                            17,
                            41,
                            49,
                            885792,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 3,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            15,
                            17,
                            41,
                            53,
                            610613,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 3,
                }
            ),
            id="column_validation",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "id": [2, 1, 3],
                    "count": [1, 1, 1],
                    "count__col_int8": [1, 1, 1],
                    "count__col_int16": [1, 1, 1],
                }
            ),
            pyarrow.table(
                {
                    "id": [2, 1, 3],
                    "count": [1, 1, 1],
                    "count__col_int8": [1, 1, 1],
                    "count__col_int16": [1, 1, 1],
                }
            ),
            ("id",),
            False,
            metadata.RunMetadata(
                run_id="2e8bc264-22a8-4d0c-9605-f9c92daec4e0",
                validations={
                    "count": metadata.ValidationMetadata(
                        validation_type="Column",
                        aggregation_type="count",
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name=None,
                        target_column_name=None,
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "count__col_int16": metadata.ValidationMetadata(
                        validation_type="Column",
                        aggregation_type="count",
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int16",
                        target_column_name="col_int16",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "count__col_int8": metadata.ValidationMetadata(
                        validation_type="Column",
                        aggregation_type="count",
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int8",
                        target_column_name="col_int8",
                        primary_keys=[],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 15, 19, 21, 0, 253135, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 15, 19, 21, 3, 252970, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: [
                        "count",
                        "count",
                        "count",
                        "count__col_int8",
                        "count__col_int8",
                        "count__col_int8",
                        "count__col_int16",
                        "count__col_int16",
                        "count__col_int16",
                    ],
                    consts.VALIDATION_TYPE: ["Column"] * 9,
                    consts.AGGREGATION_TYPE: ["count"] * 9,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 9,
                    consts.SOURCE_COLUMN_NAME: [
                        None,
                        None,
                        None,
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int16",
                        "col_int16",
                        "col_int16",
                    ],
                    consts.SOURCE_AGG_VALUE: ["1"] * 9,
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 9,
                    consts.TARGET_COLUMN_NAME: [
                        None,
                        None,
                        None,
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int16",
                        "col_int16",
                        "col_int16",
                    ],
                    consts.TARGET_AGG_VALUE: ["1"] * 9,
                    consts.GROUP_BY_COLUMNS: [
                        '{"id": "2"}',
                        '{"id": "1"}',
                        '{"id": "3"}',
                    ]
                    * 3,
                    consts.CONFIG_PRIMARY_KEYS: [None] * 9,
                    consts.NUM_RANDOM_ROWS: [None] * 9,
                    consts.VALIDATION_DIFFERENCE: [0.0] * 9,
                    consts.VALIDATION_PCT_DIFFERENCE: [0.0] * 9,
                    consts.VALIDATION_PCT_THRESHOLD: [0.0] * 9,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 9,
                    consts.CONFIG_RUN_ID: ["2e8bc264-22a8-4d0c-9605-f9c92daec4e0"] * 9,
                    consts.CONFIG_LABELS: [[]] * 9,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026, 9, 15, 19, 21, 0, 253135, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 9,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026, 9, 15, 19, 21, 3, 252970, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 9,
                }
            ),
            id="column_validation_with_grouped_by",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "col_int8": pyarrow.array([2, 1, 3], type=pyarrow.int8()),
                    "col_int64": pyarrow.array([2, 1, 3], type=pyarrow.int64()),
                    "col_dec_20": pyarrow.array(
                        [
                            decimal.Decimal("12345678901234567890.000000000"),
                            decimal.Decimal("12345678901234567890.000000000"),
                            decimal.Decimal("12345678901234567890.000000000"),
                        ],
                        type=pyarrow.decimal128(38, 9),
                    ),
                    "col_float32": pyarrow.array(
                        [123456.2, 123456.1, 123456.3], type=pyarrow.float64()
                    ),
                    "col_varchar_30": pyarrow.array(
                        ["Hello DVT", "Hello DVT", "Hello DVT"], type=pyarrow.string()
                    ),
                    "col_date": pyarrow.array(
                        [
                            datetime.date(1970, 1, 2),
                            datetime.date(1970, 1, 1),
                            datetime.date(1970, 1, 3),
                        ],
                        type=pyarrow.date32(),
                    ),
                    "col_tstz": pyarrow.array(
                        [
                            datetime.datetime(
                                1970, 1, 2, 2, 0, 2, tzinfo=datetime.timezone.utc
                            ),
                            datetime.datetime(
                                1970, 1, 1, 1, 0, 1, tzinfo=datetime.timezone.utc
                            ),
                            datetime.datetime(
                                1970, 1, 3, 3, 0, 3, tzinfo=datetime.timezone.utc
                            ),
                        ],
                        type=pyarrow.timestamp("us", tz="UTC"),
                    ),
                    "id": pyarrow.array([2, 1, 3], type=pyarrow.int64()),
                }
            ),
            pyarrow.table(
                {
                    "col_int8": pyarrow.array([2, 1, 3], type=pyarrow.int8()),
                    "col_int64": pyarrow.array([2, 1, 3], type=pyarrow.int64()),
                    "col_dec_20": pyarrow.array(
                        [
                            decimal.Decimal("12345678901234567890.000000000"),
                            decimal.Decimal("12345678901234567890.000000000"),
                            decimal.Decimal("12345678901234567890.000000000"),
                        ],
                        type=pyarrow.decimal128(38, 9),
                    ),
                    "col_float32": pyarrow.array(
                        [123456.2, 123456.1, 123456.3], type=pyarrow.float64()
                    ),
                    "col_varchar_30": pyarrow.array(
                        ["Hello DVT", "Hello DVT", "Hello DVT"], type=pyarrow.string()
                    ),
                    "col_date": pyarrow.array(
                        [
                            datetime.date(1970, 1, 2),
                            datetime.date(1970, 1, 1),
                            datetime.date(1970, 1, 3),
                        ],
                        type=pyarrow.date32(),
                    ),
                    "col_tstz": pyarrow.array(
                        [
                            datetime.datetime(
                                1970, 1, 2, 2, 0, 2, tzinfo=datetime.timezone.utc
                            ),
                            datetime.datetime(
                                1970, 1, 1, 1, 0, 1, tzinfo=datetime.timezone.utc
                            ),
                            datetime.datetime(
                                1970, 1, 3, 3, 0, 3, tzinfo=datetime.timezone.utc
                            ),
                        ],
                        type=pyarrow.timestamp("us", tz="UTC"),
                    ),
                    "id": pyarrow.array([2, 1, 3], type=pyarrow.int64()),
                }
            ),
            ("id",),
            True,
            metadata.RunMetadata(
                run_id="ea8a48b2-b4ba-4275-b58c-85b2b75fa3ed",
                validations={
                    "col_date": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_date",
                        target_column_name="col_date",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_dec_20": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_dec_20",
                        target_column_name="col_dec_20",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_float32": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_float32",
                        target_column_name="col_float32",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_int64": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int64",
                        target_column_name="col_int64",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_int8": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int8",
                        target_column_name="col_int8",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_tstz": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_tstz",
                        target_column_name="col_tstz",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_varchar_30": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_varchar_30",
                        target_column_name="col_varchar_30",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 27, 22, 16, 26, 52563, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 27, 22, 16, 28, 937587, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: [
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int64",
                        "col_int64",
                        "col_int64",
                        "col_dec_20",
                        "col_dec_20",
                        "col_dec_20",
                        "col_float32",
                        "col_float32",
                        "col_float32",
                        "col_varchar_30",
                        "col_varchar_30",
                        "col_varchar_30",
                        "col_date",
                        "col_date",
                        "col_date",
                        "col_tstz",
                        "col_tstz",
                        "col_tstz",
                    ],
                    consts.VALIDATION_TYPE: ["Row"] * 21,
                    consts.AGGREGATION_TYPE: [None] * 21,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_core_types"]
                    * 21,
                    consts.SOURCE_COLUMN_NAME: [
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int64",
                        "col_int64",
                        "col_int64",
                        "col_dec_20",
                        "col_dec_20",
                        "col_dec_20",
                        "col_float32",
                        "col_float32",
                        "col_float32",
                        "col_varchar_30",
                        "col_varchar_30",
                        "col_varchar_30",
                        "col_date",
                        "col_date",
                        "col_date",
                        "col_tstz",
                        "col_tstz",
                        "col_tstz",
                    ],
                    consts.SOURCE_AGG_VALUE: [
                        "2",
                        "1",
                        "3",
                        "2",
                        "1",
                        "3",
                        "12345678901234567890.000000000",
                        "12345678901234567890.000000000",
                        "12345678901234567890.000000000",
                        "123456.2",
                        "123456.1",
                        "123456.3",
                        "Hello DVT",
                        "Hello DVT",
                        "Hello DVT",
                        "1970-01-02",
                        "1970-01-01",
                        "1970-01-03",
                        "1970-01-02 02:00:02+00:00",
                        "1970-01-01 01:00:01+00:00",
                        "1970-01-03 03:00:03+00:00",
                    ],
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_core_types"]
                    * 21,
                    consts.TARGET_COLUMN_NAME: [
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int64",
                        "col_int64",
                        "col_int64",
                        "col_dec_20",
                        "col_dec_20",
                        "col_dec_20",
                        "col_float32",
                        "col_float32",
                        "col_float32",
                        "col_varchar_30",
                        "col_varchar_30",
                        "col_varchar_30",
                        "col_date",
                        "col_date",
                        "col_date",
                        "col_tstz",
                        "col_tstz",
                        "col_tstz",
                    ],
                    consts.TARGET_AGG_VALUE: [
                        "2",
                        "1",
                        "3",
                        "2",
                        "1",
                        "3",
                        "12345678901234567890.000000000",
                        "12345678901234567890.000000000",
                        "12345678901234567890.000000000",
                        "123456.2",
                        "123456.1",
                        "123456.3",
                        "Hello DVT",
                        "Hello DVT",
                        "Hello DVT",
                        "1970-01-02",
                        "1970-01-01",
                        "1970-01-03",
                        "1970-01-02 02:00:02+00:00",
                        "1970-01-01 01:00:01+00:00",
                        "1970-01-03 03:00:03+00:00",
                    ],
                    consts.GROUP_BY_COLUMNS: [
                        '{"id": "2"}',
                        '{"id": "1"}',
                        '{"id": "3"}',
                    ]
                    * 7,
                    consts.CONFIG_PRIMARY_KEYS: ["{id}"] * 21,
                    consts.NUM_RANDOM_ROWS: [None] * 21,
                    consts.VALIDATION_DIFFERENCE: [_NAN] * 21,
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN] * 21,
                    consts.VALIDATION_PCT_THRESHOLD: [0.0] * 21,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 21,
                    consts.CONFIG_RUN_ID: ["ea8a48b2-b4ba-4275-b58c-85b2b75fa3ed"] * 21,
                    consts.CONFIG_LABELS: [[]] * 21,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026, 9, 27, 22, 16, 26, 52563, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 21,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            27,
                            22,
                            16,
                            28,
                            937587,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 21,
                }
            ),
            id="comp_row_many_types_validation",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "col_int8": [2, 1, 3],
                    "col_int16": [2, 1, 3],
                    "id": [2, 1, 3],
                }
            ),
            pyarrow.table(
                {
                    "col_int8": [2, 1, 3],
                    "col_int16": [2, 1, 3],
                    "id": [2, 1, 3],
                }
            ),
            ("id",),
            True,
            metadata.RunMetadata(
                run_id="cb07cc0e-252e-4204-b07f-c220175c0a75",
                validations={
                    "col_int16": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int16",
                        target_column_name="col_int16",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "col_int8": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="col_int8",
                        target_column_name="col_int8",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 18, 16, 31, 9, 788079, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 18, 16, 31, 14, 127144, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: [
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int16",
                        "col_int16",
                        "col_int16",
                    ],
                    consts.VALIDATION_TYPE: ["Row"] * 6,
                    consts.AGGREGATION_TYPE: [None] * 6,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 6,
                    consts.SOURCE_COLUMN_NAME: [
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int16",
                        "col_int16",
                        "col_int16",
                    ],
                    consts.SOURCE_AGG_VALUE: ["2", "1", "3", "2", "1", "3"],
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 6,
                    consts.TARGET_COLUMN_NAME: [
                        "col_int8",
                        "col_int8",
                        "col_int8",
                        "col_int16",
                        "col_int16",
                        "col_int16",
                    ],
                    consts.TARGET_AGG_VALUE: ["2", "1", "3", "2", "1", "3"],
                    consts.GROUP_BY_COLUMNS: [
                        '{"id": "2"}',
                        '{"id": "1"}',
                        '{"id": "3"}',
                    ]
                    * 2,
                    consts.CONFIG_PRIMARY_KEYS: ["{id}"] * 6,
                    consts.NUM_RANDOM_ROWS: [None] * 6,
                    consts.VALIDATION_DIFFERENCE: [_NAN] * 6,
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN] * 6,
                    consts.VALIDATION_PCT_THRESHOLD: [0.0] * 6,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 6,
                    consts.CONFIG_RUN_ID: ["cb07cc0e-252e-4204-b07f-c220175c0a75"] * 6,
                    consts.CONFIG_LABELS: [[]] * 6,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026, 9, 18, 16, 31, 9, 788079, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 6,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            18,
                            16,
                            31,
                            14,
                            127144,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 6,
                }
            ),
            id="comp_row_validation",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "hash__all": [
                        "37303565336232333937366431356239333734646166383037373434373961653137333664653731353637343665653235636361316138313333656633653735",
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    "id": [2, 1, 3],
                }
            ),
            pyarrow.table(
                {
                    "hash__all": [
                        "37303565336232333937366431356239333734646166383037373434373961653137333664653731353637343665653235636361316138313333656633653735",
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    "id": [2, 1, 3],
                }
            ),
            ("id",),
            True,
            metadata.RunMetadata(
                run_id="17afba6b-e05a-4bad-ad18-d7ceb7d1ad2a",
                validations={
                    "hash__all": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="hash__all",
                        target_column_name="hash__all",
                        primary_keys=["id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 15, 19, 27, 49, 142728, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 15, 19, 27, 52, 707596, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: ["hash__all"] * 3,
                    consts.VALIDATION_TYPE: ["Row"] * 3,
                    consts.AGGREGATION_TYPE: [None] * 3,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 3,
                    consts.SOURCE_COLUMN_NAME: ["hash__all"] * 3,
                    consts.SOURCE_AGG_VALUE: [
                        "37303565336232333937366431356239333734646166383037373434373961653137333664653731353637343665653235636361316138313333656633653735",
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 3,
                    consts.TARGET_COLUMN_NAME: ["hash__all"] * 3,
                    consts.TARGET_AGG_VALUE: [
                        "37303565336232333937366431356239333734646166383037373434373961653137333664653731353637343665653235636361316138313333656633653735",
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    consts.GROUP_BY_COLUMNS: [
                        '{"id": "2"}',
                        '{"id": "1"}',
                        '{"id": "3"}',
                    ],
                    consts.CONFIG_PRIMARY_KEYS: ["{id}"] * 3,
                    consts.NUM_RANDOM_ROWS: [None] * 3,
                    consts.VALIDATION_DIFFERENCE: [_NAN] * 3,
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN] * 3,
                    consts.VALIDATION_PCT_THRESHOLD: [0.0] * 3,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 3,
                    consts.CONFIG_RUN_ID: ["17afba6b-e05a-4bad-ad18-d7ceb7d1ad2a"] * 3,
                    consts.CONFIG_LABELS: [[]] * 3,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            15,
                            19,
                            27,
                            49,
                            142728,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 3,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            15,
                            19,
                            27,
                            52,
                            707596,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 3,
                }
            ),
            id="row_validation",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "binary_id": pyarrow.array(
                        [
                            bytes.fromhex("4456542d6b65792d35"),
                            bytes.fromhex("4456542d6b65792d34"),
                            bytes.fromhex("4456542d6b65792d33"),
                            bytes.fromhex("4456542d6b65792d32"),
                            bytes.fromhex("4456542d6b65792d31"),
                        ],
                        type=pyarrow.binary(),
                    ),
                    "other_data": ["Row 5", "Row 4", "Row 3", "Row 2", "Row 1"],
                    "int_id": [5, 4, 3, 2, 1],
                }
            ),
            pyarrow.table(
                {
                    "binary_id": pyarrow.array(
                        [
                            bytes.fromhex("4456542d6b65792d35"),
                            bytes.fromhex("4456542d6b65792d34"),
                            bytes.fromhex("4456542d6b65792d33"),
                            bytes.fromhex("4456542d6b65792d32"),
                            bytes.fromhex("4456542d6b65792d31"),
                        ],
                        type=pyarrow.binary(),
                    ),
                    "other_data": ["Row 5", "Row 4", "Row 3", "Row 2", "Row 1"],
                    "int_id": [5, 4, 3, 2, 1],
                }
            ),
            ("int_id",),
            True,
            metadata.RunMetadata(
                run_id="174a2bdb-265c-4230-9b51-cb8178285709",
                validations={
                    "binary_id": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_binary",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_binary",
                        source_column_name="binary_id",
                        target_column_name="binary_id",
                        primary_keys=["int_id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                    "other_data": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_binary",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_binary",
                        source_column_name="other_data",
                        target_column_name="other_data",
                        primary_keys=["int_id"],
                        num_random_rows=None,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 28, 1, 23, 38, 293669, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 28, 1, 23, 41, 256741, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: ["binary_id"] * 5 + ["other_data"] * 5,
                    consts.VALIDATION_TYPE: ["Row"] * 10,
                    consts.AGGREGATION_TYPE: [None] * 10,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_binary"] * 10,
                    consts.SOURCE_COLUMN_NAME: ["binary_id"] * 5 + ["other_data"] * 5,
                    consts.SOURCE_AGG_VALUE: [
                        "4456542d6b65792d35",
                        "4456542d6b65792d34",
                        "4456542d6b65792d33",
                        "4456542d6b65792d32",
                        "4456542d6b65792d31",
                        "Row 5",
                        "Row 4",
                        "Row 3",
                        "Row 2",
                        "Row 1",
                    ],
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_binary"] * 10,
                    consts.TARGET_COLUMN_NAME: ["binary_id"] * 5 + ["other_data"] * 5,
                    consts.TARGET_AGG_VALUE: [
                        "4456542d6b65792d35",
                        "4456542d6b65792d34",
                        "4456542d6b65792d33",
                        "4456542d6b65792d32",
                        "4456542d6b65792d31",
                        "Row 5",
                        "Row 4",
                        "Row 3",
                        "Row 2",
                        "Row 1",
                    ],
                    consts.GROUP_BY_COLUMNS: [
                        '{"int_id": "5"}',
                        '{"int_id": "4"}',
                        '{"int_id": "3"}',
                        '{"int_id": "2"}',
                        '{"int_id": "1"}',
                    ]
                    * 2,
                    consts.CONFIG_PRIMARY_KEYS: ["{int_id}"] * 10,
                    consts.NUM_RANDOM_ROWS: [None] * 10,
                    consts.VALIDATION_DIFFERENCE: [_NAN] * 10,
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN] * 10,
                    consts.VALIDATION_PCT_THRESHOLD: [0.0] * 10,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 10,
                    consts.CONFIG_RUN_ID: ["174a2bdb-265c-4230-9b51-cb8178285709"] * 10,
                    consts.CONFIG_LABELS: [[]] * 10,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026, 9, 28, 1, 23, 38, 293669, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 10,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026, 9, 28, 1, 23, 41, 256741, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 10,
                }
            ),
            id="row_validation_comparison_binary",
        ),
        pytest.param(
            pyarrow.table(
                {
                    "hash__all": [
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    "id": [1, 3],
                }
            ),
            pyarrow.table(
                {
                    "hash__all": [
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    "id": [1, 3],
                }
            ),
            ("id",),
            True,
            metadata.RunMetadata(
                run_id="e91b0d8a-560e-4118-9e97-6095fdf094ba",
                validations={
                    "hash__all": metadata.ValidationMetadata(
                        validation_type="Row",
                        aggregation_type=None,
                        source_table_schema="pso_data_validator",
                        source_table_name="dvt_core_types",
                        target_table_schema="pso_data_validator",
                        target_table_name="dvt_core_types",
                        source_column_name="hash__all",
                        target_column_name="hash__all",
                        primary_keys=["id"],
                        num_random_rows=2,
                        threshold=0.0,
                    ),
                },
                labels=[],
                start_time=datetime.datetime(
                    2026, 9, 30, 21, 47, 5, 427114, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    2026, 9, 30, 21, 47, 13, 752809, tzinfo=datetime.timezone.utc
                ),
            ),
            pandas.DataFrame(
                {
                    consts.VALIDATION_NAME: ["hash__all"] * 2,
                    consts.VALIDATION_TYPE: ["Row"] * 2,
                    consts.AGGREGATION_TYPE: [None] * 2,
                    consts.SOURCE_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 2,
                    consts.SOURCE_COLUMN_NAME: ["hash__all"] * 2,
                    consts.SOURCE_AGG_VALUE: [
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    consts.TARGET_TABLE_NAME: ["pso_data_validator.dvt_core_types"] * 2,
                    consts.TARGET_COLUMN_NAME: ["hash__all"] * 2,
                    consts.TARGET_AGG_VALUE: [
                        "34393339646532373033376632303836323265323861303434393331313030333162373036376439326365306564656234613663383138313536623139623239",
                        "33386232393965356262326632333034633136653233663763616233333037326236666536313966646163653832343330383130393566646431333466653531",
                    ],
                    consts.GROUP_BY_COLUMNS: [
                        '{"id": "1"}',
                        '{"id": "3"}',
                    ],
                    consts.CONFIG_PRIMARY_KEYS: ["{id}"] * 2,
                    consts.NUM_RANDOM_ROWS: [2, 2],
                    consts.VALIDATION_DIFFERENCE: [_NAN] * 2,
                    consts.VALIDATION_PCT_DIFFERENCE: [_NAN] * 2,
                    consts.VALIDATION_PCT_THRESHOLD: [0.0] * 2,
                    consts.VALIDATION_STATUS: [consts.VALIDATION_STATUS_SUCCESS] * 2,
                    consts.CONFIG_RUN_ID: ["e91b0d8a-560e-4118-9e97-6095fdf094ba"] * 2,
                    consts.CONFIG_LABELS: [[]] * 2,
                    consts.CONFIG_START_TIME: [
                        datetime.datetime(
                            2026, 9, 30, 21, 47, 5, 427114, tzinfo=datetime.timezone.utc
                        )
                    ]
                    * 2,
                    consts.CONFIG_END_TIME: [
                        datetime.datetime(
                            2026,
                            9,
                            30,
                            21,
                            47,
                            13,
                            752809,
                            tzinfo=datetime.timezone.utc,
                        )
                    ]
                    * 2,
                }
            ),
            id="row_validation_random_row",
        ),
    ],
)
def test_generate_report_captured_reports(
    module_under_test,
    source_table,
    target_table,
    join_on_fields,
    is_value_comparison,
    run_metadata,
    expected,
):
    with freeze_time(run_metadata.end_time):
        report = module_under_test.generate_report(
            run_metadata,
            source_table,
            target_table,
            join_on_fields=join_on_fields,
            is_value_comparison=is_value_comparison,
        )
    sort_cols = [consts.VALIDATION_NAME]
    if join_on_fields:
        sort_cols.append(consts.GROUP_BY_COLUMNS)
    report = (
        report.sort_values(sort_cols, key=lambda s: s.astype(str))
        .reset_index(drop=True)
        .reindex(sorted(report.columns), axis=1)
    )
    expected = (
        expected.sort_values(sort_cols, key=lambda s: s.astype(str))
        .reset_index(drop=True)
        .reindex(sorted(expected.columns), axis=1)
    )
    _assert_frame_equal(report, expected)
