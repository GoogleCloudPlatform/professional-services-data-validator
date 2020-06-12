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

import ibis.pandas
import pandas
import pandas.testing
import pytest


@pytest.fixture
def module_under_test():
    from data_validation import combiner

    return combiner


def test_generate_report_with_different_columns(module_under_test):
    source = pandas.DataFrame({"count": [1], "sum": [3]})
    target = pandas.DataFrame({"count": [2]})
    pandas_client = ibis.pandas.connect(
        {
            module_under_test.DEFAULT_SOURCE: source,
            module_under_test.DEFAULT_TARGET: target,
        }
    )
    with pytest.raises(
        ValueError, match="Expected source and target to have same schema"
    ):
        module_under_test.generate_report(pandas_client)


@pytest.mark.parametrize(
    ("source_df", "target_df", "expected"),
    (
        (
            pandas.DataFrame({"count": [1]}),
            pandas.DataFrame({"count": [2]}),
            pandas.DataFrame(
                {
                    "validation_name": ["count"],
                    "source_agg_value": ["1"],
                    "target_agg_value": ["2"],
                    "source_group_by_columns": [[]],
                    "target_group_by_columns": [[]],
                }
            ),
        ),
        (
            pandas.DataFrame({"count": [12], "sum": [-1]}),
            pandas.DataFrame({"count": [13], "sum": [1]}),
            pandas.DataFrame(
                {
                    "validation_name": ["count", "sum"],
                    "source_agg_value": ["12", "-1"],
                    "target_agg_value": ["13", "1"],
                    "source_group_by_columns": [[], []],
                    "target_group_by_columns": [[], []],
                }
            ),
        ),
    ),
)
def test_generate_report_without_group_by(
    module_under_test, source_df, target_df, expected
):
    pandas_client = ibis.pandas.connect(
        {"test_source": source_df, "test_target": target_df}
    )
    report = module_under_test.generate_report(
        pandas_client, source_table="test_source", target_table="test_target"
    )
    # Sort to order in the comparison.
    report = report.sort_values("validation_name", ignore_index=True)
    expected = expected.sort_values("validation_name", ignore_index=True)
    pandas.testing.assert_frame_equal(report, expected)
