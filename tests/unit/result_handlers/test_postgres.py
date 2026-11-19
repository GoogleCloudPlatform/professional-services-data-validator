# Copyright 2026 Google LLC
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

from unittest import mock

import numpy
import pandas
import pytest

from data_validation import consts


@pytest.fixture
def module_under_test():
    from data_validation.result_handlers import postgres

    return postgres


@pytest.mark.parametrize(
    "labels,expected",
    [
        ([], "{}"),
        ([("a", "1"), ("b", "2")], '{"a=1","b=2"}'),
        ([numpy.array(["a", "1"])], '{"a=1"}'),
        ([("env", "it's")], '{"env=it\'s"}'),
        ([("env", 'it"s')], r'{"env=it\"s"}'),
        ([("path", "c:\\dir")], r'{"path=c:\\dir"}'),
        ([("a", "{x,y}")], '{"a={x,y}"}'),
        ([("a", " x ")], '{"a= x "}'),
        (["unexpected"], '{"unexpected"}'),
    ],
)
def test_dataframe_to_sql_labels_array_literal(module_under_test, labels, expected):
    """Test labels are converted to PostgreSQL array literals with each element quoted and escaped."""
    handler = module_under_test.PostgresResultHandler(mock.Mock())
    result_df = pandas.DataFrame({consts.CONFIG_LABELS: [labels]})
    with mock.patch.object(pandas.DataFrame, "to_sql") as mock_to_sql:
        handler._dataframe_to_sql("schema_name", "table_name", result_df)
    mock_to_sql.assert_called_once()
    assert result_df[consts.CONFIG_LABELS][0] == expected
