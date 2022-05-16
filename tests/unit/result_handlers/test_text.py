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

import pytest

from pandas import DataFrame

SAMPLE_CONFIG = {}
SAMPLE_RESULT_DATA = [
    [0, 1, 2, 3, "Column", "source", "target"],
    [4, 5, 6, 7, "Column", "source", "target"],
    [8, 9, 10, 11, "Column", "source", "target"],
]
SAMPLE_RESULT_COLUMNS = [
    "A",
    "B",
    "C",
    "D",
    "validation_type",
    "source_agg_value",
    "target_agg_value",
]
SAMPLE_RESULT_COLUMNS_FILTER_LIST = [
    "B",
    "D",
    "validation_type",
    "source_agg_value",
    "target_agg_value",
]


@pytest.fixture
def module_under_test():
    from data_validation.result_handlers import text

    return text


def test_import(module_under_test):
    """Test import cleanly"""
    assert module_under_test is not None


def test_basic_result_handler(module_under_test):
    """Test basic handler executes"""
    result_df = DataFrame(SAMPLE_RESULT_DATA, columns=SAMPLE_RESULT_COLUMNS)
    format = "csv"
    result_handler = module_under_test.TextResultHandler(
        format, SAMPLE_RESULT_COLUMNS_FILTER_LIST
    )

    handler_output = result_handler.execute(SAMPLE_CONFIG, result_df)
    assert handler_output["A"].sum() == result_df["A"].sum()


def test_unsupported_result_format(module_under_test):
    """Check for invalid format"""
    with pytest.raises(ValueError):
        result_df = DataFrame(SAMPLE_RESULT_DATA, columns=SAMPLE_RESULT_COLUMNS)
        format = "foobar"
        result_handler = module_under_test.TextResultHandler(
            format, SAMPLE_RESULT_COLUMNS_FILTER_LIST
        )

        handler_output = result_handler.execute(SAMPLE_CONFIG, result_df)
        assert handler_output["A"].sum() == result_df["A"].sum()


def test_columns_to_print(module_under_test, capsys):
    """Check for trimmed columns in grid print"""
    result_df = DataFrame(SAMPLE_RESULT_DATA, columns=SAMPLE_RESULT_COLUMNS)
    format = "table"
    result_handler = module_under_test.TextResultHandler(
        format, SAMPLE_RESULT_COLUMNS_FILTER_LIST
    )
    result_handler.execute(SAMPLE_CONFIG, result_df)

    grid_text = "│A│C││0│2││4│6││8│10│"
    printed_text = capsys.readouterr().out
    print(printed_text)
    printed_text = (
        printed_text.replace("\n", "")
        .replace("'", "")
        .replace(" ", "")
        .replace("╒═════╤═════╕", "")
        .replace("╞═════╪═════╡", "")
        .replace("├─────┼─────┤", "")
        .replace("╘═════╧═════╛", "")
    )
    assert printed_text == grid_text
