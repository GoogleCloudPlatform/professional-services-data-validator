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
    {"table_name": "my_table", "count": 10},
    {"table_name": "my_table", "count": 10},
]


@pytest.fixture
def module_under_test():
    from data_validation.result_handlers import text

    return text


def test_import(module_under_test):
    """Test import cleanly """
    assert module_under_test is not None


def test_basic_result_handler(module_under_test):
    """Test basic handler executes """
    format = "json"
    result_df = DataFrame(SAMPLE_RESULT_DATA)
    result_handler = module_under_test.TextResultHandler()

    handler_output = result_handler.execute(SAMPLE_CONFIG, format, result_df)
    assert handler_output["count"].sum() == result_df["count"].sum()
