# Copyright 2024 Google LLC
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


@pytest.fixture
def module_under_test():
    import data_validation.raw_query

    return data_validation.raw_query


@pytest.mark.parametrize(
    "test_input",
    [
        [
            "a",
            "some-long-string".ljust(256, "x"),
        ],
        [
            "b",
            "some-long-string".ljust(1024, "z"),
        ],
        # I was unable to figure out how to mock up a SQLAlchemy Row() object here.
        # Therefore resorted to testing via an integration test.
    ],
)
def test_print_raw_query_output_long_input(module_under_test, capsys, test_input: list):
    module_under_test.print_raw_query_output(test_input)
    captured = capsys.readouterr()
    assert test_input[1] in captured.out
    assert "characters truncated" not in captured.out


@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        ([0, 12345678], "0, 12345678"),
        ([1, 12345.678], "1, 12345.678"),
        ([1, -12345678], "1, -12345678"),
        ([1, 0.001], "1, 0.001"),
    ],
)
def test_print_raw_query_output_numbers(
    module_under_test, capsys, test_input: list, expected_output: str
):
    module_under_test.print_raw_query_output(test_input)
    captured = capsys.readouterr()
    assert expected_output in captured.out


@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        (["a", "some-string"], "'a', 'some-string'"),
        (["a", "string-with-'single'-quote"], "'a', 'string-with-'single'-quote'"),
        (["a", 'string-with-"double"-quote'], "'a', 'string-with-\"double\"-quote'"),
    ],
)
def test_print_raw_query_output_strings(
    module_under_test, capsys, test_input: list, expected_output: str
):
    module_under_test.print_raw_query_output(test_input)
    captured = capsys.readouterr()
    # breakpoint()
    assert expected_output in captured.out
