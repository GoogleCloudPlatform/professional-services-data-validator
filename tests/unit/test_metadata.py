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


@pytest.fixture
def module_under_test():
    from data_validation import metadata

    return metadata


@pytest.mark.parametrize(
    ("source_column", "target_column", "result_type", "expected"),
    (
        ("source_col", "target_col", "source", "source_col"),
        ("source_col", "target_col", "target", "target_col"),
        (None, None, "source", None),
        (None, None, "target", None),
    ),
)
def test_get_column_name(
    module_under_test, source_column, target_column, result_type, expected
):
    validation = module_under_test.ValidationMetadata(
        "", "", "", "", "", "", source_column, target_column, "", "", ""
    )
    column_name = validation.get_column_name(result_type)
    assert column_name == expected


def test_get_column_name_with_unexpected_result_type(module_under_test):
    validation = module_under_test.ValidationMetadata(
        "", "", "", "", "", "", "", "", "", "", ""
    )
    with pytest.raises(ValueError, match="Unexpected result_type"):
        validation.get_column_name("oops_i_goofed")


@pytest.mark.parametrize(
    (
        "source_table_schema",
        "source_table",
        "target_table_schema",
        "target_table",
        "result_type",
        "expected",
    ),
    (
        (
            "source_table_schema",
            "source_tbl",
            "target_table_schema",
            "target_tbl",
            "source",
            "source_table_schema.source_tbl",
        ),
        (
            "source_table_schema",
            "source_tbl",
            "target_table_schema",
            "target_tbl",
            "target",
            "target_table_schema.target_tbl",
        ),
    ),
)
def test_get_table_name(
    module_under_test,
    source_table_schema,
    source_table,
    target_table_schema,
    target_table,
    result_type,
    expected,
):
    validation = module_under_test.ValidationMetadata(
        "",
        "",
        source_table_schema,
        source_table,
        target_table_schema,
        target_table,
        None,
        None,
        "",
        "",
        "",
    )
    table_name = validation.get_table_name(result_type)
    assert table_name == expected


def test_get_table_name_with_unexpected_result_type(module_under_test):
    validation = module_under_test.ValidationMetadata(
        "", "", "", "", "", "", "", "", "", "", ""
    )
    with pytest.raises(ValueError, match="Unexpected result_type"):
        validation.get_table_name("oops_i_goofed")
