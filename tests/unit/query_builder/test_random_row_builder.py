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

from unittest import mock
import pytest

import data_validation.query_builder.random_row_builder as random_row_builder


@pytest.fixture
def module_under_test():
    return random_row_builder


def test_import(module_under_test):
    assert module_under_test is not None


def test_init(module_under_test):
    primary_keys = ["abc", "def"]
    builder = module_under_test.RandomRowBuilder(primary_keys, 100)

    assert builder.primary_keys == primary_keys
    assert builder.batch_size == 100


def test_maybe_add_random_sort_unsupported():
    mock_client = mock.MagicMock()
    mock_client.name = "unsupported_db"

    mock_table = mock.MagicMock()

    builder = random_row_builder.RandomRowBuilder(["id"], 10)

    result = builder.maybe_add_random_sort(mock_client, mock_table)
    assert result == mock_table


def test_maybe_add_random_sort_teradata_spanner():
    mock_client = mock.MagicMock()
    mock_client.name = "teradata"

    mock_table = mock.MagicMock()
    mock_table_sliced = mock.MagicMock()
    mock_table.__getitem__.return_value = mock_table_sliced
    mock_table_limited = mock.MagicMock()
    mock_table_sliced.limit.return_value = mock_table_limited

    builder = random_row_builder.RandomRowBuilder(["id"], 10)

    result = builder.maybe_add_random_sort(mock_client, mock_table)

    mock_table.__getitem__.assert_called_once_with(["id"])
    mock_table_sliced.limit.assert_called_once_with(10)
    assert result == mock_table_limited


@mock.patch("ibis.random")
def test_maybe_add_random_sort_supported(mock_random):
    mock_client = mock.MagicMock()
    mock_client.name = "bigquery"

    mock_random_expr = mock.MagicMock()
    mock_random.return_value = mock_random_expr

    mock_table = mock.MagicMock()
    mock_table_sliced = mock.MagicMock()
    mock_table_ordered = mock.MagicMock()
    mock_table_limited = mock.MagicMock()

    mock_table.__getitem__.return_value = mock_table_sliced
    mock_table_sliced.order_by.return_value = mock_table_ordered
    mock_table_ordered.limit.return_value = mock_table_limited

    builder = random_row_builder.RandomRowBuilder(["id"], 10)

    result = builder.maybe_add_random_sort(mock_client, mock_table)

    mock_table.__getitem__.assert_called_once_with(["id"])
    mock_table_sliced.order_by.assert_called_once_with(mock_random_expr)
    mock_table_ordered.limit.assert_called_once_with(10)
    assert result == mock_table_limited


@mock.patch("ibis.random")
def test_maybe_add_random_sort_aliased(mock_random):
    mock_client = mock.MagicMock()
    mock_client.name = "db2_zos"

    mock_random_expr = mock.MagicMock()
    mock_random.return_value = mock_random_expr

    mock_table = mock.MagicMock()
    mock_table_sliced = mock.MagicMock()
    mock_table_mutated = mock.MagicMock()
    mock_table_ordered = mock.MagicMock()
    mock_table_limited = mock.MagicMock()
    mock_table_final = mock.MagicMock()

    mock_table.__getitem__.return_value = mock_table_sliced
    mock_table_sliced.mutate.return_value = mock_table_mutated
    mock_table_mutated.order_by.return_value = mock_table_ordered
    mock_table_ordered.limit.return_value = mock_table_limited
    mock_table_limited.__getitem__.return_value = mock_table_final

    builder = random_row_builder.RandomRowBuilder(["id"], 10)

    result = builder.maybe_add_random_sort(mock_client, mock_table)

    mock_table.__getitem__.assert_called_once_with(["id"])
    mock_table_sliced.mutate.assert_called_once_with(
        **{"random_sort": mock_random_expr}
    )
    mock_table_mutated.order_by.assert_called_once_with("random_sort")
    mock_table_ordered.limit.assert_called_once_with(10)
    mock_table_limited.__getitem__.assert_called_once_with(["id"])

    assert result == mock_table_final
