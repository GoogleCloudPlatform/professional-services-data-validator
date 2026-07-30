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

import ibis
import pandas

from data_validation.query_builder.query_builder import FilterField

from third_party.ibis.ibis_bigquery import Backend as BigQueryBackend
from third_party.ibis.ibis_cloud_spanner import Backend as SpannerBackend


def test_filter_field_and():
    field1 = FilterField.equal_to("col1", "val1")
    field2 = FilterField.equal_to("col2", 2)
    and_field = FilterField.and_([field1, field2])

    assert and_field.expr == ibis.and_
    assert and_field.left == [field1, field2]


def test_filter_field_is_null():
    null_field = FilterField.is_null("col1")
    assert null_field.expr == ibis.expr.types.ColumnExpr.isnull
    assert null_field.left_field == "col1"


def test_composite_isin_tuple_in_path():
    mock_client = mock.MagicMock()
    mock_client.name = "postgres"
    mock_client.dvt_tuple_in_supported.return_value = True

    df = pandas.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})
    ff = FilterField.composite_isin(mock_client, ["col1", "col2"], df)

    assert ff.expr == "tuple_in"
    assert ff.left == ["col1", "col2"]
    assert ff.right == [("a", 1), ("b", 2)]


def test_composite_isin_or_of_ands_fallback():
    mock_client = mock.MagicMock()
    mock_client.name = "mssql"
    mock_client.dvt_tuple_in_supported.return_value = False

    df = pandas.DataFrame({"col1": ["a", "b"], "col2": [1, None]})
    ff = FilterField.composite_isin(mock_client, ["col1", "col2"], df)

    assert ff.expr == ibis.or_
    assert len(ff.left) == 2

    # First row: col1='a' AND col2=1
    row1_and = ff.left[0]
    assert row1_and.expr == ibis.and_
    assert row1_and.left[0].left_field == "col1"
    assert row1_and.left[0].right == "a"
    assert row1_and.left[1].left_field == "col2"
    assert row1_and.left[1].right == 1

    # Second row: col1='b' AND col2 IS NULL
    row2_and = ff.left[1]
    assert row2_and.expr == ibis.and_
    assert row2_and.left[0].left_field == "col1"
    assert row2_and.left[0].right == "b"
    assert row2_and.left[1].expr == ibis.expr.types.ColumnExpr.isnull
    assert row2_and.left[1].left_field == "col2"


@mock.patch("data_validation.clients.get_max_in_list_size", return_value=2)
def test_composite_isin_chunking(mock_get_max_in):
    mock_client = mock.MagicMock()
    mock_client.name = "mssql"
    mock_client.dvt_tuple_in_supported.return_value = False

    df = pandas.DataFrame({"col1": ["a", "b", "c", "d", "e"], "col2": [1, 2, 3, 4, 5]})
    ff = FilterField.composite_isin(mock_client, ["col1", "col2"], df)

    assert ff.expr == ibis.or_
    # Chunked into sub-batches of 2
    assert len(ff.left) == 3
    for sub_batch in ff.left:
        assert sub_batch.expr == ibis.or_


@mock.patch("data_validation.clients.get_max_in_list_size", return_value=2)
def test_composite_isin_tuple_in_chunking(mock_get_max_in):
    mock_client = mock.MagicMock()
    mock_client.name = "postgres"
    mock_client.dvt_tuple_in_supported.return_value = True

    df = pandas.DataFrame({"col1": ["a", "b", "c", "d", "e"], "col2": [1, 2, 3, 4, 5]})
    ff = FilterField.composite_isin(mock_client, ["col1", "col2"], df)

    assert ff.expr == ibis.or_
    # Chunked into sub-batches of 2
    assert len(ff.left) == 3
    for sub_batch in ff.left:
        assert sub_batch.expr == "tuple_in"


def test_backend_class_tuple_in_supported():
    try:
        from third_party.ibis.ibis_oracle import Backend as OracleBackend

        assert OracleBackend().dvt_tuple_in_supported() is True
    except ImportError:
        pass

    try:
        from third_party.ibis.ibis_db2 import Backend as Db2Backend

        assert Db2Backend().dvt_tuple_in_supported() is True
    except ImportError:
        pass

    try:
        from third_party.ibis.ibis_db2_zos import Backend as Db2zOSBackend

        assert Db2zOSBackend().dvt_tuple_in_supported() is True
    except ImportError:
        pass

    try:
        from third_party.ibis.ibis_mssql import Backend as MsSqlBackend

        assert MsSqlBackend().dvt_tuple_in_supported() is False
    except ImportError:
        pass

    try:
        from ibis.backends.postgres import Backend as PostgresBackend

        assert PostgresBackend().dvt_tuple_in_supported() is True
    except ImportError:
        pass

    try:
        from ibis.backends.mysql import Backend as MySQLBackend

        assert MySQLBackend().dvt_tuple_in_supported() is True
    except ImportError:
        pass

    try:
        from ibis.backends.snowflake import Backend as SnowflakeBackend

        assert SnowflakeBackend().dvt_tuple_in_supported() is True
    except ImportError:
        pass

    assert BigQueryBackend().dvt_tuple_in_supported() is True
    assert SpannerBackend().dvt_tuple_in_supported() is True

    try:
        from third_party.ibis.ibis_sybase import Backend as SybaseBackend

        assert SybaseBackend().dvt_tuple_in_supported() is False
    except ImportError:
        pass

    try:
        from third_party.ibis.ibis_teradata import Backend as TeradataBackend

        assert TeradataBackend().dvt_tuple_in_supported() is False
    except ImportError:
        pass

    try:
        from third_party.ibis.ibis_redshift import Backend as RedshiftBackend

        assert RedshiftBackend().dvt_tuple_in_supported() is False
    except ImportError:
        pass


def test_filter_field_large_or_compilation():
    """Verify that compiling a large OR tree doesn't raise RecursionError.
    This ensures our balanced binary tree mitigation works."""
    table = ibis.table([("a", "int64"), ("b", "int64")], name="my_table")

    # 1000 items would typically trigger recursion error in sqlglot if parsed linearly
    eq_filters = []
    for i in range(1000):
        # We simulate the fallback row condition: (a=i AND b=i)
        f = FilterField.and_([
            FilterField.equal_to("a", i),
            FilterField.equal_to("b", i)
        ])
        eq_filters.append(f)

    large_or = FilterField.or_(eq_filters)

    try:
        # Compilation should succeed via balanced binary tree
        compiled = large_or.compile(table)
        assert compiled is not None
    except RecursionError:
        import pytest
        pytest.fail("RecursionError encountered while compiling large OR condition.")
