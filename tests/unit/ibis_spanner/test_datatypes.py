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

from google.cloud.spanner_v1.types import StructType, Type, TypeCode
import ibis.expr.datatypes as dt
import pytest

from third_party.ibis.ibis_cloud_spanner import Backend
from third_party.ibis.ibis_cloud_spanner.datatypes import (
    dtype_from_spanner_field,
    schema_from_spanner,
)


@pytest.mark.parametrize(
    ("type_code", "expected_dtype"),
    [
        (TypeCode.INT64, dt.int64),
        (TypeCode.FLOAT32, dt.float32),
        (TypeCode.FLOAT64, dt.float64),
        (TypeCode.BOOL, dt.boolean),
        (TypeCode.STRING, dt.string),
        (TypeCode.DATE, dt.date),
        (TypeCode.TIMESTAMP, dt.Timestamp(timezone="UTC")),
        (TypeCode.BYTES, dt.binary),
        (TypeCode.NUMERIC, dt.Decimal(38, 9)),
        (TypeCode.JSON, dt.json),
    ],
)
def test_dtype_from_spanner_field(type_code, expected_dtype):
    dtype = dtype_from_spanner_field(Type(code=type_code))

    assert dtype == expected_dtype
    assert isinstance(dtype, dt.DataType)


def test_schema_from_spanner_with_float32():
    fields = [
        StructType.Field(name="id", type_=Type(code=TypeCode.INT64)),
        StructType.Field(name="col_float32", type_=Type(code=TypeCode.FLOAT32)),
        StructType.Field(name="col_float64", type_=Type(code=TypeCode.FLOAT64)),
    ]

    schema = schema_from_spanner(fields)

    assert schema["id"] == dt.int64
    assert schema["col_float32"] == dt.float32
    assert schema["col_float64"] == dt.float64


def test_backend_get_schema_with_float32():
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()
    backend.instance.database.return_value.table.return_value.schema = [
        StructType.Field(name="id", type_=Type(code=TypeCode.INT64)),
        StructType.Field(name="col_float32", type_=Type(code=TypeCode.FLOAT32)),
    ]

    schema = backend.get_schema("dvt_core_types")

    assert schema["id"] == dt.int64
    assert schema["col_float32"] == dt.float32


def test_backend_get_schema_using_query_with_float32():
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()

    mock_result = mock.MagicMock()
    mock_result.__iter__.return_value = iter([[1, 1.5]])
    mock_result.fields = [
        StructType.Field(name="id", type_=Type(code=TypeCode.INT64)),
        StructType.Field(name="col_float32", type_=Type(code=TypeCode.FLOAT32)),
    ]
    mock_snapshot = (
        backend.instance.database.return_value.snapshot.return_value.__enter__.return_value
    )
    mock_snapshot.execute_sql.return_value = mock_result

    schema = backend._get_schema_using_query(
        "SELECT id, col_float32 FROM dvt_core_types"
    )

    assert schema["id"] == dt.int64
    assert schema["col_float32"] == dt.float32
