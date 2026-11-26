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
        (TypeCode.UUID, dt.uuid),
    ],
)
def test_dtype_from_spanner_field(type_code, expected_dtype):
    dtype = dtype_from_spanner_field(Type(code=type_code))

    assert dtype == expected_dtype
    assert isinstance(dtype, dt.DataType)


@pytest.mark.parametrize(
    ("element_type_code", "expected_dtype"),
    [
        (TypeCode.INT64, dt.Array(dt.int64)),
        (TypeCode.STRING, dt.Array(dt.string)),
        (TypeCode.NUMERIC, dt.Array(dt.Decimal(38, 9))),
        (TypeCode.UUID, dt.Array(dt.uuid)),
    ],
)
def test_dtype_from_spanner_field_array(element_type_code, expected_dtype):
    field = Type(code=TypeCode.ARRAY, array_element_type=Type(code=element_type_code))

    dtype = dtype_from_spanner_field(field)

    assert dtype == expected_dtype


@pytest.mark.parametrize(
    "type_code",
    [
        getattr(TypeCode, name)
        for name in (
            "STRUCT",
            "PROTO",
            "ENUM",
            "INTERVAL",
            "TYPE_CODE_UNSPECIFIED",
        )
        if hasattr(TypeCode, name)
    ],
)
def test_dtype_from_spanner_field_unsupported(type_code):
    with pytest.raises(
        TypeError, match=f"^Unsupported Spanner data type: {type_code.name}$"
    ):
        dtype_from_spanner_field(Type(code=type_code))


def test_dtype_from_spanner_field_array_of_unsupported():
    field = Type(code=TypeCode.ARRAY, array_element_type=Type(code=TypeCode.STRUCT))

    with pytest.raises(TypeError, match="^Unsupported Spanner data type: STRUCT$"):
        dtype_from_spanner_field(field)


@pytest.mark.filterwarnings("ignore:Unrecognized TypeCode enum value")
def test_dtype_from_spanner_field_unknown_type_code():
    """A type code newer than the installed google-cloud-spanner arrives as a raw int."""
    with pytest.raises(TypeError, match="^Unsupported Spanner data type: 999$"):
        dtype_from_spanner_field(Type(code=999))


@pytest.mark.filterwarnings("ignore:Unrecognized TypeCode enum value")
def test_dtype_from_spanner_field_array_of_unknown_type_code():
    field = Type(code=TypeCode.ARRAY, array_element_type=Type(code=999))

    with pytest.raises(TypeError, match="^Unsupported Spanner data type: 999$"):
        dtype_from_spanner_field(field)


def test_backend_get_schema_with_unsupported_type():
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()
    backend.instance.database.return_value.table.return_value.schema = [
        StructType.Field(name="id", type_=Type(code=TypeCode.INT64)),
        StructType.Field(name="col_struct", type_=Type(code=TypeCode.STRUCT)),
    ]

    with pytest.raises(TypeError, match="^Unsupported Spanner data type: STRUCT$"):
        backend.get_schema("dvt_core_types")


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


def test_backend_get_schema_marks_not_null_columns():
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()
    database = backend.instance.database.return_value
    database.table.return_value.schema = [
        StructType.Field(name="col_nn", type_=Type(code=TypeCode.INT64)),
        StructType.Field(name="col_nullable", type_=Type(code=TypeCode.STRING)),
    ]
    mock_snapshot = database.snapshot.return_value.__enter__.return_value
    mock_snapshot.execute_sql.return_value = iter([["col_nn"]])

    schema = backend.get_schema("dvt_null_not_null")

    assert schema["col_nn"] == dt.Int64(nullable=False)
    assert not schema["col_nn"].nullable
    assert schema["col_nullable"].nullable
    assert list(schema.names) == ["col_nn", "col_nullable"]
    sql = mock_snapshot.execute_sql.call_args.args[0]
    assert "is_nullable = 'NO'" in sql
    assert "LOWER(table_name) = LOWER(@table_name)" in sql
    assert mock_snapshot.execute_sql.call_args.kwargs["params"] == {
        "table_name": "dvt_null_not_null"
    }


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


def test_backend_get_schema_with_uuid():
    """UUID columns map to Ibis UUID, keeping NOT NULL (issue-1881)."""
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()
    database = backend.instance.database.return_value
    database.table.return_value.schema = [
        StructType.Field(name="id", type_=Type(code=TypeCode.UUID)),
        StructType.Field(name="col_uuid", type_=Type(code=TypeCode.UUID)),
    ]
    mock_snapshot = database.snapshot.return_value.__enter__.return_value
    mock_snapshot.execute_sql.return_value = iter([["id"]])

    schema = backend.get_schema("dvt_uuid_id")

    assert schema["id"] == dt.UUID(nullable=False)
    assert schema["col_uuid"] == dt.uuid
    assert schema["col_uuid"].nullable


def test_backend_get_schema_using_query_with_uuid():
    """Custom query schemas map UUID columns to Ibis UUID (issue-1881)."""
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()

    mock_result = mock.MagicMock()
    mock_result.__iter__.return_value = iter([[1, None]])
    mock_result.fields = [
        StructType.Field(name="id", type_=Type(code=TypeCode.INT64)),
        StructType.Field(name="col_uuid", type_=Type(code=TypeCode.UUID)),
    ]
    mock_snapshot = (
        backend.instance.database.return_value.snapshot.return_value.__enter__.return_value
    )
    mock_snapshot.execute_sql.return_value = mock_result

    schema = backend._get_schema_using_query("SELECT id, col_uuid FROM dvt_uuid_id")

    assert schema["id"] == dt.int64
    assert schema["col_uuid"] == dt.uuid
