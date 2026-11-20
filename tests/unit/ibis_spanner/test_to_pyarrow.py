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

import base64
import datetime
import decimal
from unittest import mock

from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.types import StructType, Type, TypeCode
import ibis
import pyarrow
import pytest

# Ensure DVT's to_pyarrow() patch is applied as it would be in a real DVT execution.
import third_party.ibis.ibis_addon.operations  # noqa: F401
from third_party.ibis.ibis_cloud_spanner import Backend
from third_party.ibis.ibis_cloud_spanner.to_pyarrow import (
    arrow_type_from_spanner_field,
    record_batches_from_result,
)


class _FakeResult:
    """Stand-in for google-cloud-spanner's StreamedResultSet.

    Rows are returned as the client would decode them, field metadata is only available
    once reading has started, and reading after the snapshot has closed fails.
    """

    def __init__(self, fields, rows, snapshot=None):
        self._fields = fields
        self._rows = rows
        self._snapshot = snapshot
        self._started = False

    def __iter__(self):
        self._started = True
        for row in self._rows:
            if self._snapshot is not None:
                assert not self._snapshot.closed, "row read after the snapshot closed"
            yield row

    @property
    def fields(self):
        return self._fields if self._started else []


class _FakeSnapshot:
    def __init__(self, fields, rows):
        self._fields = fields
        self._rows = rows
        self.closed = False
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def execute_sql(self, sql):
        self.sql = sql
        return _FakeResult(self._fields, self._rows, snapshot=self)


def _field(name, code, element_code=None):
    if element_code is None:
        return StructType.Field(name=name, type_=Type(code=code))
    return StructType.Field(
        name=name,
        type_=Type(code=code, array_element_type=Type(code=element_code)),
    )


def _b64(value: bytes) -> bytes:
    """BYTES values as the Spanner client returns them: base64 text as bytes."""
    return base64.b64encode(value)


@pytest.mark.parametrize(
    ("type_code", "expected_type"),
    [
        (TypeCode.BOOL, pyarrow.bool_()),
        (TypeCode.INT64, pyarrow.int64()),
        (TypeCode.FLOAT64, pyarrow.float64()),
        (TypeCode.FLOAT32, pyarrow.float32()),
        (TypeCode.STRING, pyarrow.string()),
        (TypeCode.BYTES, pyarrow.binary()),
        (TypeCode.DATE, pyarrow.date32()),
        (TypeCode.TIMESTAMP, pyarrow.timestamp("us", tz="UTC")),
        (TypeCode.NUMERIC, pyarrow.decimal128(38, 9)),
        (TypeCode.JSON, pyarrow.string()),
    ],
)
def test_arrow_type_from_spanner_field(type_code, expected_type):
    assert arrow_type_from_spanner_field(Type(code=type_code)) == expected_type


@pytest.mark.parametrize(
    ("element_code", "expected_type"),
    [
        (TypeCode.INT64, pyarrow.list_(pyarrow.int64())),
        (TypeCode.BYTES, pyarrow.list_(pyarrow.binary())),
        (TypeCode.NUMERIC, pyarrow.list_(pyarrow.decimal128(38, 9))),
    ],
)
def test_arrow_type_from_spanner_field_array(element_code, expected_type):
    field_type = Type(code=TypeCode.ARRAY, array_element_type=Type(code=element_code))
    assert arrow_type_from_spanner_field(field_type) == expected_type


@pytest.mark.parametrize(
    "type_code",
    [
        TypeCode.STRUCT,
        TypeCode.PROTO,
        TypeCode.ENUM,
        TypeCode.INTERVAL,
        TypeCode.UUID,
        TypeCode.TYPE_CODE_UNSPECIFIED,
    ],
)
def test_arrow_type_from_spanner_field_unsupported(type_code):
    with pytest.raises(
        TypeError, match=f"^Unsupported Spanner data type: {type_code.name}$"
    ):
        arrow_type_from_spanner_field(Type(code=type_code))


@pytest.mark.filterwarnings("ignore:Unrecognized TypeCode enum value")
def test_arrow_type_from_spanner_field_unknown_type_code():
    with pytest.raises(TypeError, match="^Unsupported Spanner data type: 999$"):
        arrow_type_from_spanner_field(Type(code=999))


ALL_TYPE_FIELDS = [
    _field("col_bool", TypeCode.BOOL),
    _field("col_int64", TypeCode.INT64),
    _field("col_float64", TypeCode.FLOAT64),
    _field("col_float32", TypeCode.FLOAT32),
    _field("col_string", TypeCode.STRING),
    _field("col_bytes", TypeCode.BYTES),
    _field("col_date", TypeCode.DATE),
    _field("col_timestamp", TypeCode.TIMESTAMP),
    _field("col_numeric", TypeCode.NUMERIC),
    _field("col_json", TypeCode.JSON),
]
ALL_TYPE_ROW = [
    True,
    -9223372036854775808,
    1.5,
    0.25,
    "Ünïcödé",
    _b64(b"\x00\xffbinary"),
    datetime.date(1, 1, 1),
    DatetimeWithNanoseconds(
        9999, 12, 31, 23, 59, 59, nanosecond=999999999, tzinfo=datetime.timezone.utc
    ),
    decimal.Decimal("99999999999999999999999999999.999999999"),
    JsonObject({"a": 1, "b": [1, 2]}),
]
NULL_ROW = [None] * len(ALL_TYPE_FIELDS)


def test_record_batches_from_result_converts_values():
    schema, batches = record_batches_from_result(
        _FakeResult(ALL_TYPE_FIELDS, [ALL_TYPE_ROW, NULL_ROW])
    )

    assert schema.names == [field.name for field in ALL_TYPE_FIELDS]
    assert len(batches) == 1
    rows = pyarrow.Table.from_batches(batches).to_pylist()
    assert rows[0] == {
        "col_bool": True,
        "col_int64": -9223372036854775808,
        "col_float64": 1.5,
        "col_float32": 0.25,
        "col_string": "Ünïcödé",
        # Decoded from base64 rather than the base64 text itself.
        "col_bytes": b"\x00\xffbinary",
        "col_date": datetime.date(1, 1, 1),
        # Microseconds, the nanosecond digits are dropped.
        "col_timestamp": datetime.datetime(
            9999, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc
        ),
        "col_numeric": decimal.Decimal("99999999999999999999999999999.999999999"),
        "col_json": '{"a":1,"b":[1,2]}',
    }
    assert rows[1] == {field.name: None for field in ALL_TYPE_FIELDS}


def test_record_batches_from_result_all_null_column_keeps_spanner_type():
    schema, batches = record_batches_from_result(
        _FakeResult([_field("col_numeric", TypeCode.NUMERIC)], [[None], [None]])
    )

    assert schema.field("col_numeric").type == pyarrow.decimal128(38, 9)
    assert batches[0].column(0).type == pyarrow.decimal128(38, 9)


def test_record_batches_from_result_arrays():
    fields = [
        _field("col_int64s", TypeCode.ARRAY, TypeCode.INT64),
        _field("col_bytes_array", TypeCode.ARRAY, TypeCode.BYTES),
        _field("col_json_array", TypeCode.ARRAY, TypeCode.JSON),
    ]
    rows = [
        [[1, None, 3], [_b64(b"a"), None], [JsonObject({"k": "v"}), None]],
        [None, None, None],
    ]

    _, batches = record_batches_from_result(_FakeResult(fields, rows))

    assert pyarrow.Table.from_batches(batches).to_pylist() == [
        {
            "col_int64s": [1, None, 3],
            "col_bytes_array": [b"a", None],
            "col_json_array": ['{"k":"v"}', None],
        },
        {"col_int64s": None, "col_bytes_array": None, "col_json_array": None},
    ]


@pytest.mark.parametrize(
    ("row_count", "chunk_size", "expected_batch_sizes"),
    [
        (5, 2, [2, 2, 1]),
        # An exact multiple of chunk_size must not produce an empty trailing batch.
        (4, 2, [2, 2]),
        (3, 10, [3]),
    ],
)
def test_record_batches_from_result_chunks(row_count, chunk_size, expected_batch_sizes):
    fields = [_field("id", TypeCode.INT64), _field("col_bytes", TypeCode.BYTES)]
    rows = [[i, _b64(bytes([i]))] for i in range(row_count)]

    schema, batches = record_batches_from_result(
        _FakeResult(fields, rows), chunk_size=chunk_size
    )

    assert [batch.num_rows for batch in batches] == expected_batch_sizes
    assert all(batch.schema == schema for batch in batches)
    assert pyarrow.Table.from_batches(batches).column("col_bytes").to_pylist() == [
        bytes([i]) for i in range(row_count)
    ]


def test_record_batches_from_result_no_rows():
    fields = [_field("id", TypeCode.INT64), _field("col_json", TypeCode.JSON)]

    schema, batches = record_batches_from_result(_FakeResult(fields, []))

    assert schema == pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64()),
            pyarrow.field("col_json", pyarrow.string()),
        ]
    )
    assert batches == []


def test_record_batches_from_result_no_field_metadata():
    assert record_batches_from_result(_FakeResult([], [])) == (None, [])


def test_record_batches_from_result_unsupported_type():
    with pytest.raises(TypeError, match="^Unsupported Spanner data type: STRUCT$"):
        record_batches_from_result(
            _FakeResult([_field("col_struct", TypeCode.STRUCT)], [[("a", 1)]])
        )


IBIS_SCHEMA = [
    ("col_bool", "boolean"),
    ("col_int64", "int64"),
    ("col_float64", "float64"),
    ("col_float32", "float32"),
    ("col_string", "string"),
    ("col_bytes", "binary"),
    ("col_date", "date"),
    ("col_timestamp", "timestamp('UTC')"),
    ("col_numeric", "decimal(38, 9)"),
    ("col_json", "json"),
]


def _backend(fields, rows):
    """Return a Backend whose database returns a new fake snapshot per call, and the snapshots."""
    backend = Backend()
    backend.dataset = "pso_data_validator"
    backend.instance = mock.MagicMock()
    backend.database_name = mock.MagicMock()
    snapshots = []

    def _new_snapshot():
        snapshots.append(_FakeSnapshot(fields, rows))
        return snapshots[-1]

    backend.database_name.snapshot.side_effect = _new_snapshot
    return backend, snapshots


def test_backend_to_pyarrow_reuses_connected_database():
    """Creating a Database per query leaks a session pool and a maintenance thread."""
    backend, _ = _backend(ALL_TYPE_FIELDS, [ALL_TYPE_ROW])
    table = ibis.table(IBIS_SCHEMA, name="dvt_core_types")

    backend.to_pyarrow(table)
    backend.to_pyarrow(table)

    backend.instance.database.assert_not_called()
    assert backend.database_name.snapshot.call_count == 2


def test_backend_to_pyarrow():
    """End to end through DVT's to_pyarrow(), reading rows inside the snapshot."""
    backend, snapshots = _backend(ALL_TYPE_FIELDS, [ALL_TYPE_ROW, NULL_ROW])
    table = ibis.table(IBIS_SCHEMA, name="dvt_core_types")

    result = backend.to_pyarrow(table)

    assert [s.closed for s in snapshots] == [True]
    assert "dvt_core_types" in snapshots[0].sql
    assert result.num_rows == 2
    assert result.schema.names == [name for name, _ in IBIS_SCHEMA]
    assert result.schema.field("col_numeric").type == pyarrow.decimal128(38, 9)
    assert result.schema.field("col_timestamp").type == pyarrow.timestamp(
        "us", tz="UTC"
    )
    assert result.schema.field("col_bytes").type == pyarrow.binary()
    assert result.column("col_bytes").to_pylist() == [b"\x00\xffbinary", None]
    assert result.column("col_json").to_pylist() == ['{"a":1,"b":[1,2]}', None]


def test_backend_to_pyarrow_no_rows():
    backend, snapshots = _backend(ALL_TYPE_FIELDS, [])
    table = ibis.table(IBIS_SCHEMA, name="dvt_core_types")

    result = backend.to_pyarrow(table)

    assert [s.closed for s in snapshots] == [True]
    assert result.num_rows == 0
    assert result.schema == table.schema().to_pyarrow()


def test_backend_to_pyarrow_batches_chunk_size():
    fields = [_field("id", TypeCode.INT64)]
    backend, _ = _backend(fields, [[i] for i in range(5)])
    table = ibis.table([("id", "int64")], name="dvt_core_types")

    reader = backend.to_pyarrow_batches(table, chunk_size=2)

    assert [batch.num_rows for batch in reader] == [2, 2, 1]


def test_backend_to_pyarrow_aggregate():
    """Column validations fetch a single aggregate row."""
    fields = [_field("count", TypeCode.INT64), _field("sum__col", TypeCode.NUMERIC)]
    backend, _ = _backend(fields, [[3, decimal.Decimal("6.5")]])
    table = ibis.table([("col", "decimal(38, 9)")], name="dvt_core_types")
    expr = table.aggregate(count=table.count(), sum__col=table.col.sum())

    result = backend.to_pyarrow(expr)

    assert result.to_pylist() == [{"count": 3, "sum__col": decimal.Decimal("6.5")}]
