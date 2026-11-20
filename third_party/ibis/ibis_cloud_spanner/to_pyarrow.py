# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build PyArrow record batches from Spanner query results.

Spanner has no native Arrow API. Rather than letting PyArrow infer column types from the
Python values returned by google-cloud-spanner, each column's Arrow type comes from the
Spanner result's field types. This matters because:
  - BYTES values are returned as base64 text and must be decoded.
  - JSON values are returned as JsonObject (a dict) and must be serialized to strings.
  - NUMERIC precision, and the type of all-NULL columns, would otherwise be guessed per batch.
"""

import base64
import itertools
from typing import Callable, List, Optional, Tuple

import pyarrow as pa
from google.cloud.spanner_v1.types import Type, TypeCode

# TIMESTAMP is microseconds: Arrow nanosecond timestamps cannot hold Spanner's full
# range (years 0001 to 9999), sub-microsecond digits are dropped.
_SCALAR_ARROW_TYPES = {
    TypeCode.BOOL: pa.bool_(),
    TypeCode.INT64: pa.int64(),
    TypeCode.FLOAT64: pa.float64(),
    TypeCode.FLOAT32: pa.float32(),
    TypeCode.STRING: pa.string(),
    TypeCode.BYTES: pa.binary(),
    TypeCode.DATE: pa.date32(),
    TypeCode.TIMESTAMP: pa.timestamp("us", tz="UTC"),
    TypeCode.NUMERIC: pa.decimal128(38, 9),
    TypeCode.JSON: pa.string(),
}


def _type_code(field_type: Type) -> TypeCode:
    try:
        return TypeCode(field_type.code)
    except ValueError:
        # A type newer than the installed google-cloud-spanner arrives as a raw int.
        raise TypeError(f"Unsupported Spanner data type: {field_type.code}") from None


def arrow_type_from_spanner_field(field_type: Type) -> pa.DataType:
    """Return the Arrow type for a Spanner field type."""
    typ = _type_code(field_type)
    if typ == TypeCode.ARRAY:
        return pa.list_(arrow_type_from_spanner_field(field_type.array_element_type))
    try:
        return _SCALAR_ARROW_TYPES[typ]
    except KeyError:
        raise TypeError(f"Unsupported Spanner data type: {typ.name}") from None


def _value_converter(field_type: Type) -> Optional[Callable]:
    """Return a function converting a non-NULL client value, or None if none is needed."""
    typ = _type_code(field_type)
    if typ == TypeCode.BYTES:
        return base64.b64decode
    if typ == TypeCode.JSON:
        return lambda value: value.serialize()
    if typ == TypeCode.ARRAY:
        element_converter = _value_converter(field_type.array_element_type)
        if element_converter is None:
            return None
        return lambda value: [
            None if element is None else element_converter(element) for element in value
        ]
    return None


def _to_arrow_array(
    values: tuple, arrow_type: pa.DataType, converter: Optional[Callable]
) -> pa.Array:
    if converter is not None:
        values = [None if value is None else converter(value) for value in values]
    return pa.array(values, type=arrow_type)


def record_batches_from_result(
    result, chunk_size: int = 1_000_000
) -> Tuple[Optional[pa.Schema], List[pa.RecordBatch]]:
    """Read a Spanner StreamedResultSet into typed PyArrow record batches.

    The result must be read while its snapshot is open.

    Returns:
        The Arrow schema and record batches. The schema is None if the result has no field
        metadata, the caller should then fall back to the expected schema.
    """
    rows = iter(result)
    schema = None
    converters = []
    batches = []
    while True:
        chunk = list(itertools.islice(rows, chunk_size))
        if schema is None:
            # Field metadata is only populated once the first response has been read.
            fields = result.fields
            if not fields:
                return None, []
            schema = pa.schema(
                [
                    pa.field(field.name, arrow_type_from_spanner_field(field.type_))
                    for field in fields
                ]
            )
            converters = [_value_converter(field.type_) for field in fields]
        if not chunk:
            break
        columns = zip(*chunk)
        arrays = [
            _to_arrow_array(values, field.type, converter)
            for values, field, converter in zip(columns, schema, converters)
        ]
        batches.append(pa.RecordBatch.from_arrays(arrays, schema=schema))
        if len(chunk) < chunk_size:
            break
    return schema, batches
