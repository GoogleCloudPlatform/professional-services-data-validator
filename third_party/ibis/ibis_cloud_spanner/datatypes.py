# Copyright 2021 Google Inc.
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
from typing import List

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from google.cloud.spanner_v1.types import Type, TypeCode, StructType

_DTYPE_TO_IBIS_TYPE = {
    TypeCode.INT64: dt.int64,
    TypeCode.FLOAT64: dt.double,
    TypeCode.BOOL: dt.boolean,
    TypeCode.STRING: dt.string,
    TypeCode.DATE: dt.date,
    TypeCode.TIMESTAMP: dt.Timestamp(timezone="UTC"),
    TypeCode.BYTES: dt.binary,
    TypeCode.NUMERIC: dt.Decimal(38, 9),
    TypeCode.JSON: dt.json,
}


def dtype_from_spanner_field(field: Type) -> dt.DataType:
    """Convert Spanner `Type` to an ibis type."""
    typ = TypeCode(field.code)
    if typ == TypeCode.ARRAY:
        ibis_type = dt.Array(field.array_element_type)
    ibis_type = _DTYPE_TO_IBIS_TYPE.get(typ, typ)
    return ibis_type


def schema_from_spanner(fields: List[StructType.Field]) -> sch.Schema:
    return sch.Schema({f.name: dtype_from_spanner_field(f.type_) for f in fields})
