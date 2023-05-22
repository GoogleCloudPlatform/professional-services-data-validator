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

import google.cloud.spanner_v1 as spanner
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from google.cloud.spanner_v1.types import TypeCode

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


@dt.dtype.register(object)
def spanner_field_to_ibis_dtype(field):
    """Convert Spanner `Type` to an ibis type."""
    typ = TypeCode(field.code)
    if typ == TypeCode.ARRAY:
        ibis_type = dt.Array(field.array_element_type)
    ibis_type = _DTYPE_TO_IBIS_TYPE.get(typ, typ)
    return ibis_type


@sch.infer.register(spanner.table.Table)
def spanner_schema(table):
    """Infer the schema of a Spanner `table` object."""
    fields = {el.name: dt.dtype(el.type_) for el in table.schema}
    return sch.schema(fields)
