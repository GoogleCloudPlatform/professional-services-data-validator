# Copyright 2025 Google Inc.
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

from functools import partial

from sqlalchemy.types import DATETIME
from sqlalchemy_sybase.base import SybaseDialect, ischema_names
import ibis.expr.datatypes as dt


ischema_names["bigdatetime"] = DATETIME

_type_mapping = {
    # Exact numerics
    "bigint": dt.Int64,
    "bit": dt.Boolean,
    "decimal": dt.Decimal,
    "int": dt.Int32,
    "money": dt.Int64,
    "numeric": dt.Decimal,
    "smallint": dt.Int16,
    "smallmoney": dt.Int32,
    "tinyint": dt.Int8,
    # Approximate numerics
    "float": dt.Float64,
    "real": dt.Float32,
    # Date and time
    "date": dt.Date,
    "datetime": dt.Timestamp,
    "bigdatetime": dt.Timestamp,
    "smalldatetime": dt.Timestamp,
    "time": dt.Time,
    # Character string
    "char": dt.String,
    "text": dt.String,
    "varchar": dt.String,
    # Unicode character strings
    "nchar": dt.String,
    "ntext": dt.String,
    "nvarchar": dt.String,
    # Binary string
    "binary": dt.Binary,
    "image": dt.Binary,
    "varbinary": dt.Binary,
    # Other data types
    "uniqueidentifier": dt.UUID,
    # This timestamp datatype is also known as "rowversion", and the original name is really unfortunate.
    # See:
    # https://learn.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-ver16
    "timestamp": dt.Binary,
}


# def _type_from_result_set_info(col: _FieldDescription) -> dt.DataType:
def type_from_result_set_info(
    col_name: str, type_name: str, precision: int, scale: int, nullable: int
) -> dt.DataType:
    """Construct an ibis type from Sybase result set description."""
    typ = _type_mapping.get(type_name)
    breakpoint()
    if typ is None:
        raise NotImplementedError(f"Sybase type {type_name} is not supported")

    if type_name == "decimal":
        typ = partial(typ, precision=precision, scale=scale)
    # elif type_name in ("GEOMETRY", "GEOGRAPHY"):
    #    typ = partial(typ, geotype=type_name.lower())
    # elif type_name == "DATETIME2":
    #    typ = partial(typ, scale=scale)
    # elif type_name == "DATETIMEOFFSET":
    #    typ = partial(typ, scale=col["scale"], timezone="UTC")
    # elif type_name == "FLOAT":
    #    if col["precision"] <= 24:
    #        typ = dt.Float32
    #    else:
    #        typ = dt.Float64
    return typ(nullable=bool(nullable))
