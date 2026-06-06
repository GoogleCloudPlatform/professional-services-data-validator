# Copyright 2023 Google Inc.
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
from typing import Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.types as sat
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects.oracle.oracledb import OracleDialect_oracledb

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy.datatypes import AlchemyType
import oracledb

class OracleType(AlchemyType):
    dialect = "oracle"

    @classmethod
    def from_ibis(cls, dtype: dt.DataType) -> sat.TypeEngine:
        if isinstance(dtype, dt.String):
            return sa.sql.sqltypes.String(length=4000)
        elif isinstance(dtype, dt.Float32):
            return sat.Float(precision=23).with_variant(
                oracle.FLOAT(), "oracle"
            )
        elif isinstance(dtype, dt.Float64):
            return sat.Float(precision=53).with_variant(
                oracle.FLOAT(), "oracle"
            )
        return super().from_ibis(dtype)

    @classmethod
    def to_ibis(cls, typ: sat.TypeEngine, nullable: bool = True) -> dt.DataType:
        if isinstance(typ, (oracle.CLOB, oracle.NCLOB, oracle.LONG, oracle.ROWID, oracle.VARCHAR2, oracle.VARCHAR, oracle.NVARCHAR, oracle.NVARCHAR2, oracle.CHAR, oracle.NCHAR)):
            return dt.String(nullable=nullable)
        elif isinstance(typ, oracle.NUMBER):
            return dt.Decimal(typ.precision, typ.scale, nullable=nullable)
        elif isinstance(typ, oracle.FLOAT):
            return dt.Decimal(nullable=nullable)
        elif isinstance(typ, (oracle.BFILE, oracle.RAW, oracle.BLOB)):
            return dt.Binary(nullable=nullable)
        elif isinstance(typ, oracle.DATE):
            return dt.Date(nullable=nullable)
        elif isinstance(typ, oracle.TIMESTAMP):
            if typ.timezone:
                return dt.Timestamp(timezone="UTC", nullable=nullable)
            else:
                return dt.Timestamp(nullable=nullable)
        elif isinstance(typ, (oracle.INTERVAL, sat.Interval)):
            return dt.Interval(nullable=nullable)
        elif isinstance(typ, oracle.BINARY_FLOAT):
            return dt.Float32(nullable=nullable)
        elif isinstance(typ, oracle.BINARY_DOUBLE):
            return dt.Float64(nullable=nullable)
        else:
            return super().to_ibis(typ, nullable=nullable)


class _FieldDescription(TypedDict):
    """Type of result of oracledb.Cursor.description."""

    name: Optional[str]
    data_type: Optional[str]
    display_size: Optional[int]
    internal_size: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    null_ok: Optional[int]


def _get_type(col: _FieldDescription) -> dt.DataType:
    typename = col[1]
    typ = _type_mapping.get(typename)
    if typ is None:
        raise NotImplementedError(f"Oracle type {typename} is not supported")

    if typename == oracledb.DB_TYPE_NUMBER:
        if col[5] == -127:
            # This will occur if type is NUMBER with no precision/scale or if type is FLOAT.
            typ = partial(typ)
        else:
            typ = partial(typ, precision=col[4], scale=col[5])

    return typ(nullable=bool(col[6]))


_type_mapping = {
    oracledb.DB_TYPE_VARCHAR: dt.String,
    oracledb.DB_TYPE_NVARCHAR: dt.String,
    oracledb.DB_TYPE_CHAR: dt.String,
    oracledb.DB_TYPE_NCHAR: dt.String,
    oracledb.DB_TYPE_DATE: dt.Date,
    oracledb.DB_TYPE_TIMESTAMP: dt.Timestamp,
    oracledb.DB_TYPE_TIMESTAMP_TZ: dt.Timestamp(timezone="UTC"),
    oracledb.DB_TYPE_TIMESTAMP_LTZ: dt.Timestamp(timezone="UTC"),
    oracledb.DB_TYPE_RAW: dt.Binary,
    oracledb.DB_TYPE_LONG_RAW: dt.Binary,
    oracledb.DB_TYPE_BFILE: dt.Binary,
    oracledb.DB_TYPE_NUMBER: dt.Decimal,
    oracledb.DB_TYPE_LONG: dt.String,
    oracledb.DB_TYPE_NCLOB: dt.String,
    oracledb.DB_TYPE_CLOB: dt.String,
    oracledb.DB_TYPE_BLOB: dt.Binary,
    oracledb.DB_TYPE_BINARY_FLOAT: dt.Float32,
    oracledb.DB_TYPE_BINARY_DOUBLE: dt.Float64,
    oracledb.DB_TYPE_INTERVAL_DS: dt.Interval,
    oracledb.DB_TYPE_INTERVAL_YM: dt.Interval,
    oracledb.DB_TYPE_BOOLEAN: dt.Boolean,
}

# SQL Alchemy doesn't support LONG RAW which drops us into Ibis 5.1.0 method:
# _handle_failed_column_type_inference().
# This method has a flaw that is doesn't prefix the table name with the schema
# which causes a table not found exception.
# I (nj@2024-02-15) felt the simplest change was to add LONG RAW to SQLAlchemy
# as a RAW variant, as below.
if "LONG RAW" not in OracleDialect_oracledb.ischema_names:
    OracleDialect_oracledb.ischema_names["LONG RAW"] = oracle.RAW
# Same as above but for LOCAL TIME ZONE.
if "TIMESTAMP WITH LOCAL TIME ZONE" not in OracleDialect_oracledb.ischema_names:
    OracleDialect_oracledb.ischema_names["TIMESTAMP WITH LOCAL TIME ZONE"] = (
        oracle.TIMESTAMP(timezone=True)
    )

if "BOOLEAN" not in OracleDialect_oracledb.ischema_names:
    OracleDialect_oracledb.ischema_names["BOOLEAN"] = sat.BOOLEAN
