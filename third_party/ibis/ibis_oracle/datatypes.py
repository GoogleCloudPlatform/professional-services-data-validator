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
from sqlalchemy.dialects.oracle.cx_oracle import OracleDialect_cx_oracle

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy.datatypes import ibis_type_to_sqla
import cx_Oracle

# Update to avoid cast to CLOB/Text
ibis_type_to_sqla[dt.String] = sa.sql.sqltypes.String(length=4000)

# This is only required for SQLAlchemy 2.0+
# ibis_type_to_sqla[dt.Float32] = sat.Float(precision=23).with_variant(oracle.FLOAT(), 'oracle')
# ibis_type_to_sqla[dt.Float64] = sat.Float(precision=53).with_variant(oracle.FLOAT(), 'oracle')


class _FieldDescription(TypedDict):
    """Type of result of cx_Oracle.Cursor.description."""

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

    if typename == cx_Oracle.DB_TYPE_NUMBER:
        if col[5] == -127:
            # This will occur if type is NUMBER with no precision/scale or if type is FLOAT.
            typ = partial(typ)
        else:
            typ = partial(typ, precision=col[4], scale=col[5])

    return typ(nullable=bool(col[6]))


_type_mapping = {
    cx_Oracle.DB_TYPE_VARCHAR: dt.String,
    cx_Oracle.DB_TYPE_NVARCHAR: dt.String,
    cx_Oracle.DB_TYPE_CHAR: dt.String,
    cx_Oracle.DB_TYPE_NCHAR: dt.String,
    cx_Oracle.DB_TYPE_DATE: dt.Date,
    cx_Oracle.DB_TYPE_TIMESTAMP: dt.Timestamp,
    cx_Oracle.DB_TYPE_TIMESTAMP_TZ: dt.Timestamp(timezone="UTC"),
    cx_Oracle.DB_TYPE_TIMESTAMP_LTZ: dt.Timestamp(timezone="UTC"),
    cx_Oracle.DB_TYPE_RAW: dt.Binary,
    cx_Oracle.DB_TYPE_LONG_RAW: dt.Binary,
    cx_Oracle.DB_TYPE_BFILE: dt.Binary,
    cx_Oracle.DB_TYPE_NUMBER: dt.Decimal,
    cx_Oracle.DB_TYPE_LONG: dt.String,
    cx_Oracle.DB_TYPE_NCLOB: dt.String,
    cx_Oracle.DB_TYPE_CLOB: dt.String,
    cx_Oracle.DB_TYPE_BLOB: dt.Binary,
    cx_Oracle.DB_TYPE_BINARY_FLOAT: dt.Float32,
    cx_Oracle.DB_TYPE_BINARY_DOUBLE: dt.Float64,
    cx_Oracle.DB_TYPE_INTERVAL_DS: dt.Interval,
}

# SQL Alchemy doesn't support LONG RAW which drops us into Ibis 5.1.0 method:
# _handle_failed_column_type_inference().
# This method has a flaw that is doesn't prefix the table name with the schema
# which causes a table not found exception.
# I (nj@2024-02-15) felt the simplest change was to add LONG RAW to SQL Alchemy
# as a RAW variant, as below.
if "LONG RAW" not in OracleDialect_cx_oracle.ischema_names:
    OracleDialect_cx_oracle.ischema_names["LONG RAW"] = oracle.RAW
# Same as above but for LOCAL TIME ZONE.
if "TIMESTAMP WITH LOCAL TIME ZONE" not in OracleDialect_cx_oracle.ischema_names:
    OracleDialect_cx_oracle.ischema_names[
        "TIMESTAMP WITH LOCAL TIME ZONE"
    ] = oracle.TIMESTAMP(timezone=True)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.CLOB)
def sa_oracle_CLOB(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.NCLOB)
def sa_oracle_NCLOB(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.LONG)
def sa_oracle_LONG(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.NUMBER)
def sa_oracle_NUMBER(_, satype, nullable=True):
    return dt.Decimal(satype.precision, satype.scale, nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.FLOAT)
def sa_oracle_FLOAT(_, satype, nullable=True):
    # Oracle FLOAT is a NUMBER under the hood.
    return dt.Decimal(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.BFILE)
def sa_oracle_BFILE(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.RAW)
def sa_oracle_RAW(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.DATE)
def sa_oracle_DATE(_, satype, nullable=True):
    return dt.Date(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.VARCHAR2))
def sa_oracle_VARCHAR2(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.VARCHAR))
def sa_oracle_VARCHAR(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.NVARCHAR))
def sa_oracle_NVARCHAR(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.NVARCHAR2))
def sa_oracle_NVARCHAR2(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.CHAR))
def sa_oracle_CHAR(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.NCHAR))
def sa_oracle_NCHAR(_, satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.TIMESTAMP)
def sa_oracle_TIMESTAMP(_, satype, nullable=True):
    if satype.timezone:
        return dt.Timestamp(timezone="UTC", nullable=nullable)
    else:
        return dt.Timestamp(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, (sa.dialects.oracle.INTERVAL))
def sa_oracle_INTERVAL_DS(_, satype, nullable=True):
    return dt.Interval(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.BLOB)
def sa_oracle_BLOB(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.BINARY_FLOAT)
def sa_oracle_BINARY_FLOAT(_, satype, nullable=True):
    return dt.Float32(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.BINARY_DOUBLE)
def sa_oracle_BINARY_DOUBLE(_, satype, nullable=True):
    return dt.Float64(nullable=nullable)


@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.ROWID)
def sa_oracle_ROWID(_, satype, nullable=True):
    return dt.String(nullable=nullable)
