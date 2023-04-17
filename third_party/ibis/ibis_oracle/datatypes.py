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
from sqlalchemy.dialects.oracle.cx_oracle import OracleDialect_cx_oracle

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy.datatypes import ibis_type_to_sqla
import cx_Oracle

# Update to avoid cast to CLOB/Text
ibis_type_to_sqla[dt.String] = sa.sql.sqltypes.String(length=4000)


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
        raise NotImplementedError(
            f"Oracle type {typename} is not supported"
        )
    
    if typename == cx_Oracle.DB_TYPE_NUMBER:
        if col[4] == 0 and col[5] == -127:
            # This will occur if type is NUMBER with no precision/scale
            typ = partial(typ, precision=38, scale=0)
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
        cx_Oracle.DB_TYPE_RAW: dt.Binary,
        cx_Oracle.DB_TYPE_BFILE: dt.Binary,
        cx_Oracle.DB_TYPE_NUMBER: dt.Decimal,
        cx_Oracle.DB_TYPE_LONG: dt.String,
        cx_Oracle.DB_TYPE_NCLOB: dt.String,
        cx_Oracle.DB_TYPE_CLOB: dt.String,
        cx_Oracle.DB_TYPE_BLOB: dt.Binary,
    }

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
    return dt.Decimal(satype.precision or 38, satype.scale or 0, nullable=nullable)


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
    return dt.Timestamp(nullable=nullable)

@dt.dtype.register(OracleDialect_cx_oracle, sa.dialects.oracle.BLOB)
def sa_oracle_BLOB(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)