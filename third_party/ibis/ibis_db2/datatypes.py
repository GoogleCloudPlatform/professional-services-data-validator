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

from ibis.backends.base.sql.alchemy.datatypes import AlchemyType
import ibis.expr.datatypes as dt
import ibm_db_dbi
from ibm_db_sa.ibm_db import DB2Dialect_ibm_db
import sqlalchemy as sa
import sqlalchemy.types as sat

# Types from https://github.com/ibmdb/python-ibmdb/blob/master/IBM_DB/ibm_db/ibm_db_dbi.py
_type_mapping = {
    ibm_db_dbi.STRING: dt.String,
    ibm_db_dbi.NUMBER: dt.Int32,
    ibm_db_dbi.TEXT: dt.String,
    ibm_db_dbi.BINARY: dt.Binary,
    ibm_db_dbi.BIGINT: dt.Int64,
    ibm_db_dbi.FLOAT: dt.Float64,
    ibm_db_dbi.DECIMAL: dt.Decimal,
    ibm_db_dbi.DATE: dt.Date,
    ibm_db_dbi.TIME: dt.Time,
    ibm_db_dbi.DATETIME: dt.Timestamp,
    ibm_db_dbi.BOOLEAN: dt.Boolean,
    ibm_db_dbi.ROWID: dt.String,
}


class Db2Type(AlchemyType):
    dialect = "db2"

    @classmethod
    def from_ibis(cls, dtype: dt.DataType) -> sat.TypeEngine:
        if dtype.is_string():
            return sa.sql.sqltypes.String(length=3000)
        return super().from_ibis(dtype)

    @classmethod
    def to_ibis(cls, typ: sat.TypeEngine, nullable: bool = True) -> dt.DataType:
        if isinstance(typ, sat.BINARY):
            return dt.Binary(nullable=nullable)
        return super().to_ibis(typ, nullable=nullable)


DB2Dialect_ibm_db.ischema_names["BINARY"] = sat.BINARY
DB2Dialect_ibm_db.ischema_names["DECFLOAT"] = sat.DOUBLE
DB2Dialect_ibm_db.ischema_names["VARBINARY"] = sat.BINARY
# Db2 z/OS variants.
DB2Dialect_ibm_db.ischema_names["VARBIN"] = DB2Dialect_ibm_db.ischema_names["VARBINARY"]
DB2Dialect_ibm_db.ischema_names["VARG"] = DB2Dialect_ibm_db.ischema_names["VARGRAPHIC"]


def _get_type(column) -> dt.DataType:
    typename = column[1]
    typ = _type_mapping.get(typename)
    if typ is None:
        raise NotImplementedError(f"Db2 type {typename} is not supported")

    if typ == dt.Decimal:
        precision = column[4]
        scale = column[5]
        if precision is not None and scale is not None:
            return dt.Decimal(precision, scale)
        return dt.Decimal()

    if isinstance(typ, type):
        return typ()

    return typ
