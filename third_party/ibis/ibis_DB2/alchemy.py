# Copyright 2020 Google Inc.
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

import ibm_db_sa
import sqlalchemy as sa
from ibm_db_sa.base import DB2Dialect

import ibis.expr.datatypes as dt11
import ibis.expr.datatypes as dts
import ibis.backends.base_sqlalchemy.alchemy as s_al
import third_party.ibis.ibis_DB2.expr.datatypes as dt

_ibis_type_to_sqla = {
    dt.CLOB: sa.CLOB,
    dt.BLOB: sa.BLOB,
    dt.SMALLINT: sa.SMALLINT,
    dt.INTEGER: sa.INTEGER,
    dt.BIGINT: sa.BIGINT,
}
_ibis_type_to_sqla.update(s_al._ibis_type_to_sqla)


def _to_sqla_type(itype, type_map=None):
    if type_map is None:
        type_map = _ibis_type_to_sqla
    if isinstance(itype, dt11.Decimal):
        return sa.types.NUMERIC(itype.precision, itype.scale)
    elif isinstance(itype, dt11.Date):
        return sa.Date()
    elif isinstance(itype, dt11.Timestamp):
        return sa.TIMESTAMP(bool(itype.timezone))
    elif isinstance(itype, dt11.Array):
        ibis_type = itype.value_type
        if not isinstance(ibis_type, (dt11.Primitive, dt11.String)):
            raise TypeError(
                'Type {} is not a primitive type or string type'.format(
                    ibis_type
                )
            )
        return sa.ARRAY(_to_sqla_type(ibis_type, type_map=type_map))
    else:
        return type_map[type(itype)]


class AlchemyExprTranslator(s_al.AlchemyExprTranslator):
    s_al.AlchemyExprTranslator._type_map = _ibis_type_to_sqla


class AlchemyDialect(s_al.AlchemyDialect):
    s_al.translator = AlchemyExprTranslator


class AlchemyTable(s_al.AlchemyTable):
    pass


class AlchemyDatabaseSchema(s_al.AlchemyDatabaseSchema):
    pass


class AlchemyDatabase(s_al.AlchemyDatabase):
    pass


class AlchemyClient(s_al.AlchemyClient):
    pass


@dt.dtype.register(DB2Dialect, ibm_db_sa.CLOB)
def sa_db2_CLOB(_, satype, nullable=True):
    return dt.CLOB(nullable=nullable)


@dt.dtype.register(DB2Dialect, ibm_db_sa.BLOB)
def sa_db2_BLOB(_, satype, nullable=True):
    return dt.BLOB(nullable=nullable)


@dt.dtype.register(DB2Dialect, ibm_db_sa.GRAPHIC)
def sa_db2_GRAPHIC(_, satype, nullable=True):
    return dt.GRAPHIC(nullable=nullable)


@dt.dtype.register(DB2Dialect, ibm_db_sa.VARGRAPHIC)
def sa_db2_VARGRAPHIC(_, satype, nullable=True):
    return dt.VARGRAPHIC(nullable=nullable)


@dt.dtype.register(DB2Dialect, ibm_db_sa.SMALLINT)
def sa_db2_SMALLINT(_, satype, nullable=True):
    return dt.SMALLINT(nullable=nullable)


@dt.dtype.register(DB2Dialect, ibm_db_sa.INTEGER)
def sa_db2_INTEGER(_, satype, nullable=True):
    return dt.INTEGER(nullable=nullable)


@dt.dtype.register(DB2Dialect, ibm_db_sa.BIGINT)
def sa_db2_BIGINT(_, satype, nullable=True):
    return dt.BIGINT(nullable=nullable)


@dts.dtype.register(DB2Dialect, ibm_db_sa.DOUBLE)
def sa_db2_DOUBLE(_, satype, nullable=True):
    return dts.Double(nullable=nullable)


