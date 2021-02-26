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

import sqlalchemy as sa
import snowflake.sqlalchemy as sf_sa
import third_party.ibis.ibis_snowflake.expr.datatypes as dt
import ibis.expr.datatypes as dt_i
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

import ibis.expr.datatypes as dt11
import ibis.backends.base_sqlalchemy.alchemy as s_al
from snowflake.sqlalchemy.base import dialect as snowdialect
import snowflake.sqlalchemy.snowdialect as dd

@dt_i.dtype.register(snowdialect, dd.ARRAY)
def sa_array(_, satype, nullable=True):
    """
    dimensions = satype.dimensions
    if dimensions is not None and dimensions != 1:
        raise NotImplementedError('Nested array types not yet supported')

    value_dtype = dt.dtype(dialect, satype.item_type)"""
    return dt.Array(nullable=nullable)

_ibis_type_to_sqla = {
    dt.TIMESTAMP_NTZ: sf_sa.TIMESTAMP_NTZ,
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
        # SQLAlchemy DateTimes do not store the timezone, just whether the db
        # supports timezones.
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


@dt_i.dtype.register(SnowflakeDialect, sf_sa.snowdialect.TIMESTAMP_NTZ)
def sa_snowflake_TIMESTAMP_NTZ(_, satype, nullable=True):
    return dt.TIMESTAMP_NTZ(nullable=nullable)