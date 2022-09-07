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
import third_party.ibis.ibis_oracle.expr.datatypes as dt11

import ibis.expr.datatypes as dt
import ibis.backends.base_sqlalchemy.alchemy as s_al

_ibis_type_to_sqla = {
    dt11.CLOB: sa.CLOB,
    # dt11.NCLOB: sa.NCLOB,
    # dt11.LONG: sa.LONG,
    # dt11.NUMBER: sa.NUMBER,
    # dt11.BFILE: sa.BFILE,
    # dt11.RAW: sa.RAW,
    dt11.LONGRAW: sa.Binary,
}
_ibis_type_to_sqla.update(s_al._ibis_type_to_sqla)
_ibis_type_to_sqla[dt.String] = sa.sql.sqltypes.String(length=4000)

def _to_sqla_type(itype, type_map=None):
    if type_map is None:
        type_map = _ibis_type_to_sqla
    if isinstance(itype, dt.Decimal):
        return sa.types.NUMERIC(itype.precision, itype.scale)
    elif isinstance(itype, dt.Date):
        return sa.Date()
    elif isinstance(itype, dt.Timestamp):
        # SQLAlchemy DateTimes do not store the timezone, just whether the db
        # supports timezones.
        return sa.TIMESTAMP(bool(itype.timezone))
    elif isinstance(itype, dt.Array):
        ibis_type = itype.value_type
        if not isinstance(ibis_type, (dt.Primitive, dt.String)):
            raise TypeError(
                "Type {} is not a primitive type or string type".format(ibis_type)
            )
        return sa.ARRAY(_to_sqla_type(ibis_type, type_map=type_map))
    else:
        return type_map[type(itype)]


class AlchemyExprTranslator(s_al.AlchemyExprTranslator):
    s_al.AlchemyExprTranslator._type_map = _ibis_type_to_sqla


class AlchemyDialect(s_al.AlchemyDialect):
    s_al.translator = AlchemyExprTranslator
