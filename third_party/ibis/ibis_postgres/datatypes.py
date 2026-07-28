# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ibis.expr.datatypes as dt
from ibis.backends.postgres.datatypes import PostgresType
from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import ischema_names


class XML(sqltypes.TypeEngine):
    __visit_name__ = "XML"


ischema_names["xml"] = XML

orig_to_ibis = PostgresType.to_ibis


@classmethod
def dvt_postgres_to_ibis(cls, typ, nullable=True):
    if isinstance(typ, postgresql.INTERVAL) and typ.fields is None:
        return dt.Interval(unit="s", nullable=nullable)
    elif isinstance(typ, sqltypes.Time):
        return dt.Time(nullable=nullable)
    elif isinstance(typ, postgresql.OID):
        return dt.Int32(nullable=nullable)
    elif isinstance(typ, XML):
        return dt.Unknown(nullable=nullable)
    return orig_to_ibis(typ, nullable=nullable)


PostgresType.to_ibis = dvt_postgres_to_ibis
