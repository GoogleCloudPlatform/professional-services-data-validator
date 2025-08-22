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
from ibis.backends.postgres.datatypes import PostgresType, _from_postgres_types
from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import ischema_names
import sqlalchemy.types as sat


class DVTPostgresType(PostgresType):
    @classmethod
    def to_ibis(cls, typ: sat.TypeEngine, nullable: bool = True) -> dt.DataType:
        if isinstance(typ, postgresql.TIME):
            # Ibis Time doesn't support timezone therefore not included it below.
            return dt.Time(nullable=nullable)
        else:
            return super().to_ibis(typ, nullable=nullable)


class XML(sqltypes.TypeEngine):
    __visit_name__ = "XML"


ischema_names["xml"] = XML

# TODO need to test these work!
# DVT override to only support INTERVAL with no fields.
_from_postgres_types[postgresql.INTERVAL] = dt.Interval
_from_postgres_types[postgresql.OID] = dt.int32
_from_postgres_types[postgresql.TIME] = dt.Time
# Matching Ibis v9.2 behaviour and mapping PostgreSQL xml type to unknown.
_from_postgres_types[XML] = dt.Unknown
