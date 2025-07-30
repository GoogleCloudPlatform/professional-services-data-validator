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
from ibis.backends.postgres.datatypes import sa_postgres_interval
from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import PGDialect, ischema_names


class XML(sqltypes.TypeEngine):
    __visit_name__ = "XML"


ischema_names["xml"] = XML


@dt.dtype.register(PGDialect, postgresql.INTERVAL)
def dvt_sa_postgres_interval(_, satype, nullable=True):
    """DVT override of ibis/backends/postgres/datatypes/sa_postgres_interval to support INTERVAL with no fields."""
    if satype.fields is None:
        return dt.Interval(nullable=nullable)

    return sa_postgres_interval(_, satype, nullable)


@dt.dtype.register(PGDialect, postgresql.OID)
def sa_pg_oid(_, sa_type, nullable=True):
    return dt.int32(nullable=nullable)


# Matching Ibis v9.2 behaviour and mapping PostgreSQL xml type to unknown.
@dt.dtype.register(PGDialect, XML)
def sa_pg_xml(_, sa_type, nullable=True):
    return dt.Unknown(nullable=nullable)
