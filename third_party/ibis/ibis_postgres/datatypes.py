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
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import PGDialect


@dt.dtype.register(PGDialect, postgresql.OID)
def sa_pg_oid(_, sa_type, nullable=True):
    return dt.int32(nullable=nullable)
