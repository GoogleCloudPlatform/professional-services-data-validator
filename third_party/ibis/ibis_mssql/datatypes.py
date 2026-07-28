# Copyright 2020 Google LLC
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
from ibis.backends.mssql.datatypes import _to_mssql_types, MSSQLType
from sqlalchemy.dialects import mssql

# Needs to be VARCHAR instead of NVARCHAR for Hash function
_to_mssql_types[dt.String] = mssql.VARCHAR
_to_mssql_types[dt.Float64] = mssql.FLOAT

_original_to_ibis = MSSQLType.to_ibis


@classmethod
def _sa_mssql_to_ibis(cls, typ, nullable=True):
    if isinstance(typ, mssql.DATETIMEOFFSET):
        # Override DATETIMEOFFSET to remove timestamp scale of 7 for valid schema matching
        return dt.Timestamp(timezone="UTC", nullable=nullable)
    elif isinstance(typ, (mssql.DATETIME2, mssql.DATETIME)):
        # Override DATETIME2 and DATETIME to remove timestamp scale of 7 for valid schema matching
        return dt.Timestamp(nullable=nullable)
    elif isinstance(typ, mssql.TINYINT):
        return dt.Int16(nullable=nullable)
    return _original_to_ibis(typ, nullable=nullable)


MSSQLType.to_ibis = _sa_mssql_to_ibis

