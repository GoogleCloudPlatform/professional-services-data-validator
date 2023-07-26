# Copyright(C) 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0(the "License"); you may not
# use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the
# License for the specific language governing permissions and limitations under
# the License.
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
import sqlalchemy as sa
import ibis.expr.datatypes as dt

from snowflake.sqlalchemy import (
    NUMBER
)

@dt.dtype.register(SnowflakeDialect, NUMBER)
def sa_sf_numeric(_, satype, nullable=True):
    return dt.Decimal(
        precision=satype.precision or 38,
        scale=satype.scale or 0,
        nullable=nullable,
    )