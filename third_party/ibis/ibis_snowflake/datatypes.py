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
from typing import Iterable, Tuple

import ibis.expr.datatypes as dt
import sqlalchemy as sa
from ibis.backends.snowflake import Backend as SnowflakeBackend
from ibis.backends.snowflake.datatypes import parse
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.sqlalchemy import NUMBER, BINARY
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


@dt.dtype.register(SnowflakeDialect, NUMBER)
def sa_sf_numeric(_, satype, nullable=True):
    return dt.Decimal(
        precision=satype.precision or 38,
        scale=satype.scale or 0,
        nullable=nullable,
    )


@dt.dtype.register(SnowflakeDialect, BINARY)
def sa_sf_binary(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)


def _metadata(self, query: str) -> Iterable[Tuple[str, dt.DataType]]:
    with self.begin() as con, con.connection.cursor() as cur:
        result = cur.describe(query)

    for name, type_code, _, _, precision, scale, is_nullable in result:
        # Add check for type_code < 3 to ensure type is numeric, not TIMESTAMP_NTZ
        # which also populates scale
        if type_code < 3 and precision is not None and scale is not None:
            typ = dt.Decimal(precision=precision, scale=scale, nullable=is_nullable)
        else:
            typ = parse(FIELD_ID_TO_NAME[type_code]).copy(nullable=is_nullable)
        yield name, typ


SnowflakeBackend._metadata = _metadata
