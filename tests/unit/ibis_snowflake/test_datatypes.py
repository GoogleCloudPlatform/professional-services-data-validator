# Copyright 2026 Google LLC
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
from ibis.backends.snowflake.datatypes import SnowflakeType
from snowflake.sqlalchemy import BINARY, NUMBER

import third_party.ibis.ibis_snowflake.datatypes  # noqa: F401


def test_snowflake_number_maps_to_decimal():
    dtype = SnowflakeType.to_ibis(NUMBER(10, 0), nullable=False)

    assert isinstance(dtype, dt.Decimal)
    assert dtype.precision == 10
    assert dtype.scale == 0
    assert not dtype.nullable


def test_snowflake_binary_maps_to_binary():
    dtype = SnowflakeType.to_ibis(BINARY(), nullable=False)

    assert isinstance(dtype, dt.Binary)
    assert not dtype.nullable
