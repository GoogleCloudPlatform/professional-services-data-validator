# Copyright 2025 Google LLC
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

import sqlalchemy as sa
from ibis.backends.base.sql.alchemy.registry import _cast as sa_fixed_cast


def format_hashbytes_snowflake(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.sha2(arg)


def epoch_time_snowflake(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.date_part(sa.sql.literal_column("epoch_seconds"), arg)


def cast_snowflake(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.dtype
    sa_arg = t.translate(arg)

    # Specialize going from numeric(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ):
        # When casting a number to string Snowflake includes the full scale, e.g.:
        #   SELECT CAST(CAST(100 AS DECIMAL(5,2)) AS VARCHAR(10));
        #     100.00
        # This doesn't match most engines which would return "100".
        # Using to_char() function instead of cast to return a more typical value.
        # We've wrapped to_char in rtrim(".") due to whole numbers having a trailing ".".
        precision = arg_dtype.precision or 38
        fmt = (
            "FM"
            + ("9" * (precision - arg_dtype.scale - 1))
            + "0."
            + ("9" * arg_dtype.scale)
        )
        return sa.func.rtrim(sa.func.to_char(sa_arg, fmt), ".")

    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.hex_encode(sa_arg, sa.literal(0))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.hex_decode_binary(sa_arg)

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)
