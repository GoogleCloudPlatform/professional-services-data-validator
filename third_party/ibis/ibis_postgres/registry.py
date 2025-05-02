# Copyright 2024 Google Inc.
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
from ibis.backends.base.sql.alchemy.registry import _cast as sa_fixed_cast


def sa_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    convert = sa.func.convert_to(arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.sha256(convert)
    return sa.func.encode(hash_func, sa.sql.literal_column("'hex'"))


def sa_epoch_seconds(translator, op):
    """Override for standard ExtractEpochSeconds but catering for larger second values.

    This expression also truncates fractional seconds from the incoming datetime.
    This matches behaviour on other SQL engines' epoch seconds expressions."""
    arg = translator.translate(op.arg)
    return sa.cast(
        sa.extract("epoch", sa.func.date_trunc(sa.sql.literal_column("'second'"), arg)),
        sa.BIGINT,
    )


def sa_cast_postgres(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype
    sa_arg = t.translate(arg)

    if arg_dtype.is_decimal() and typ.is_string():
        # trim_scale() is only available in PostgreSQL 13+ but solves a lot of problems
        # when trying to get consistently formatted numerics.
        # We've documented a workaround for PostgreSQL 12 and older.
        return sa.cast(sa.func.trim_scale(sa_arg), t.get_sqla_type(typ))
    elif arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.encode(sa_arg, sa.literal("hex"))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.decode(sa_arg, sa.literal("hex"))

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)
