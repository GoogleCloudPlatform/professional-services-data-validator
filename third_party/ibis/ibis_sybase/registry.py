# Copyright 2025 Google Inc.
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
from third_party.ibis.ibis_mssql.registry import sa_cast_mssql


def sa_cast_sybase(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)
    # Specialize going from DECIMAL(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ) or ((arg_dtype.is_float32() or arg_dtype.is_float64()) and typ.is_string()):
        # Prevent SQL Server specific workaround.
        return sa_fixed_cast(t, op)
    elif typ.is_timestamp():
        # There must be a way to set the target name for dt.Timestamp globally, need to try again.
        return sa.func.convert(sa.literal_column("'BIGDATETIME'"), sa_arg)
    elif arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.bintostr(sa_arg)
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.strtobin(sa_arg)

    # Follow our SQL Server code path.
    return sa_cast_mssql(t, op)


def sa_format_hashbytes(translator, op):
    """Format a hash string for DVT validation.

    Unfortunately the Sybase SHA algorithm is not compatible with SHA256 therefore this
    function is more an example of how we might to it rather than a useful implementation.

    Sybase ASE does not support a SHA256 algorithm:
    https://userapps.support.sap.com/sap/support/knowledge/en/3108409
    """
    arg = translator.translate(op.arg)
    cast_arg = sa.func.convert(sa.sql.literal_column("VARCHAR(16384)"), arg)
    hash_func = sa.func.hash(cast_arg, sa.sql.literal_column("'sha1'"))
    return hash_func


def sa_format_string_length(translator, op):
    """Sybase string length function is char_length()."""
    arg = translator.translate(op.arg)
    return sa.func.char_length(arg)


def sa_string_join(t, op):
    sep, elements = op.args
    columns = [str(col.name) for col in map(t.translate, elements)]
    return sa.sql.literal_column(" || ".join(columns))


def strftime(translator, op):
    """Use Sybase convert() for strftime().

    This is pretty restrictive due to the limited styles offered by Sybase,
    we've just covered off the generic formats used when casting date based columns
    to string in order to complete row data comparison.

    Convert formats:
    https://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc36271.1600/doc/html/san1393050423349.html

    Incredibly there isn't a format matching ISO formats."""
    arg, pattern = map(translator.translate, op.args)
    arg_type = op.args[0].output_dtype
    if (
        hasattr(arg_type, "timezone") and arg_type.timezone
    ):  # Our datetime comparisons do not include timezone, so we need to cast this to Datetime which is timezone naive
        arg = sa.cast(arg, sa.types.DateTime)
    if pattern.value == "%Y-%m-%d":
        # format 23 (yyyy-mm-ddTHH:mm:ss) - we trim the time from.
        return sa.func.substring(
            sa.func.convert(sa.text("VARCHAR"), arg, sa.literal_column("23")), 1, 10
        )
    elif pattern.value == "%Y-%m-%d %H:%M:%S":
        # format 23 again (yyyy-mm-ddTHH:mm:ss) - we replace the "T".
        return sa.func.str_replace(
            sa.func.convert(sa.text("VARCHAR"), arg, sa.literal_column("23")),
            sa.literal_column("'T'"),
            sa.literal_column("' '"),
        )
    elif pattern.value == "%Y-%m-%d %H:%M:%S.%f":
        # ODBC canonical (with microseconds)
        return sa.func.convert(sa.text("VARCHAR"), arg, 140)
    else:
        raise NotImplementedError(
            f"strftime format {pattern.value} not supported for Sybase."
        )


def sa_epoch_seconds(translator, op):
    """Override for standard ExtractEpochSeconds but catering for larger second values."""
    arg = translator.translate(op.arg)
    return sa.cast(
        sa.func.datediff(
            sa.text("SECOND"), sa.literal_column("'1970-01-01 00:00:00'"), arg
        ),
        sa.BIGINT,
    )
