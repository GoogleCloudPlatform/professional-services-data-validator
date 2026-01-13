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
import ibis.expr.operations as ops

from ibis.backends.base.sql.alchemy import (
    fixed_arity,
    sqlalchemy_operation_registry,
    sqlalchemy_window_functions_registry,
)
from ibis.backends.base.sql.alchemy.registry import _literal as base_literal

from third_party.ibis.ibis_mssql import registry as mssql_registry


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
        # Sybase ASE pads with zeros up to scale and does not appear to have a format function to
        # remove irrelevant trailing zeros.
        # The expression below is not very elegant but will ensure we don't have trailing spaces when scale>0.
        # The gist of it is:
        #   1. Replace all zeros with spaces and then rtrim (shame rtrim does not allow a replacement char)
        #   2. Replace spaces back to zeros. This is assuming it is not possible to get spaces for other reasons.
        #   3. Do the same with the dot character in case we've ended up with a lonely trailing dot.
        # The expression below assumes the decimla place spearator is ".". I can't find a session setting to control
        # this, it might be part of the language but my test system only has "us_english". We might need users
        # to modify the DVT user to have us_english if their default results in a decimal separator of ",".
        converted = sa.func.convert(sa.literal_column("VARCHAR(50)"), sa_arg)
        trailing_zero_removed = sa.func.str_replace(
            sa.func.rtrim(
                sa.func.str_replace(
                    sa.func.str_replace(
                        sa.func.rtrim(
                            sa.func.str_replace(
                                converted,
                                sa.literal_column("'0'"),
                                sa.literal_column("' '"),
                            )
                        ),
                        sa.literal_column("' '"),
                        sa.literal_column("'0'"),
                    ),
                    sa.literal_column("'.'"),
                    sa.literal_column("' '"),
                )
            ),
            sa.literal_column("' '"),
            sa.literal_column("'.'"),
        )
        return sa.case(
            (
                sa.func.charindex(sa.literal_column("'.'"), converted) > 0,
                trailing_zero_removed,
            ),
            else_=converted,
        )
    elif typ.is_timestamp():
        # There must be a way to set the target name for dt.Timestamp globally, but I couldn't find it.
        return sa.func.convert(sa.literal_column("'BIGDATETIME'"), sa_arg)
    elif arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.bintostr(sa_arg)
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.strtobin(sa_arg)
    elif (arg_dtype.is_integer() or arg_dtype.is_decimal()) and typ.is_string():
        # We can't cast to TEXT or VARCHAR(MAX) on Sybase so need to introduce a length:
        #    Error: Explicit conversion from datatype 'INT NULL' to 'TEXT' is not allowed.
        return sa.func.convert(sa.literal_column("VARCHAR(40)"), sa_arg)

    # Follow our SQL Server code path.
    return mssql_registry.sa_cast_mssql(t, op)


def sa_format_hashbytes(translator, op):
    """Format a hash string for DVT validation.

    Unfortunately the Sybase SHA algorithm is not compatible with SHA256 therefore this
    function is more an example of how we might to it rather than a useful implementation.

    Sybase ASE does not support a SHA256 algorithm:
    https://userapps.support.sap.com/sap/support/knowledge/en/3108409

    It might be possible to add a customer UDF for this, see issue-xxxx for details.
    """
    arg = translator.translate(op.arg)
    cast_arg = sa.func.convert(sa.sql.literal_column("VARCHAR(16384)"), arg)
    hash_func = sa.func.hash(cast_arg, sa.sql.literal_column("'sha256'"))
    return hash_func


def sa_format_string_length(translator, op):
    """Sybase string length function is char_length()."""
    arg = translator.translate(op.arg)
    return sa.func.char_length(arg)


def sa_string_join(t, op):
    sep, args = op.args
    expr = None
    for arg in args:
        sa_arg = t.translate(arg)
        if expr is None:
            expr = sa_arg
        else:
            expr = expr.concat(sa_arg)
    return expr


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


def sa_whitespace_rstrip(t, op):
    sa_arg = t.translate(op.arg)
    return sa.func.rtrim(sa_arg)


def sa_literal(t, op):
    if op.output_dtype.is_timestamp() and op.output_dtype.timezone:
        # Sybase ASE does not have a time zoned data type.
        value = op.value.replace(tzinfo=None)
        return sa.literal(value)
    else:
        return base_literal(t, op)


operation_registry = sqlalchemy_operation_registry.copy()
operation_registry.update(sqlalchemy_window_functions_registry)

operation_registry[ops.Cast] = sa_cast_sybase
operation_registry[ops.ExtractEpochSeconds] = sa_epoch_seconds
operation_registry[ops.HashBytes] = sa_format_hashbytes
operation_registry[ops.IfNull] = fixed_arity(sa.func.isnull, 2)
operation_registry[ops.Literal] = sa_literal
operation_registry[ops.RandomScalar] = mssql_registry.sa_format_new_id
operation_registry[ops.RStrip] = sa_whitespace_rstrip
operation_registry[ops.Strftime] = strftime
operation_registry[ops.StringJoin] = sa_string_join
operation_registry[ops.StringLength] = sa_format_string_length
operation_registry[ops.TableColumn] = mssql_registry.sa_table_column
