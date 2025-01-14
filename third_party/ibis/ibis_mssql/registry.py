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

from ibis.backends.base.sql.alchemy import (
    get_sqla_table,
)
from ibis.backends.base.sql.alchemy.registry import get_col


def sa_table_column(t, op):
    ctx = t.context
    table = op.table

    sa_table = get_sqla_table(ctx, table)

    out_expr = get_col(sa_table, op)
    out_expr.quote = t._quote_column_names

    if op.output_dtype.is_timestamp():
        timezone = op.output_dtype.timezone
        if timezone is not None:
            # Using literal_column on SQL Server because the time zone string cannot be a bind.
            out_expr = sa.literal_column(
                f"{out_expr.name} AT TIME ZONE '{timezone}'"
            ).label(op.name)

    # If the column does not originate from the table set in the current SELECT
    # context, we should format as a subquery
    if t.permit_subquery and ctx.is_foreign_expr(table):
        try:
            subq = sa_table.subquery()
        except AttributeError:
            subq = sa_table
        return sa.select(subq.c[out_expr.name])

    return out_expr


def strftime(translator, op):
    """Use MS SQL CONVERT() in place of STRFTIME().

    This is pretty restrictive due to the limited styles offered by SQL Server,
    we've just covered off the generic formats used when casting date based columns
    to string in order to complete row data comparison."""
    arg, pattern = map(translator.translate, op.args)
    supported_convert_styles = {
        "%Y-%m-%d": 23,  # ISO8601
        "%Y-%m-%d %H:%M:%S": 20,  # ODBC canonical
        "%Y-%m-%d %H:%M:%S.%f": 21,  # ODBC canonical (with milliseconds)
    }
    try:
        convert_style = supported_convert_styles[pattern.value]
    except KeyError:
        raise NotImplementedError(
            f"strftime format {pattern.value} not supported for SQL Server."
        )
    arg_type = op.args[0].output_dtype
    if (
        hasattr(arg_type, "timezone") and arg_type.timezone
    ):  # our datetime comparisons do not include timezone, so we need to cast this to Datetime which is timezone naive
        arg = sa.cast(arg, sa.types.DateTime)
    return sa.func.convert(sa.text("VARCHAR"), arg, convert_style)


def sa_epoch_seconds(translator, op):
    """Override for standard ExtractEpochSeconds but catering for larger second values."""
    arg = translator.translate(op.arg)
    return sa.cast(
        sa.func.datediff_big(sa.text("s"), "1970-01-01 00:00:00", arg), sa.BIGINT
    )


def sa_format_binary_length(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.datalength(arg)


def sa_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    cast_arg = sa.func.convert(sa.sql.literal_column("VARCHAR(MAX)"), arg)
    hash_func = sa.func.hashbytes(sa.sql.literal_column("'SHA2_256'"), cast_arg)
    hash_to_string = sa.func.convert(
        sa.sql.literal_column("CHAR(64)"), hash_func, sa.sql.literal_column("2")
    )
    return sa.func.lower(hash_to_string)
