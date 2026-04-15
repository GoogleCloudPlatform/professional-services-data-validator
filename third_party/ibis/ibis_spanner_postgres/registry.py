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

import ibis.expr.operations as ops
from ibis.backends.base.sql.alchemy.registry import (
    get_col,
    get_sqla_table,
    _cast as sa_fixed_cast,
)
from ibis.backends.postgres.registry import (
    operation_registry as base_pg_operation_registry,
)
import sqlalchemy as sa

from third_party.ibis.ibis_postgres import registry as postgres_registry


operation_registry = base_pg_operation_registry.copy()


def _format_hashbytes(translator, op):
    raise ValueError("Spanner does not support a HASH function")


def _string_join(t, op):
    # Copied from Oracle registry because PostgreSQL concat_ws() is not supported in Spanner PostgreSQL.
    sep, elements = op.args
    columns = [str(col.name) for col in map(t.translate, elements)]
    return sa.sql.literal_column(" || ".join(columns))


def _table_column(t, op):
    ctx = t.context
    table = op.table

    sa_table = get_sqla_table(ctx, table)
    out_expr = get_col(sa_table, op)

    # Commenting TIME ZONE clause for Spanner.
    # if op.output_dtype.is_timestamp():
    #    timezone = op.output_dtype.timezone
    #    if timezone is not None:
    #        out_expr = out_expr.op("AT TIME ZONE")(timezone).label(op.name)

    # If the column does not originate from the table set in the current SELECT
    # context, we should format as a subquery
    if t.permit_subquery and ctx.is_foreign_expr(table):
        return sa.select(out_expr)

    return out_expr


def _cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype
    sa_arg = t.translate(arg)

    # TO/FROM_HEX Spanner function are not exposed to PostgreSQL dialect.
    # Also PostgreSQL equivalent encode/decode are not included in Spanner PostgreSQL dialect.
    # Both lines below will fail but are included for reference.
    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.to_hex(sa_arg)
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.from_hex(sa_arg)

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def _count(t, op):
    arg = op.arg
    where = getattr(op, "where", None)

    # In Spanner PostgreSQL, count(numeric) is unsupported.
    # We must cast Decimal columns to strings first.
    if hasattr(arg, "output_dtype") and arg.output_dtype.is_decimal():
        sa_arg = t.translate(arg)
        sa_arg = sa.cast(sa_arg, sa.String)

        if where is not None:
            sa_where = t.translate(where)
            sa_arg = sa.case([(sa_where, sa_arg)], else_=None)

        return sa.func.count(sa_arg)

    # Fallback to original implementation
    return base_pg_operation_registry[ops.Count](t, op)


operation_registry.update(
    {
        ops.Cast: _cast,
        ops.Count: _count,
        ops.ExtractEpochSeconds: postgres_registry.sa_epoch_seconds,
        ops.HashBytes: _format_hashbytes,
        ops.StringJoin: _string_join,
        ops.TableColumn: _table_column,
    }
)
