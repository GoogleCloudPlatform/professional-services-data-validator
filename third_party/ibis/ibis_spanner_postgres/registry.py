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
from sqlalchemy.dialects.postgresql.base import BYTEA

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


def _sa_cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype
    sa_arg = t.translate(arg)

    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.encode(sa_arg, sa.literal("hex"))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.decode(sa_arg, sa.literal("hex"))

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


operation_registry.update(
    {
        ops.Cast: _sa_cast,
        ops.ExtractEpochSeconds: postgres_registry.sa_epoch_seconds,
        ops.HashBytes: _format_hashbytes,
        ops.StringJoin: _string_join,
        ops.TableColumn: _table_column,
    }
)
