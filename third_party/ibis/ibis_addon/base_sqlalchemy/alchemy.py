# Copyright 2022 Google LLC
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
from ibis.backends.base_sqlalchemy.alchemy import (
    _to_sqla_type,
    AlchemyTable,
    _AlchemyTableSet,
)
import ibis.expr.schema as sch
import ibis.expr.operations as ops


def _schema_to_sqlalchemy_columns(schema: sch.Schema):
    return [sa.column(n, _to_sqla_type(t)) for n, t in schema.items()]


def _format_table_new(self, expr):
    ctx = self.context
    ref_expr = expr
    op = ref_op = expr.op()

    if isinstance(op, ops.SelfReference):
        ref_expr = op.table
        ref_op = ref_expr.op()

    alias = ctx.get_ref(expr)
    if isinstance(ref_op, AlchemyTable):
        result = ref_op.sqla_table
    elif isinstance(ref_op, ops.SQLQueryResult):
        columns = _schema_to_sqlalchemy_columns(ref_op.schema)
        result = sa.text(ref_op.query).columns(*columns)
    elif isinstance(ref_op, ops.UnboundTable):
        # use SQLAlchemy's TableClause and ColumnClause for unbound tables
        schema = ref_op.schema
        result = sa.table(
            ref_op.name if ref_op.name is not None else ctx.get_ref(expr),
            *(
                sa.column(n, _to_sqla_type(t))
                for n, t in zip(schema.names, schema.types)
            ),
        )
    else:
        # A subquery
        if ctx.is_extracted(ref_expr):
            # Was put elsewhere, e.g. WITH block, we just need to grab
            # its alias
            alias = ctx.get_ref(expr)

            # hack
            if isinstance(op, ops.SelfReference):

                table = ctx.get_table(ref_expr)
                self_ref = table.alias(alias)
                ctx.set_table(expr, self_ref)
                return self_ref
            else:
                return ctx.get_table(expr)

        result = ctx.get_compiled_expr(expr)
        alias = ctx.get_ref(expr)

    result = result.alias(alias)
    ctx.set_table(expr, result)
    return result


_AlchemyTableSet._format_table = _format_table_new
