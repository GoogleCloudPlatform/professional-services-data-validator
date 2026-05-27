# Copyright 2020 Google LLC
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

from collections import OrderedDict

import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis.backends.base.sql.compiler.select_builder import SelectBuilder


class NewSelectBuilder(SelectBuilder):
    # We need to override this method https://github.com/ibis-project/ibis/blob/5.1.0/ibis/backends/base/sql/compiler/select_builder.py#L330
    # in the original SelectBuilder to avoid the use of CTEs to support MySQL versions < 8.0
    def _analyze_subqueries(self):
        # Find the subqueries, and record them in the passed query context.
        subqueries = _extract_subqueries(self)
        self.subqueries = []
        for expr in subqueries:
            # See #173. Might have been extracted already in a parent context.
            if not self.context.is_extracted(expr):
                self.subqueries.append(expr)
                self.context.set_extracted(expr)


def _extract_subqueries(select_stmt):
    helper = ExtractSubqueries(select_stmt)
    return helper.get_result()


def extract_noop(self, _):
    return


class ExtractSubqueries:
    def __init__(self, query, greedy=False):
        self.query = query
        self.greedy = greedy
        self.expr_counts = OrderedDict()
        self.node_to_expr = {}

    def get_result(self):
        if self.query.table_set is not None:
            self.visit(self.query.table_set)

        for clause in self.query.filters:
            self.visit(clause)

        expr_counts = self.expr_counts

        if self.greedy:
            to_extract = list(expr_counts.keys())
        else:
            to_extract = [op for op, count in expr_counts.items() if count > 1]

        node_to_expr = self.node_to_expr
        return [node_to_expr[op] for op in to_extract]

    def observe(self, key):

        if key not in self.node_to_expr:
            self.node_to_expr[key] = key

        assert self.node_to_expr[key].equals(key)
        self.expr_counts[key] = self.expr_counts.setdefault(key, 0) + 1

    def seen(self, op):
        return op in self.expr_counts

    def visit(self, node):
        method = "visit_{}".format(type(node).__name__)

        if hasattr(self, method):
            f = getattr(self, method)
            f(node)
        elif isinstance(node, ops.Join):
            self.visit_join(node)
        elif isinstance(node, ops.PhysicalTable):
            self.visit_physical_table(node)
        elif isinstance(node, ops.Value):
            for arg in node.args:
                if not isinstance(arg, ir.Expr):
                    continue
                self.visit(arg)
        else:
            raise NotImplementedError(type(node))

    def visit_join(self, node):
        self.visit(node.left)
        self.visit(node.right)

    visit_physical_table = extract_noop

    def visit_Exists(self, node):
        self.visit(node.foreign_table)
        for pred in node.predicates:
            self.visit(pred)

    visit_NotExistsSubquery = visit_ExistsSubquery = visit_Exists

    def visit_Aggregation(self, op):
        self.visit(op.table)
        self.observe(op)

    def visit_Distinct(self, op):
        self.observe(op)

    def visit_Limit(self, op):
        self.visit(op.table)
        self.observe(op)

    def visit_Union(self, op):
        self.visit(op.left)
        self.visit(op.right)
        self.observe(op)

    def visit_Intersection(self, op):
        self.visit(op.left)
        self.visit(op.right)
        self.observe(op)

    def visit_Difference(self, op):
        self.visit(op.left)
        self.visit(op.right)
        self.observe(op)

    def visit_MaterializedJoin(self, op):
        self.visit(op.join)
        self.observe(op)

    def visit_Selection(self, op):
        self.visit(op.table)
        self.observe(op)

    def visit_SQLQueryResult(self, op):
        self.observe(op)

    def visit_TableColumn(self, op):
        table = op.table
        if not self.seen(table):
            self.visit(table)

    def visit_SelfReference(self, op):
        self.visit(op.table)
