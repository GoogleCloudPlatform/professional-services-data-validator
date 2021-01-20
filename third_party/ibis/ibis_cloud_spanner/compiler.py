# Copyright 2021 Google Inc.
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

import datetime
from functools import partial

import numpy as np
import regex as re
import toolz
from multipledispatch import Dispatcher

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.sql.compiler as comp
from third_party.ibis.ibis_cloud_spanner.datatypes import ibis_type_to_cloud_spanner_type
from ibis.impala import compiler as impala_compiler
from ibis.impala.compiler import (
    ImpalaSelect,
    ImpalaTableSetFormatter,
    _reduction,
    fixed_arity,
    unary,
)


class CloudSpannerSelectBuilder(comp.SelectBuilder):
    @property
    def _select_class(self):
        return CloudSpannerSelect



class CloudSpannerUnion(comp.Union):
    @staticmethod
    def keyword(distinct):
        return 'UNION DISTINCT' if distinct else 'UNION ALL'



class CloudSpannerQueryBuilder(comp.QueryBuilder):

    select_builder = CloudSpannerSelectBuilder
    union_class = CloudSpannerUnion


def build_ast(expr, context):
    builder = CloudSpannerQueryBuilder(expr, context=context)
    return builder.get_result()


def to_sql(expr, context):
    query_ast = build_ast(expr, context)
    compiled = query_ast.compile()
    return compiled


class CloudSpannerContext(comp.QueryContext):
    def _to_sql(self, expr, ctx):
        return to_sql(expr, context=ctx)



_operation_registry = impala_compiler._operation_registry.copy()


_invalid_operations = {
    ops.Translate,
    ops.FindInSet,
    ops.Capitalize,
    ops.DateDiff,
    ops.TimestampDiff,
}

_operation_registry = {
    k: v
    for k, v in _operation_registry.items()
    if k not in _invalid_operations
}


class CloudSpannerExprTranslator(impala_compiler.ImpalaExprTranslator):
    _registry = _operation_registry
    _rewrites = impala_compiler.ImpalaExprTranslator._rewrites.copy()

    context_class = CloudSpannerContext

    def _trans_param(self, expr):
        op = expr.op()
        if op not in self.context.params:
            raise KeyError(op)
        return '@{}'.format(expr.get_name())


compiles = CloudSpannerExprTranslator.compiles
rewrites = CloudSpannerExprTranslator.rewrites





class CloudSpannerTableSetFormatter(ImpalaTableSetFormatter):
    def _quote_identifier(self, name):
        if re.match(r'^[A-Za-z][A-Za-z_0-9]*$', name):
            return name
        return '`{}`'.format(name)


class CloudSpannerSelect(ImpalaSelect):

    translator = CloudSpannerExprTranslator

    @property
    def table_set_formatter(self):
        return CloudSpannerTableSetFormatter





class CloudSpannerDialect(impala_compiler.ImpalaDialect):
    translator = CloudSpannerExprTranslator


dialect = CloudSpannerDialect


