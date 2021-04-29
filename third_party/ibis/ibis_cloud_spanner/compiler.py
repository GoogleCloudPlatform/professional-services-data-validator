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

import ibis.expr.operations as ops
import ibis_bigquery
from ibis_bigquery import compiler as bigquery_compiler


def build_ast(expr, context):
    builder = bigquery_compiler.BigQueryQueryBuilder(expr, context=context)
    return builder.get_result()


def to_sql(expr, context):
    query_ast = build_ast(expr, context)
    compiled = query_ast.compile()
    return compiled


def _array_index(translator, expr):
    # SAFE_OFFSET returns NULL if out of bounds
    return "{}[OFFSET({})]".format(*map(translator.translate, expr.op().args))


def _translate_pattern(translator, pattern):
    # add 'r' to string literals to indicate to Cloud Spanner this is a raw string
    return "r" * isinstance(pattern.op(), ops.Literal) + translator.translate(pattern)


def _regex_extract(translator, expr):
    arg, pattern, index = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = "REGEXP_EXTRACT({}, {})".format(translator.translate(arg), regex)
    return result


_operation_registry = bigquery_compiler._operation_registry.copy()
_operation_registry.update(
    {ops.RegexExtract: _regex_extract, ops.ArrayIndex: _array_index,}
)


compiles = bigquery_compiler.BigQueryExprTranslator.compiles
rewrites = bigquery_compiler.BigQueryExprTranslator.rewrites


dialect = ibis_bigquery.Backend().dialect
