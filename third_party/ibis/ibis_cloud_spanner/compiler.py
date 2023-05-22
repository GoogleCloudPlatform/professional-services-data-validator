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
from ibis.backends.base.sql import compiler as sql_compiler
from ibis.backends.bigquery import compiler as bigquery_compiler

compiles = bigquery_compiler.BigQueryExprTranslator.compiles
rewrites = bigquery_compiler.BigQueryExprTranslator.rewrites


class SpannerSelect(sql_compiler.Select):
    def format_limit(self):
        if not self.limit:
            return None

        limit_sql = f"TABLESAMPLE RESERVOIR ({self.limit.n} ROWS)"
        return limit_sql


class SpannerCompiler(sql_compiler.Compiler):
    # Spanner uses BigQuery SQL syntax
    translator_class = bigquery_compiler.BigQueryExprTranslator
    table_set_formatter_class = bigquery_compiler.BigQueryTableSetFormatter
    union_class = bigquery_compiler.BigQueryUnion
    intersect_class = bigquery_compiler.BigQueryIntersection
    difference_class = bigquery_compiler.BigQueryDifference
    select_class = SpannerSelect

    support_values_syntax_in_select = False
