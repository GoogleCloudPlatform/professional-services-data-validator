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
import re

import ibis.expr.operations as ops
from ibis.backends.base.sql import compiler as sql_compiler
from ibis.backends.bigquery import compiler as bigquery_compiler
from ibis.backends.bigquery import rewrites
from third_party.ibis.ibis_cloud_spanner import registry


_NAME_REGEX = re.compile(r"[_A-Za-z][A-Za-z_0-9]*")


class SpannerSelect(sql_compiler.Select):
    def format_limit(self):
        if not self.limit:
            return None

        limit_sql = f"TABLESAMPLE RESERVOIR ({self.limit.n} ROWS)"
        return limit_sql


class SpannerExprTranslator(sql_compiler.ExprTranslator):
    """Translate expressions to strings."""

    _registry = registry._registry
    _rewrites = rewrites.REWRITES

    _forbids_frame_clause = (
        *sql_compiler.ExprTranslator._forbids_frame_clause,
        ops.Lag,
        ops.Lead,
    )

    _unsupported_reductions = (ops.ApproxMedian, ops.ApproxCountDistinct)
    _dialect_name = "spanner"

    @staticmethod
    def _gen_valid_name(name: str) -> str:
        name = "_".join(_NAME_REGEX.findall(name)) or "tmp"
        return f"`{name}`"

    def name(self, translated: str, name: str):
        # replace invalid characters in automatically generated names
        valid_name = self._gen_valid_name(name)
        if translated == valid_name:
            return translated
        return f"{translated} AS {valid_name}"

    @classmethod
    def compiles(cls, klass):
        def decorator(f):
            cls._registry[klass] = f
            return f

        return decorator

    def _trans_param(self, op):
        if op not in self.context.params:
            raise KeyError(op)
        return f"@{op.name}"


compiles = SpannerExprTranslator.compiles


class SpannerCompiler(sql_compiler.Compiler):
    # Spanner uses BigQuery SQL syntax
    translator_class = SpannerExprTranslator
    table_set_formatter_class = bigquery_compiler.BigQueryTableSetFormatter
    union_class = bigquery_compiler.BigQueryUnion
    intersect_class = bigquery_compiler.BigQueryIntersection
    difference_class = bigquery_compiler.BigQueryDifference
    select_class = SpannerSelect

    support_values_syntax_in_select = False
