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

from sqlalchemy_sybase import DATETIME
from ibis.backends.mssql.compiler import MsSqlCompiler, MsSqlExprTranslator
from ibis.backends.base.sql.alchemy.query_builder import AlchemySelect


class SybaseAlchemySelect(AlchemySelect):
    def _compile_subqueries(self):
        """Sybase specific subquery compilation that prevents use of CTEs.

        Sybase does not support them: https://userapps.support.sap.com/sap/support/knowledge/en/2466482

        This code was copied from Ibis v5 ibis/backends/base/sql/alchemy/query_builder.py.
        When we upgrade Ibis we'll need to revisit this.
        """
        if not self.subqueries:
            return

        for expr in self.subqueries:
            result = self.context.get_compiled_expr(expr)
            alias = self.context.get_ref(expr)
            result = result.subquery(alias)  # DVT custom change.
            # result = result.cte(alias)  # Original Ibis code.
            self.context.set_ref(expr, result)


class SybaseExprTranslator(MsSqlExprTranslator):
    _timestamp_type = DATETIME


class SybaseCompiler(MsSqlCompiler):
    translator_class = SybaseExprTranslator
    select_class = SybaseAlchemySelect

    supports_indexed_grouping_keys = False
