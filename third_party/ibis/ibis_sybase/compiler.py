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

from ibis.backends.base.sql.alchemy import AlchemyCompiler, AlchemyExprTranslator
from ibis.backends.base.sql.alchemy.query_builder import AlchemySelect
from sqlalchemy_sybase import DATETIME
from sqlalchemy_sybase.base import (
    SybaseSQLCompiler,
    SybaseDialect as sa_SybaseDialect,
)
from sqlalchemy.dialects.mssql.base import MSDialect

from third_party.ibis.ibis_sybase.registry import operation_registry


class DVTSybaseSQLCompiler(SybaseSQLCompiler):
    """DVT specific class to override sqlalchemy_sybase methods with SQL Server versions.

    Methods we override are to use TOP clause for row limiting:
        - limit_clause
        - get_select_precolumns
    """

    def limit_clause(self, cs, **kwargs):
        return ""

    def get_select_precolumns(self, select, **kw):
        """MS-SQL puts TOP, it's version of LIMIT here"""

        s = super().get_select_precolumns(select, **kw)

        if select._has_row_limiting_clause:
            # ODBC drivers and possibly others
            # don't support bind params in the SELECT clause on SQL Server.
            # so have to use literal here.
            kw["literal_execute"] = True
            s += "TOP %s " % self.process(self._get_limit_or_fetch(select), **kw)
            if select._fetch_clause is not None:
                if select._fetch_clause_options["percent"]:
                    s += "PERCENT "
                if select._fetch_clause_options["with_ties"]:
                    s += "WITH TIES "

        return s

    def _get_limit_or_fetch(self, select):
        if select._fetch_clause is None:
            return select._limit_clause
        else:
            return select._fetch_clause


sa_SybaseDialect.statement_compiler = DVTSybaseSQLCompiler


class SybaseDialect(MSDialect):
    name = "sybase"


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


class SybaseExprTranslator(AlchemyExprTranslator):
    _registry = operation_registry
    _timestamp_type = DATETIME
    _rewrites = AlchemyExprTranslator._rewrites.copy()


class SybaseCompiler(AlchemyCompiler):
    translator_class = SybaseExprTranslator
    select_class = SybaseAlchemySelect

    supports_indexed_grouping_keys = False
