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

""" The Ibis Addons Operations are intended to help facilitate new expressions
when required before they can be pushed upstream to Ibis.

Raw SQL Filters:
The ability to inject RawSQL into a query DNE in Ibis.  It must be built out
and applied to each Ibis Data Source directly as each has
extended it's own registry.  Eventually this can potentially be pushed to
Ibis as an override, though it would not apply for Pandas and other
non-textual languages.
"""

import ibis

from ibis.expr import datatypes
from ibis.expr.operations import Comparison
from ibis.impala.compiler import ImpalaExprTranslator


class RawSQL(Comparison):
    pass


def compile_raw_sql(table, sql):
    op = RawSQL(table[table.columns[0]].cast(datatypes.string), ibis.literal(sql))
    return op.to_expr()


def format_raw_sql(translator, expr):
    op = expr.op()
    rand_col, raw_sql = op.args
    return raw_sql.op().args[0]


ibis.bigquery.compiler.BigQueryExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[RawSQL] = format_raw_sql
try:
    # Try to add Teradata and pass if error (not imported)
    from third_party.ibis.ibis_teradata.compiler import TeradataExprTranslator
    TeradataExprTranslator._registry[RawSQL] = format_raw_sql
except Exception:
    pass

