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

import ibis

from ibis.expr import datatypes
from ibis.expr.operations import Comparison


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
